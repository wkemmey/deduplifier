use std::path::Path;
use std::time::SystemTime;

use anyhow::Result;
use rusqlite::{params, Connection};

use crate::utils;

/// SHA-256 of an empty byte sequence — the hash assigned to empty directories.
pub const EMPTY_DIR_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// A row from the `files` table.
#[derive(Debug, Clone, PartialEq)]
pub struct FileRecord {
    pub path: String,
    pub hash: String,
    pub size: i64,
    pub modified: i64,
}

/// A summary row from a duplicate-group query.
/// `size` is `SUM(size)` for file groups and `MAX(size)` for directory groups.
#[derive(Debug, Clone, PartialEq)]
pub struct DuplicateGroupHash {
    pub hash: String,
    pub count: i64,
    pub size: i64,
}

/// A row from the `directories` table.
#[derive(Debug, Clone, PartialEq)]
pub struct DirRecord {
    pub path: String,
    pub hash: String,
    pub size: i64,
}

// ---------------------------------------------------------------------------
// Schema / connection
// ---------------------------------------------------------------------------

pub fn setup_schema(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL,
            modified INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS directories (
            path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_hash ON files(hash)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dir_hash ON directories(hash)",
        [],
    )?;

    Ok(())
}

pub fn init_database(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    setup_schema(&conn)?;
    Ok(conn)
}

// ---------------------------------------------------------------------------
// File records
// ---------------------------------------------------------------------------

/// Returns `true` if the file at `path` is not in the DB, or its stored
/// modified timestamp differs from `modified`.
pub fn should_update_file(conn: &Connection, path: &Path, modified: SystemTime) -> Result<bool> {
    let actual = modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;
    match get_file(conn, path)? {
        None => Ok(true),
        Some(rec) => Ok(actual != rec.modified),
    }
}

/// Fetch a single file record by path; returns `None` if not found.
pub fn get_file(conn: &Connection, path: &Path) -> Result<Option<FileRecord>> {
    let path_str = utils::path_to_str(path)?;
    let result = conn
        .prepare("SELECT path, hash, size, modified FROM files WHERE path = ?1")?
        .query_row(params![path_str], |row| {
            Ok(FileRecord {
                path: row.get(0)?,
                hash: row.get(1)?,
                size: row.get(2)?,
                modified: row.get(3)?,
            })
        });

    match result {
        Ok(file) => Ok(Some(file)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Return all file records, ordered by path.
pub fn all_files(conn: &Connection) -> Result<Vec<FileRecord>> {
    let mut stmt = conn.prepare("SELECT path, hash, size, modified FROM files ORDER BY path")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(FileRecord {
                path: row.get(0)?,
                hash: row.get(1)?,
                size: row.get(2)?,
                modified: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

/// Return all file records with the given hash, ordered by path.
pub fn files_with_hash(conn: &Connection, hash: &str) -> Result<Vec<FileRecord>> {
    let mut stmt =
        conn.prepare("SELECT path, hash, size, modified FROM files WHERE hash = ?1 ORDER BY path")?;
    let rows = stmt
        .query_map(params![hash], |row| {
            Ok(FileRecord {
                path: row.get(0)?,
                hash: row.get(1)?,
                size: row.get(2)?,
                modified: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Return groups of files that share the same hash (i.e. duplicates).
/// Each item is `(hash, count, total_size_bytes)`, sorted by total_size descending.
pub fn duplicate_file_groups(conn: &Connection) -> Result<Vec<DuplicateGroupHash>> {
    let mut stmt = conn.prepare(
        "SELECT hash, COUNT(*) AS cnt, SUM(size) AS total_size
            FROM files
            GROUP BY hash
            HAVING cnt > 1
            ORDER BY total_size DESC",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(DuplicateGroupHash {
                hash: row.get(0)?,
                count: row.get(1)?,
                size: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Insert or replace a file record.
pub fn upsert_file(
    conn: &Connection,
    path: &Path,
    hash: &str,
    size: i64,
    modified: i64,
) -> Result<()> {
    let path_str = utils::path_to_str(path)?;
    conn.execute(
        "INSERT OR REPLACE INTO files (path, hash, size, modified) VALUES (?1, ?2, ?3, ?4)",
        params![path_str, hash, size, modified],
    )?;
    Ok(())
}

/// Rename a file record from `old_path` to `new_path`.
pub fn move_file(conn: &Connection, old_path: &Path, new_path: &Path) -> Result<()> {
    let old = utils::path_to_str(old_path)?;
    let new = utils::path_to_str(new_path)?;
    conn.execute(
        "UPDATE files SET path = ?1 WHERE path = ?2",
        params![new, old],
    )?;
    Ok(())
}

/// Delete a single file record.
pub fn remove_file(conn: &Connection, path: &Path) -> Result<()> {
    let path_str = utils::path_to_str(path)?;
    conn.execute("DELETE FROM files WHERE path = ?1", params![path_str])?;
    Ok(())
}

/// Update only the hash of an existing file record.
pub fn update_file_hash(conn: &Connection, path: &Path, hash: &str) -> Result<()> {
    let path_str = utils::path_to_str(path)?;
    conn.execute(
        "UPDATE files SET hash = ?1 WHERE path = ?2",
        params![hash, path_str],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Stale-file tracking  (requires the `visited_files` temp table)
// ---------------------------------------------------------------------------

/// Create (if absent) and clear the `visited_files` temp table.
/// Call once at the start of each scan.
pub fn init_visited_files(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS visited_files (path TEXT PRIMARY KEY);
         DELETE FROM visited_files;",
    )?;
    Ok(())
}

/// Record that `path` was seen during the current scan.
pub fn mark_visited(conn: &Connection, path: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO visited_files (path) VALUES (?1)",
        params![path],
    )?;
    Ok(())
}

/// Count files in the DB under `root_prefix` that were not seen in this scan.
/// `root_prefix` should be the root path string (no trailing slash needed).
pub fn stale_file_count(conn: &Connection, root_prefix: &str) -> Result<i64> {
    let sep = std::path::MAIN_SEPARATOR;
    let pattern = format!("{}{}%", root_prefix.trim_end_matches(sep), sep);
    let count = conn.query_row(
        "SELECT COUNT(*) FROM files
            LEFT JOIN visited_files ON files.path = visited_files.path
            WHERE files.path LIKE ?1 AND visited_files.path IS NULL",
        params![pattern],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// Delete all files under `root_prefix` that were not seen in this scan.
pub fn delete_stale_files(conn: &Connection, root_prefix: &str) -> Result<()> {
    let sep = std::path::MAIN_SEPARATOR;
    let pattern = format!("{}{}%", root_prefix.trim_end_matches(sep), sep);
    conn.execute(
        "DELETE FROM files WHERE path LIKE ?1 AND path IN (
            SELECT files.path FROM files
            LEFT JOIN visited_files ON files.path = visited_files.path
            WHERE files.path LIKE ?1 AND visited_files.path IS NULL
        )",
        params![pattern],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Directory records
// ---------------------------------------------------------------------------

/// Return all directory paths, ordered by path.
/// Used by similar.rs to load the full directory set in one query.
pub fn all_directory_paths(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT path FROM directories ORDER BY path")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

/// Return the immediate child directories of `dir_path` stored in the DB.
/// "Immediate" means exactly one level deeper — no grandchildren.
pub fn child_directories(conn: &Connection, dir_path: &Path) -> Result<Vec<DirRecord>> {
    let path_str = utils::path_to_str(dir_path)?;
    let sep = std::path::MAIN_SEPARATOR;
    let bare_path = path_str.trim_end_matches(sep);
    let child_pattern = format!("{bare_path}{sep}%");
    let grandchild_pattern = format!("{bare_path}{sep}%{sep}%");
    let mut stmt = conn.prepare(
        "SELECT path, hash, size FROM directories
            WHERE path LIKE ?1
            AND path NOT LIKE ?2",
    )?;
    let rows = stmt
        .query_map(params![child_pattern, grandchild_pattern], |row| {
            Ok(DirRecord {
                path: row.get(0)?,
                hash: row.get(1)?,
                size: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Return all directory records with the given hash, ordered by path.
pub fn directories_with_hash(conn: &Connection, hash: &str) -> Result<Vec<DirRecord>> {
    let mut stmt =
        conn.prepare("SELECT path, hash, size FROM directories WHERE hash = ?1 ORDER BY path")?;
    let rows = stmt
        .query_map(params![hash], |row| {
            Ok(DirRecord {
                path: row.get(0)?,
                hash: row.get(1)?,
                size: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Return groups of directories that share the same non-empty hash (i.e. duplicates).
/// Each item is `(hash, count, max_size_bytes)`, sorted by max_size descending.
pub fn duplicate_directory_groups(conn: &Connection) -> Result<Vec<DuplicateGroupHash>> {
    let mut stmt = conn.prepare(
        "SELECT hash, COUNT(*) AS cnt, MAX(size) AS max_size
            FROM directories
            WHERE hash != ?1
            GROUP BY hash
            HAVING cnt > 1
            ORDER BY max_size DESC",
    )?;
    let rows = stmt
        .query_map(params![EMPTY_DIR_HASH], |row| {
            Ok(DuplicateGroupHash {
                hash: row.get(0)?,
                count: row.get(1)?,
                size: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Insert or replace a directory record.
pub fn upsert_directory(conn: &Connection, path: &Path, hash: &str, size: i64) -> Result<()> {
    let path_str = utils::path_to_str(path)?;
    conn.execute(
        "INSERT OR REPLACE INTO directories (path, hash, size) VALUES (?1, ?2, ?3)",
        params![path_str, hash, size],
    )?;
    Ok(())
}

/// Delete all file records and all directory records whose path starts with
/// `path` (inclusive). Use this for bulk removal of an entire directory tree.
pub fn remove_tree(conn: &Connection, path: &Path) -> Result<()> {
    let path_str = utils::path_to_str(path)?;
    let sep = std::path::MAIN_SEPARATOR;
    let bare_path = path_str.trim_end_matches(sep);
    // Match the root itself OR anything directly under it, but not siblings
    // (e.g. /photos/12 must not match /photos/123).
    let subtree_pattern = format!("{}{sep}%", bare_path);
    conn.execute(
        "DELETE FROM files WHERE path = ?1 OR path LIKE ?2",
        params![bare_path, subtree_pattern],
    )?;
    conn.execute(
        "DELETE FROM directories WHERE path = ?1 OR path LIKE ?2",
        params![bare_path, subtree_pattern],
    )?;
    Ok(())
}

// ------------------------------------------------------------------
//
//
// TESTS
//
//
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        setup_schema(&conn).unwrap();
        conn
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn insert_file_raw(conn: &Connection, path: &str, hash: &str, size: i64, modified: i64) {
        conn.execute(
            "INSERT INTO files (path, hash, size, modified) VALUES (?1, ?2, ?3, ?4)",
            params![path, hash, size, modified],
        )
        .unwrap();
    }

    fn insert_dir_raw(conn: &Connection, path: &str, hash: &str, size: i64) {
        conn.execute(
            "INSERT INTO directories (path, hash, size) VALUES (?1, ?2, ?3)",
            params![path, hash, size],
        )
        .unwrap();
    }

    // -----------------------------------------------------------------------
    // should_update_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_should_update_file_not_in_db() {
        let conn = open_test_db();
        let modified = SystemTime::now();
        assert!(should_update_file(&conn, Path::new("/new/file.txt"), modified).unwrap());
    }

    #[test]
    fn test_should_update_file_same_timestamp() {
        let conn = open_test_db();
        let secs = 1_000_000i64;
        insert_file_raw(&conn, "/f.txt", "abc", 100, secs);
        let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64);
        assert!(!should_update_file(&conn, Path::new("/f.txt"), modified).unwrap());
    }

    #[test]
    fn test_should_update_file_different_timestamp() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/f.txt", "abc", 100, 1_000_000);
        let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000);
        assert!(should_update_file(&conn, Path::new("/f.txt"), modified).unwrap());
    }

    // -----------------------------------------------------------------------
    // get_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_file_not_found() {
        let conn = open_test_db();
        assert_eq!(get_file(&conn, Path::new("/missing.txt")).unwrap(), None);
    }

    #[test]
    fn test_get_file_found() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/a.txt", "hash1", 42, 999);
        let rec = get_file(&conn, Path::new("/a.txt")).unwrap().unwrap();
        assert_eq!(rec.path, "/a.txt");
        assert_eq!(rec.hash, "hash1");
        assert_eq!(rec.size, 42);
        assert_eq!(rec.modified, 999);
    }

    // -----------------------------------------------------------------------
    // files_with_hash
    // -----------------------------------------------------------------------

    #[test]
    fn test_files_with_hash_none() {
        let conn = open_test_db();
        assert!(files_with_hash(&conn, "nohash").unwrap().is_empty());
    }

    #[test]
    fn test_files_with_hash_returns_matches() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/a.txt", "shared", 10, 1);
        insert_file_raw(&conn, "/b.txt", "shared", 10, 2);
        insert_file_raw(&conn, "/c.txt", "unique", 10, 3);
        let results = files_with_hash(&conn, "shared").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.hash == "shared"));
    }

    // -----------------------------------------------------------------------
    // duplicate_file_groups
    // -----------------------------------------------------------------------

    #[test]
    fn test_duplicate_file_groups_empty() {
        let conn = open_test_db();
        assert!(duplicate_file_groups(&conn).unwrap().is_empty());
    }

    #[test]
    fn test_duplicate_file_groups_no_dups() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/a.txt", "h1", 10, 1);
        insert_file_raw(&conn, "/b.txt", "h2", 10, 2);
        assert!(duplicate_file_groups(&conn).unwrap().is_empty());
    }

    #[test]
    fn test_duplicate_file_groups_detects_dup() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/a.txt", "same", 100, 1);
        insert_file_raw(&conn, "/b.txt", "same", 100, 2);
        insert_file_raw(&conn, "/c.txt", "unique", 50, 3);
        let groups = duplicate_file_groups(&conn).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].hash, "same");
        assert_eq!(groups[0].count, 2);
        assert_eq!(groups[0].size, 200);
    }

    #[test]
    fn test_duplicate_file_groups_sorted_by_total_size_desc() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/a.txt", "small", 1, 1);
        insert_file_raw(&conn, "/b.txt", "small", 1, 2);
        insert_file_raw(&conn, "/c.txt", "big", 1000, 3);
        insert_file_raw(&conn, "/d.txt", "big", 1000, 4);
        let groups = duplicate_file_groups(&conn).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].hash, "big");
        assert_eq!(groups[1].hash, "small");
    }

    // -----------------------------------------------------------------------
    // upsert_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_upsert_file_insert_and_replace() {
        let conn = open_test_db();
        let path = Path::new("/f.txt");
        upsert_file(&conn, path, "h1", 10, 100).unwrap();
        let rec = get_file(&conn, path).unwrap().unwrap();
        assert_eq!(rec.hash, "h1");

        // Replace with new hash
        upsert_file(&conn, path, "h2", 20, 200).unwrap();
        let rec = get_file(&conn, path).unwrap().unwrap();
        assert_eq!(rec.hash, "h2");
        assert_eq!(rec.size, 20);
        assert_eq!(rec.modified, 200);
    }

    // -----------------------------------------------------------------------
    // move_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_move_file() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/old.txt", "h1", 10, 1);
        move_file(&conn, Path::new("/old.txt"), Path::new("/new.txt")).unwrap();
        assert_eq!(get_file(&conn, Path::new("/old.txt")).unwrap(), None);
        assert!(get_file(&conn, Path::new("/new.txt")).unwrap().is_some());
    }

    // -----------------------------------------------------------------------
    // remove_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_remove_file() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/f.txt", "h1", 10, 1);
        remove_file(&conn, Path::new("/f.txt")).unwrap();
        assert_eq!(get_file(&conn, Path::new("/f.txt")).unwrap(), None);
    }

    #[test]
    fn test_remove_file_nonexistent_is_ok() {
        let conn = open_test_db();
        // Should not error even if the file doesn't exist
        remove_file(&conn, Path::new("/no/such/file.txt")).unwrap();
    }

    // -----------------------------------------------------------------------
    // update_file_hash
    // -----------------------------------------------------------------------

    #[test]
    fn test_update_file_hash() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/f.txt", "old_hash", 10, 1);
        update_file_hash(&conn, Path::new("/f.txt"), "new_hash").unwrap();
        let rec = get_file(&conn, Path::new("/f.txt")).unwrap().unwrap();
        assert_eq!(rec.hash, "new_hash");
        // Other fields unchanged
        assert_eq!(rec.size, 10);
        assert_eq!(rec.modified, 1);
    }

    // -----------------------------------------------------------------------
    // Stale file tracking
    // -----------------------------------------------------------------------

    #[test]
    fn test_stale_file_count_all_visited() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/root/a.txt", "h1", 10, 1);
        insert_file_raw(&conn, "/root/b.txt", "h2", 10, 2);
        init_visited_files(&conn).unwrap();
        mark_visited(&conn, "/root/a.txt").unwrap();
        mark_visited(&conn, "/root/b.txt").unwrap();
        assert_eq!(stale_file_count(&conn, "/root").unwrap(), 0);
    }

    #[test]
    fn test_stale_file_count_one_missing() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/root/a.txt", "h1", 10, 1);
        insert_file_raw(&conn, "/root/b.txt", "h2", 10, 2);
        init_visited_files(&conn).unwrap();
        mark_visited(&conn, "/root/a.txt").unwrap();
        // b.txt not visited → stale
        assert_eq!(stale_file_count(&conn, "/root").unwrap(), 1);
    }

    #[test]
    fn test_stale_file_count_ignores_other_roots() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/root/a.txt", "h1", 10, 1);
        insert_file_raw(&conn, "/other/b.txt", "h2", 10, 2);
        init_visited_files(&conn).unwrap();
        mark_visited(&conn, "/root/a.txt").unwrap();
        // /other/b.txt is under a different root — not counted as stale for /root
        assert_eq!(stale_file_count(&conn, "/root").unwrap(), 0);
    }

    #[test]
    fn test_delete_stale_files() {
        let conn = open_test_db();
        insert_file_raw(&conn, "/root/keep.txt", "h1", 10, 1);
        insert_file_raw(&conn, "/root/gone.txt", "h2", 10, 2);
        init_visited_files(&conn).unwrap();
        mark_visited(&conn, "/root/keep.txt").unwrap();

        delete_stale_files(&conn, "/root").unwrap();

        assert!(get_file(&conn, Path::new("/root/keep.txt"))
            .unwrap()
            .is_some());
        assert_eq!(get_file(&conn, Path::new("/root/gone.txt")).unwrap(), None);
    }

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // child_directories
    // -----------------------------------------------------------------------

    #[test]
    fn test_child_directories_none() {
        let conn = open_test_db();
        let kids = child_directories(&conn, Path::new("/root")).unwrap();
        assert!(kids.is_empty());
    }

    #[test]
    fn test_child_directories_returns_immediate_only() {
        let conn = open_test_db();
        insert_dir_raw(&conn, "/root/child", "hc", 10);
        insert_dir_raw(&conn, "/root/child/grandchild", "hg", 5);
        insert_dir_raw(&conn, "/root/other", "ho", 8);
        insert_dir_raw(&conn, "/unrelated", "hu", 1);

        let kids = child_directories(&conn, Path::new("/root")).unwrap();
        let paths: Vec<&str> = kids.iter().map(|r| r.path.as_str()).collect();
        // Only immediate children, not grandchild or unrelated
        assert!(paths.contains(&"/root/child"));
        assert!(paths.contains(&"/root/other"));
        assert!(!paths.contains(&"/root/child/grandchild"));
        assert!(!paths.contains(&"/unrelated"));
    }

    // -----------------------------------------------------------------------
    // directories_with_hash
    // -----------------------------------------------------------------------

    #[test]
    fn test_directories_with_hash_none() {
        let conn = open_test_db();
        assert!(directories_with_hash(&conn, "nohash").unwrap().is_empty());
    }

    #[test]
    fn test_directories_with_hash_returns_matches() {
        let conn = open_test_db();
        insert_dir_raw(&conn, "/a", "shared", 10);
        insert_dir_raw(&conn, "/b", "shared", 10);
        insert_dir_raw(&conn, "/c", "unique", 10);
        let results = directories_with_hash(&conn, "shared").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.hash == "shared"));
    }

    // -----------------------------------------------------------------------
    // duplicate_directory_groups
    // -----------------------------------------------------------------------

    #[test]
    fn test_duplicate_directory_groups_empty() {
        let conn = open_test_db();
        assert!(duplicate_directory_groups(&conn).unwrap().is_empty());
    }

    #[test]
    fn test_duplicate_directory_groups_excludes_empty_hash() {
        let conn = open_test_db();
        insert_dir_raw(&conn, "/a", EMPTY_DIR_HASH, 0);
        insert_dir_raw(&conn, "/b", EMPTY_DIR_HASH, 0);
        // Empty dirs should not appear as duplicates
        assert!(duplicate_directory_groups(&conn).unwrap().is_empty());
    }

    #[test]
    fn test_duplicate_directory_groups_detects_dup() {
        let conn = open_test_db();
        insert_dir_raw(&conn, "/a/photos", "samehash", 1000);
        insert_dir_raw(&conn, "/b/photos", "samehash", 1000);
        insert_dir_raw(&conn, "/c/unique", "uniquehash", 500);
        let groups = duplicate_directory_groups(&conn).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].hash, "samehash");
        assert_eq!(groups[0].count, 2);
        assert_eq!(groups[0].size, 1000);
    }

    #[test]
    fn test_duplicate_directory_groups_sorted_by_max_size_desc() {
        let conn = open_test_db();
        insert_dir_raw(&conn, "/a/small", "small_hash", 10);
        insert_dir_raw(&conn, "/b/small", "small_hash", 10);
        insert_dir_raw(&conn, "/a/big", "big_hash", 9999);
        insert_dir_raw(&conn, "/b/big", "big_hash", 9999);
        let groups = duplicate_directory_groups(&conn).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].hash, "big_hash");
        assert_eq!(groups[1].hash, "small_hash");
    }

    // -----------------------------------------------------------------------
    // upsert_directory
    // -----------------------------------------------------------------------

    #[test]
    fn test_upsert_directory_insert_and_replace() {
        let conn = open_test_db();
        let path = Path::new("/mydir");
        upsert_directory(&conn, path, "h1", 100).unwrap();
        let dirs = directories_with_hash(&conn, "h1").unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].size, 100);

        // Replace
        upsert_directory(&conn, path, "h2", 200).unwrap();
        assert!(directories_with_hash(&conn, "h1").unwrap().is_empty());
        let dirs = directories_with_hash(&conn, "h2").unwrap();
        assert_eq!(dirs[0].size, 200);
    }

    // -----------------------------------------------------------------------
    // remove_tree
    // -----------------------------------------------------------------------

    #[test]
    fn test_remove_tree_removes_all_files_and_dirs() {
        let conn = open_test_db();
        insert_dir_raw(&conn, "/tree", "dh1", 100);
        insert_dir_raw(&conn, "/tree/sub", "dh2", 50);
        insert_file_raw(&conn, "/tree/a.txt", "fh1", 10, 1);
        insert_file_raw(&conn, "/tree/sub/b.txt", "fh2", 10, 2);
        insert_dir_raw(&conn, "/other", "dh3", 10);
        insert_file_raw(&conn, "/other/c.txt", "fh3", 10, 3);

        remove_tree(&conn, Path::new("/tree")).unwrap();

        // Everything under /tree is gone
        let file_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE path LIKE '/tree%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(file_count, 0);
        let dir_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM directories WHERE path LIKE '/tree%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dir_count, 0);

        // /other is untouched
        let other_files: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE path LIKE '/other%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(other_files, 1);
        let other_dirs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM directories WHERE path = '/other'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(other_dirs, 1);
    }

    #[test]
    fn test_remove_tree_does_not_affect_siblings() {
        let conn = open_test_db();
        insert_dir_raw(&conn, "/photos/12", "dh1", 10);
        insert_file_raw(&conn, "/photos/12/a.jpg", "fh1", 10, 1);
        insert_dir_raw(&conn, "/photos/123", "dh2", 10);
        insert_file_raw(&conn, "/photos/123/b.jpg", "fh2", 10, 2);

        remove_tree(&conn, Path::new("/photos/12")).unwrap();

        assert_eq!(
            get_file(&conn, Path::new("/photos/12/a.jpg")).unwrap(),
            None
        );
        assert!(get_file(&conn, Path::new("/photos/123/b.jpg"))
            .unwrap()
            .is_some());
        assert!(directories_with_hash(&conn, "dh2").unwrap().len() == 1);
    }
}
