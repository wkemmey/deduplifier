use crate::db;
use crate::hashing::{compute_directory_hash, compute_file_hash};
use crate::utils::path_to_str;
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: String,
    pub hash: String,
    pub size: u64,
}

/// First pass: walk all files under `root`, hash any that are new or changed,
/// load cached hashes for unchanged files, and populate `files_by_dir`.
/// Returns the count of files skipped due to invalid UTF-8 paths.
fn scan_files(
    conn: &Connection,
    root: &Path,
    total_files: usize,
    files_by_dir: &mut HashMap<PathBuf, Vec<FileEntry>>,
) -> Result<usize> {
    let mut processed = 0;
    let mut invalid_paths = 0usize;

    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            processed += 1;
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("<unknown>");
            // \r - return to the start of the line
            // \x1B[K - clear everything from cursor to end of line
            print!("\r\x1B[K{}/{} - {}", processed, total_files, file_name);
            io::stdout().flush()?;

            let metadata = fs::metadata(path)?;
            let modified = metadata.modified()?;
            let size = metadata.len();

            let path_str = match path_to_str(path) {
                Ok(s) => s.to_string(),
                Err(e) => {
                    eprintln!("\nWarning: skipping file with invalid UTF-8 path: {}", e);
                    invalid_paths += 1;
                    continue;
                }
            };

            db::mark_visited(conn, &path_str)?;

            if db::should_update_file(conn, path, modified)? {
                match compute_file_hash(path) {
                    Ok(hash) => {
                        let modified_secs =
                            modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;
                        db::upsert_file(conn, path, &hash, size as i64, modified_secs)?;
                        if let Some(parent) = path.parent() {
                            files_by_dir
                                .entry(parent.to_path_buf())
                                .or_default()
                                .push(FileEntry {
                                    path: path_str,
                                    hash,
                                    size,
                                });
                        }
                    }
                    Err(e) => eprintln!("Error hashing file {:?}: {}", path, e),
                }
            } else {
                // File unchanged — load hash and size from the DB cache.
                // We must use the cached hash here; re-hashing would give the same
                // result but waste I/O, and more importantly, the hash already in the
                // DB is what all other records (directory hashes, duplicates) refer to.
                if let Some(record) = db::get_file(conn, path)? {
                    if let Some(parent) = path.parent() {
                        files_by_dir
                            .entry(parent.to_path_buf())
                            .or_default()
                            .push(FileEntry {
                                path: path_str,
                                hash: record.hash,
                                size: record.size as u64,
                            });
                    }
                }
            }
        }
    }
    Ok(invalid_paths)
}

/// Second pass: compute and store directory hashes bottom-up (deepest first),
/// so each child directory's hash is committed to the DB before its parent is hashed.
fn compute_directory_hashes(
    conn: &Connection,
    root: &Path,
    files_by_dir: &HashMap<PathBuf, Vec<FileEntry>>,
) -> Result<()> {
    let mut dir_entries: Vec<PathBuf> = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.path().to_path_buf())
        .collect();

    // Deepest first — children are committed before parents are hashed
    dir_entries.sort_by(|a, b| b.components().count().cmp(&a.components().count()));

    for dir_path in dir_entries {
        compute_directory_hash(conn, &dir_path, files_by_dir)?;
    }
    Ok(())
}

pub fn scan_directory(
    conn: &Connection,
    root: &Path,
    total_files: usize,
    prompt_stale: bool,
) -> Result<usize> {
    db::init_visited_files(conn)?;

    let mut files_by_dir: HashMap<PathBuf, Vec<FileEntry>> = HashMap::new();
    let invalid_paths = scan_files(conn, root, total_files, &mut files_by_dir)?;

    let root_str = path_to_str(root)?.to_string();
    println!("\nChecking for stale database entries (this may take several minutes for large directories)...");
    io::stdout().flush()?;

    let stale_count = db::stale_file_count(conn, &root_str)?;
    if stale_count > 0 {
        println!(
            "\n{} file(s) in the database no longer exist on disk under {:?}.",
            stale_count, root
        );

        let should_delete = if prompt_stale {
            print!("Delete them from the database? [y/N] ");
            io::stdout().flush()?;
            let stdin = io::stdin();
            let mut line = String::new();
            stdin.lock().read_line(&mut line)?;
            line.trim().eq_ignore_ascii_case("y")
        } else {
            false
        };

        if should_delete {
            db::delete_stale_files(conn, &root_str)?;
            println!("Deleted {} stale file(s) from the database.", stale_count);
        } else if prompt_stale {
            println!("Skipped deletion of stale entries.");
        }
    }

    compute_directory_hashes(conn, root, &files_by_dir)?;

    Ok(invalid_paths)
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
    use crate::db::{self, setup_schema};
    use rusqlite::Connection;
    use std::fs;
    use tempfile::tempdir;

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        setup_schema(&conn).unwrap();
        conn
    }

    fn get_file_hash(conn: &Connection, path: &std::path::Path) -> String {
        conn.query_row(
            "SELECT hash FROM files WHERE path = ?1",
            rusqlite::params![path.to_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn get_dir_hash(conn: &Connection, path: &std::path::Path) -> String {
        conn.query_row(
            "SELECT hash FROM directories WHERE path = ?1",
            rusqlite::params![path.to_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn insert_ghost_file(conn: &Connection, path: &std::path::Path) {
        conn.execute(
            "INSERT INTO files (path, hash, size, modified) VALUES (?1, 'ghost', 0, 0)",
            rusqlite::params![path.to_str().unwrap()],
        )
        .unwrap();
    }

    // -----------------------------------------------------------------------
    // scan_files
    // -----------------------------------------------------------------------

    #[test]
    fn test_scan_files_populates_files_by_dir_with_correct_values() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();

        let conn = open_test_db();
        db::init_visited_files(&conn).unwrap();
        let mut files_by_dir = HashMap::new();
        scan_files(&conn, dir.path(), 1, &mut files_by_dir).unwrap();

        let files = files_by_dir
            .get(dir.path())
            .expect("directory should be in map");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].size, 5); // "hello" is 5 bytes
        assert_eq!(
            files[0].hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_scan_files_marks_all_files_visited() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "a").unwrap();
        fs::write(dir.path().join("b.txt"), "b").unwrap();

        let conn = open_test_db();
        db::init_visited_files(&conn).unwrap();
        let mut files_by_dir = HashMap::new();
        scan_files(&conn, dir.path(), 2, &mut files_by_dir).unwrap();

        let visited_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM visited_files", [], |r| r.get(0))
            .unwrap();
        assert_eq!(visited_count, 2);
    }

    #[test]
    fn test_scan_files_cache_hit_uses_stored_hash() {
        // After a first scan, corrupt the hash in the DB but leave the modified
        // time untouched. A second scan should use the cache and leave the
        // corrupted hash in place — proving it did NOT re-hash the file.
        let dir = tempdir().unwrap();
        let file = dir.path().join("a.txt");
        fs::write(&file, "hello").unwrap();

        let conn = open_test_db();

        // First scan — stores real hash and modified time
        db::init_visited_files(&conn).unwrap();
        let mut files_by_dir = HashMap::new();
        scan_files(&conn, dir.path(), 1, &mut files_by_dir).unwrap();

        // Overwrite the hash with a sentinel, keeping modified time unchanged
        conn.execute(
            "UPDATE files SET hash = 'cached_sentinel' WHERE path = ?1",
            rusqlite::params![file.to_str().unwrap()],
        )
        .unwrap();

        // Second scan — modified time hasn't changed, so cache should be used
        db::init_visited_files(&conn).unwrap();
        let mut files_by_dir2 = HashMap::new();
        scan_files(&conn, dir.path(), 1, &mut files_by_dir2).unwrap();

        let stored_hash = get_file_hash(&conn, &file);
        assert_eq!(
            stored_hash, "cached_sentinel",
            "cache should prevent re-hashing"
        );

        // files_by_dir should also reflect the cached hash, not the real one
        let files = files_by_dir2.get(dir.path()).unwrap();
        assert_eq!(files[0].hash, "cached_sentinel");
    }

    #[test]
    fn test_scan_files_rehashes_when_modified_time_changes() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("a.txt");
        fs::write(&file, "hello").unwrap();

        let conn = open_test_db();

        // First scan
        db::init_visited_files(&conn).unwrap();
        let mut files_by_dir = HashMap::new();
        scan_files(&conn, dir.path(), 1, &mut files_by_dir).unwrap();

        // Store a wrong hash and wind back the modified time in the DB so
        // should_update_file sees a mismatch on the next scan
        conn.execute(
            "UPDATE files SET hash = 'stale_hash', modified = 0 WHERE path = ?1",
            rusqlite::params![file.to_str().unwrap()],
        )
        .unwrap();

        // Second scan — modified time mismatch triggers re-hash
        db::init_visited_files(&conn).unwrap();
        let mut files_by_dir2 = HashMap::new();
        scan_files(&conn, dir.path(), 1, &mut files_by_dir2).unwrap();

        let stored_hash = get_file_hash(&conn, &file);
        assert_ne!(
            stored_hash, "stale_hash",
            "stale record should have been rehashed"
        );
    }

    // -----------------------------------------------------------------------
    // compute_directory_hashes
    // -----------------------------------------------------------------------

    #[test]
    fn test_compute_directory_hashes_stores_all_dirs() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("a.txt"), "hello").unwrap();

        let conn = open_test_db();
        db::init_visited_files(&conn).unwrap();
        let mut files_by_dir = HashMap::new();
        scan_files(&conn, dir.path(), 1, &mut files_by_dir).unwrap();
        compute_directory_hashes(&conn, dir.path(), &files_by_dir).unwrap();

        let dir_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM directories", [], |r| r.get(0))
            .unwrap();
        assert!(
            dir_count >= 2,
            "root and sub should both be in directories table"
        );
    }

    #[test]
    fn test_compute_directory_hashes_parent_reflects_child_content() {
        // Changing a file in a child directory must change the parent's hash too.
        let root = tempdir().unwrap();
        let sub = root.path().join("sub");
        fs::create_dir(&sub).unwrap();

        let get_root_hash = |content: &str| -> String {
            fs::write(sub.join("file.txt"), content).unwrap();
            let conn = open_test_db();
            db::init_visited_files(&conn).unwrap();
            let mut fbd = HashMap::new();
            scan_files(&conn, root.path(), 1, &mut fbd).unwrap();
            compute_directory_hashes(&conn, root.path(), &fbd).unwrap();
            get_dir_hash(&conn, root.path())
        };

        let hash_a = get_root_hash("content A");
        let hash_b = get_root_hash("content B");
        assert_ne!(
            hash_a, hash_b,
            "parent hash must change when child content changes"
        );
    }

    // -----------------------------------------------------------------------
    // scan_directory (integration)
    // -----------------------------------------------------------------------

    #[test]
    fn test_scan_stores_correct_file_hash_and_size() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();

        let conn = open_test_db();
        scan_directory(&conn, dir.path(), 1, false).unwrap();

        let record = db::get_file(&conn, &dir.path().join("a.txt"))
            .unwrap()
            .expect("file should be in DB");
        assert_eq!(record.size, 5);
        assert_eq!(
            record.hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_scan_identical_dirs_get_same_hash() {
        let root = tempdir().unwrap();
        let dir_a = root.path().join("a");
        let dir_b = root.path().join("b");
        fs::create_dir(&dir_a).unwrap();
        fs::create_dir(&dir_b).unwrap();
        fs::write(dir_a.join("file.txt"), "same content").unwrap();
        fs::write(dir_b.join("file.txt"), "same content").unwrap();

        let conn = open_test_db();
        scan_directory(&conn, root.path(), 2, false).unwrap();

        let hash_a = get_dir_hash(&conn, &dir_a);
        let hash_b = get_dir_hash(&conn, &dir_b);
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn test_scan_different_dirs_get_different_hash() {
        let root = tempdir().unwrap();
        let dir_a = root.path().join("a");
        let dir_b = root.path().join("b");
        fs::create_dir(&dir_a).unwrap();
        fs::create_dir(&dir_b).unwrap();
        fs::write(dir_a.join("file.txt"), "content A").unwrap();
        fs::write(dir_b.join("file.txt"), "content B").unwrap();

        let conn = open_test_db();
        scan_directory(&conn, root.path(), 2, false).unwrap();

        let hash_a = get_dir_hash(&conn, &dir_a);
        let hash_b = get_dir_hash(&conn, &dir_b);
        assert_ne!(hash_a, hash_b);
    }

    #[test]
    fn test_scan_stale_files_not_deleted_when_prompt_false() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();

        let conn = open_test_db();
        scan_directory(&conn, dir.path(), 1, false).unwrap();

        // Insert a ghost record for a file that doesn't exist on disk
        let ghost = dir.path().join("ghost.txt");
        insert_ghost_file(&conn, &ghost);

        // Rescan with prompt_stale=false — ghost record should survive
        scan_directory(&conn, dir.path(), 1, false).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE path = ?1",
                rusqlite::params![ghost.to_str().unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "stale file should not be deleted when prompt_stale=false"
        );
    }

    #[test]
    fn test_scan_returns_zero_invalid_paths_for_clean_dir() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();

        let conn = open_test_db();
        let invalid = scan_directory(&conn, dir.path(), 1, false).unwrap();
        assert_eq!(invalid, 0);
    }
}
