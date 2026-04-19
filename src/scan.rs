use crate::db::should_update_file;
use crate::hashing::{compute_directory_hash, compute_file_hash, path_to_str};
use anyhow::Result;
use rusqlite::{params, Connection};
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

pub fn scan_directory(
    conn: &Connection,
    root: &Path,
    total_files: usize,
    prompt_stale: bool,
) -> Result<usize> {
    let mut files_by_dir: HashMap<PathBuf, Vec<FileEntry>> = HashMap::new();
    let mut processed = 0;
    let mut invalid_paths = 0usize;

    // Create a temp table to track all files seen in this scan
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS visited_files (path TEXT PRIMARY KEY);
         DELETE FROM visited_files;",
    )?;

    // First pass: scan all files
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
            conn.execute(
                "INSERT OR IGNORE INTO visited_files (path) VALUES (?1)",
                params![path_str],
            )?;

            // Check if we need to update this file
            if should_update_file(conn, path, modified)? {
                match compute_file_hash(path) {
                    Ok(hash) => {
                        let modified_secs =
                            modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;

                        conn.execute(
                            "INSERT OR REPLACE INTO files (path, hash, size, modified) VALUES (?1, ?2, ?3, ?4)",
                            params![path_str, hash, size as i64, modified_secs],
                        )?;

                        if let Some(parent) = path.parent() {
                            files_by_dir
                                .entry(parent.to_path_buf())
                                .or_insert_with(Vec::new)
                                .push(FileEntry {
                                    path: path_str,
                                    hash,
                                    size,
                                });
                        }
                    }
                    Err(e) => {
                        eprintln!("Error hashing file {:?}: {}", path, e);
                    }
                }
            } else {
                // File hasn't changed, load from database
                let mut stmt = conn.prepare("SELECT hash, size FROM files WHERE path = ?1")?;
                let (hash, size): (String, i64) =
                    stmt.query_row(params![path_str], |row| Ok((row.get(0)?, row.get(1)?)))?;

                if let Some(parent) = path.parent() {
                    files_by_dir
                        .entry(parent.to_path_buf())
                        .or_insert_with(Vec::new)
                        .push(FileEntry {
                            path: path_str,
                            hash,
                            size: size as u64,
                        });
                }
            }
        }
    }

    // Find files in DB under root that were not seen in this scan.
    // LEFT JOIN is used instead of NOT IN so SQLite can use the PRIMARY KEY index
    // on visited_files, avoiding an O(n²) scan with large file sets.
    let root_str = path_to_str(root)?.to_string();
    println!("\nChecking for stale database entries (this may take several minutes for large directories)...");
    io::stdout().flush()?;
    let stale_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files
         LEFT JOIN visited_files ON files.path = visited_files.path
         WHERE files.path LIKE ?1 AND visited_files.path IS NULL",
        params![format!("{}%", root_str)],
        |row| row.get(0),
    )?;

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
            conn.execute(
                "DELETE FROM files WHERE path LIKE ?1 AND path IN (
                     SELECT files.path FROM files
                     LEFT JOIN visited_files ON files.path = visited_files.path
                     WHERE files.path LIKE ?1 AND visited_files.path IS NULL
                 )",
                params![format!("{}%", root_str)],
            )?;
            println!("Deleted {} stale file(s) from the database.", stale_count);
        } else if prompt_stale {
            println!("Skipped deletion of stale entries.");
        }
    }

    // Second pass: compute directory hashes bottom-up
    let mut dir_entries: Vec<PathBuf> = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.path().to_path_buf())
        .collect();

    // Sort by depth (deepest first) to ensure bottom-up processing
    dir_entries.sort_by(|a, b| b.components().count().cmp(&a.components().count()));

    for dir_path in dir_entries {
        compute_directory_hash(conn, &dir_path, &files_by_dir)?;
    }

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
    use crate::db::setup_schema;
    use rusqlite::Connection;
    use std::fs;
    use tempfile::tempdir;

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        setup_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn test_scan_new_files_are_hashed() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        fs::write(dir.path().join("b.txt"), "world").unwrap();

        let conn = open_test_db();
        let invalid = scan_directory(&conn, dir.path(), 2, false).unwrap();
        assert_eq!(invalid, 0);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_scan_directory_hash_stored() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();

        let conn = open_test_db();
        scan_directory(&conn, dir.path(), 1, false).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM directories", [], |r| r.get(0))
            .unwrap();
        assert!(count >= 1);
    }

    #[test]
    fn test_scan_identical_dirs_get_same_hash() {
        // Two directories with identical contents should produce the same directory hash
        let root = tempdir().unwrap();
        let dir_a = root.path().join("a");
        let dir_b = root.path().join("b");
        fs::create_dir(&dir_a).unwrap();
        fs::create_dir(&dir_b).unwrap();
        fs::write(dir_a.join("file.txt"), "same content").unwrap();
        fs::write(dir_b.join("file.txt"), "same content").unwrap();

        let conn = open_test_db();
        scan_directory(&conn, root.path(), 2, false).unwrap();

        let hash_a: String = conn
            .query_row(
                "SELECT hash FROM directories WHERE path = ?1",
                rusqlite::params![dir_a.to_str().unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        let hash_b: String = conn
            .query_row(
                "SELECT hash FROM directories WHERE path = ?1",
                rusqlite::params![dir_b.to_str().unwrap()],
                |r| r.get(0),
            )
            .unwrap();

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

        let hash_a: String = conn
            .query_row(
                "SELECT hash FROM directories WHERE path = ?1",
                rusqlite::params![dir_a.to_str().unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        let hash_b: String = conn
            .query_row(
                "SELECT hash FROM directories WHERE path = ?1",
                rusqlite::params![dir_b.to_str().unwrap()],
                |r| r.get(0),
            )
            .unwrap();

        assert_ne!(hash_a, hash_b);
    }
}
