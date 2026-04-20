use anyhow::Result;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::db;
use crate::scan::FileEntry;

pub fn count_files(root: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if entry.path().is_file() {
            count += 1;
        }
    }
    Ok(count)
}

pub fn compute_file_hash(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

pub fn compute_directory_hash(
    conn: &Connection,
    dir_path: &Path,
    files_by_dir: &HashMap<PathBuf, Vec<FileEntry>>,
) -> Result<()> {
    // child files and directories in dir_path, as (name, hash, size) tuples
    let mut children = Vec::new();

    // Get immediate child files from the in-memory scan structure, not the DB —
    // this ensures we hash the files just scanned, not stale data from a prior run.
    if let Some(files) = files_by_dir.get(dir_path) {
        for file in files {
            // Use just the filename, not the full path, so hash doesn't depend on directory tree
            if let Some(filename) = Path::new(&file.path).file_name() {
                children.push((
                    filename.to_string_lossy().to_string(),
                    file.hash.clone(),
                    file.size,
                ));
            }
        }
    }

    // Get immediate child directories from the DB — the caller traverses bottom-up,
    // so child directory hashes are already committed before we hash the parent.
    for child in db::child_directories(conn, dir_path)? {
        let child_path = PathBuf::from(&child.path);
        if let Some(dirname) = child_path.file_name() {
            children.push((dirname.to_string_lossy().to_string(), child.hash, child.size as u64));
        }
    }

    // Sort children by name for repeatability
    children.sort_by(|a, b| a.0.cmp(&b.0));

    // Compute combined hash using only relative names and content hashes
    let mut hasher = Sha256::new();
    let mut total_size = 0u64;
    for (name, hash, size) in &children {
        hasher.update(name.as_bytes());
        hasher.update(b":");
        hasher.update(hash.as_bytes());
        hasher.update(b"\n");
        total_size += size;
    }
    let dir_hash = format!("{:x}", hasher.finalize());

    db::upsert_directory(conn, dir_path, &dir_hash, total_size as i64)?;

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
    use crate::db::{setup_schema, upsert_directory, directories_with_hash, EMPTY_DIR_HASH};
    use std::fs;
    use tempfile::tempdir;

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        setup_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn test_count_files_empty_dir() {
        let dir = tempdir().unwrap();
        assert_eq!(count_files(dir.path()).unwrap(), 0);
    }

    #[test]
    fn test_count_files_with_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        fs::write(dir.path().join("b.txt"), "world").unwrap();
        assert_eq!(count_files(dir.path()).unwrap(), 2);
    }

    #[test]
    fn test_count_files_nested() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        fs::write(sub.join("b.txt"), "world").unwrap();
        assert_eq!(count_files(dir.path()).unwrap(), 2);
    }

    #[test]
    fn test_compute_file_hash_known_value() {
        // SHA-256 of "hello world" (no newline) — verifies we produce the correct hash, not just a consistent one
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.txt");
        fs::write(&file, "hello world").unwrap();
        assert_eq!(
            compute_file_hash(&file).unwrap(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_compute_file_hash_deterministic() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.txt");
        fs::write(&file, "hello world").unwrap();
        let hash1 = compute_file_hash(&file).unwrap();
        let hash2 = compute_file_hash(&file).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_compute_file_hash_different_contents() {
        let dir = tempdir().unwrap();
        let f1 = dir.path().join("a.txt");
        let f2 = dir.path().join("b.txt");
        fs::write(&f1, "hello").unwrap();
        fs::write(&f2, "world").unwrap();
        assert_ne!(
            compute_file_hash(&f1).unwrap(),
            compute_file_hash(&f2).unwrap()
        );
    }

    #[test]
    fn test_compute_file_hash_same_contents() {
        let dir = tempdir().unwrap();
        let f1 = dir.path().join("a.txt");
        let f2 = dir.path().join("b.txt");
        fs::write(&f1, "same content").unwrap();
        fs::write(&f2, "same content").unwrap();
        assert_eq!(
            compute_file_hash(&f1).unwrap(),
            compute_file_hash(&f2).unwrap()
        );
    }

    // -----------------------------------------------------------------------
    // compute_directory_hash
    // -----------------------------------------------------------------------

    #[test]
    fn test_compute_directory_hash_empty_dir_produces_empty_hash() {
        let conn = open_test_db();
        let dir = tempdir().unwrap();
        let files_by_dir = HashMap::new();

        compute_directory_hash(&conn, dir.path(), &files_by_dir).unwrap();

        let records = directories_with_hash(&conn, EMPTY_DIR_HASH).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].size, 0);
    }

    #[test]
    fn test_compute_directory_hash_deterministic() {
        // Same inputs in two independent DBs must produce the same hash.
        let dir = tempdir().unwrap();
        let mut files_by_dir = HashMap::new();
        files_by_dir.insert(
            dir.path().to_path_buf(),
            vec![
                FileEntry { path: dir.path().join("a.txt").to_str().unwrap().to_string(), hash: "hash_a".to_string(), size: 10 },
                FileEntry { path: dir.path().join("b.txt").to_str().unwrap().to_string(), hash: "hash_b".to_string(), size: 20 },
            ],
        );

        let conn1 = open_test_db();
        compute_directory_hash(&conn1, dir.path(), &files_by_dir).unwrap();
        let hash1: String = conn1.query_row("SELECT hash FROM directories LIMIT 1", [], |r| r.get(0)).unwrap();

        let conn2 = open_test_db();
        compute_directory_hash(&conn2, dir.path(), &files_by_dir).unwrap();
        let hash2: String = conn2.query_row("SELECT hash FROM directories LIMIT 1", [], |r| r.get(0)).unwrap();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_compute_directory_hash_different_files_different_hash() {
        let conn1 = open_test_db();
        let conn2 = open_test_db();
        let dir = tempdir().unwrap();

        let mut files_a = HashMap::new();
        files_a.insert(
            dir.path().to_path_buf(),
            vec![FileEntry { path: dir.path().join("a.txt").to_str().unwrap().to_string(), hash: "hash_a".to_string(), size: 10 }],
        );
        let mut files_b = HashMap::new();
        files_b.insert(
            dir.path().to_path_buf(),
            vec![FileEntry { path: dir.path().join("a.txt").to_str().unwrap().to_string(), hash: "hash_different".to_string(), size: 10 }],
        );

        compute_directory_hash(&conn1, dir.path(), &files_a).unwrap();
        compute_directory_hash(&conn2, dir.path(), &files_b).unwrap();

        let hash1: String = conn1.query_row("SELECT hash FROM directories LIMIT 1", [], |r| r.get(0)).unwrap();
        let hash2: String = conn2.query_row("SELECT hash FROM directories LIMIT 1", [], |r| r.get(0)).unwrap();
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_compute_directory_hash_ignores_grandchildren() {
        // A grandchild dir should NOT affect the parent's hash — only immediate children do.
        let conn1 = open_test_db();
        let conn2 = open_test_db();
        let dir = tempdir().unwrap();
        let child = dir.path().join("child");
        let grandchild = child.join("grandchild");

        let files_by_dir = HashMap::new();

        // conn1: child dir only
        upsert_directory(&conn1, &child, "child_hash", 50).unwrap();
        compute_directory_hash(&conn1, dir.path(), &files_by_dir).unwrap();

        // conn2: child dir + grandchild dir (grandchild should not change parent hash)
        upsert_directory(&conn2, &child, "child_hash", 50).unwrap();
        upsert_directory(&conn2, &grandchild, "grandchild_hash", 25).unwrap();
        compute_directory_hash(&conn2, dir.path(), &files_by_dir).unwrap();

        let hash1: String = conn1.query_row(
            "SELECT hash FROM directories WHERE path = ?1",
            rusqlite::params![dir.path().to_str().unwrap()],
            |r| r.get(0),
        ).unwrap();
        let hash2: String = conn2.query_row(
            "SELECT hash FROM directories WHERE path = ?1",
            rusqlite::params![dir.path().to_str().unwrap()],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(hash1, hash2, "grandchild should not affect parent directory hash");
    }

    #[test]
    fn test_compute_directory_hash_size_is_sum_of_children() {
        let conn = open_test_db();
        let dir = tempdir().unwrap();

        let mut files_by_dir = HashMap::new();
        files_by_dir.insert(
            dir.path().to_path_buf(),
            vec![
                FileEntry { path: dir.path().join("a.txt").to_str().unwrap().to_string(), hash: "h1".to_string(), size: 100 },
                FileEntry { path: dir.path().join("b.txt").to_str().unwrap().to_string(), hash: "h2".to_string(), size: 200 },
            ],
        );

        compute_directory_hash(&conn, dir.path(), &files_by_dir).unwrap();

        let size: i64 = conn.query_row("SELECT size FROM directories LIMIT 1", [], |r| r.get(0)).unwrap();
        assert_eq!(size, 300);
    }
}
