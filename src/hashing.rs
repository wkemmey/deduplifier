use anyhow::Result;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::scan::FileEntry;

/// Convert a Path to a &str, returning a clear error if the path contains invalid UTF-8.
/// Files with invalid paths are skipped (not added to the DB) so the scan can continue.
pub fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        anyhow::anyhow!(
            "Path contains invalid UTF-8 characters: {}",
            path.to_string_lossy()
        )
    })
}

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
    let mut items = Vec::new();

    // Get immediate child files
    if let Some(files) = files_by_dir.get(dir_path) {
        for file in files {
            // Use just the filename, not the full path
            if let Some(filename) = Path::new(&file.path).file_name() {
                items.push((
                    filename.to_string_lossy().to_string(),
                    file.hash.clone(),
                    file.size,
                ));
            }
        }
    }

    // Get immediate child directories from database
    let dir_path_str = path_to_str(dir_path)?.to_string();
    let mut stmt = conn.prepare(
        "SELECT path, hash, size FROM directories WHERE path LIKE ?1 AND path NOT LIKE ?2",
    )?;
    let pattern1 = format!("{}%", dir_path_str);
    let pattern2 = format!("{}%/%", dir_path_str);
    let dir_iter = stmt.query_map(params![pattern1, pattern2], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;

    for dir in dir_iter {
        let (path, hash, size) = dir?;
        let child_path = PathBuf::from(&path);
        if let Some(parent) = child_path.parent() {
            if parent == dir_path {
                // Use just the directory name, not the full path
                if let Some(dirname) = child_path.file_name() {
                    items.push((dirname.to_string_lossy().to_string(), hash, size as u64));
                }
            }
        }
    }

    // Sort items by name for repeatability
    items.sort_by(|a, b| a.0.cmp(&b.0));

    // Compute combined hash using only relative names and content hashes
    let mut hasher = Sha256::new();
    let mut total_size = 0u64;
    for (name, hash, size) in &items {
        hasher.update(name.as_bytes());
        hasher.update(b":");
        hasher.update(hash.as_bytes());
        hasher.update(b"\n");
        total_size += size;
    }
    let result = hasher.finalize();
    let dir_hash = format!("{:x}", result);

    conn.execute(
        "INSERT OR REPLACE INTO directories (path, hash, size) VALUES (?1, ?2, ?3)",
        params![dir_path_str, dir_hash, total_size as i64],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_path_to_str_valid() {
        let path = std::path::Path::new("/some/valid/path.txt");
        assert_eq!(path_to_str(path).unwrap(), "/some/valid/path.txt");
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
}
