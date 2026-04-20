use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

// ---------------------------------------------------------------------------
// File operations
// ---------------------------------------------------------------------------

/// Move a file from `from` to `to`, creating parent directories as needed.
/// On the same filesystem (drive or volume) this is an atomic rename; across
/// filesystems it falls back to copy-then-delete.
pub fn move_file(from: &Path, to: &Path) -> Result<()> {
    ensure_parent_exists(to)?;
    fs::rename(from, to)
        .with_context(|| format!("moving {} -> {}", from.display(), to.display()))
}

/// Copy a file from `src` to `dst`, creating parent directories as needed.
/// The original is left in place.
pub fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    ensure_parent_exists(dst)?;
    fs::copy(src, dst)
        .with_context(|| format!("copying {} -> {}", src.display(), dst.display()))?;
    Ok(())
}

/// Delete a single file.
pub fn delete_file(path: &Path) -> Result<()> {
    fs::remove_file(path)
        .with_context(|| format!("deleting file {}", path.display()))
}

// ---------------------------------------------------------------------------
// Directory operations
// ---------------------------------------------------------------------------

/// Create `path` and all missing parent directories.
pub fn ensure_dir_exists(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("creating directory {}", path.display()))
}

/// Remove `path` only if it is empty.
/// Returns `true` if the directory was removed, `false` if it was non-empty
/// (so the caller can decide whether to warn). Any other error is propagated.
pub fn delete_dir_if_empty(path: &Path) -> Result<bool> {
    match fs::remove_dir(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => Ok(false),
        Err(e) => Err(e).with_context(|| format!("removing directory {}", path.display())),
    }
}

/// Recursively delete `path` and everything inside it.
pub fn delete_dir_all(path: &Path) -> Result<()> {
    fs::remove_dir_all(path)
        .with_context(|| format!("removing directory tree {}", path.display()))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Ensure the parent directory of `path` exists, creating it if necessary.
fn ensure_parent_exists(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent directory for {}", path.display()))?;
    }
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
    use std::fs;
    use tempfile::tempdir;

    // -----------------------------------------------------------------------
    // move_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_move_file_basic() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.txt");
        let dst = dir.path().join("b.txt");
        fs::write(&src, b"hello").unwrap();

        move_file(&src, &dst).unwrap();

        assert!(!src.exists());
        assert_eq!(fs::read(&dst).unwrap(), b"hello");
    }

    #[test]
    fn test_move_file_creates_parent() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.txt");
        let dst = dir.path().join("sub/dir/b.txt");
        fs::write(&src, b"hello").unwrap();

        move_file(&src, &dst).unwrap();

        assert!(!src.exists());
        assert!(dst.exists());
    }

    #[test]
    fn test_move_file_missing_source_errors() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("nonexistent.txt");
        let dst = dir.path().join("b.txt");

        assert!(move_file(&src, &dst).is_err());
    }

    // -----------------------------------------------------------------------
    // copy_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_copy_file_basic() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.txt");
        let dst = dir.path().join("b.txt");
        fs::write(&src, b"data").unwrap();

        copy_file(&src, &dst).unwrap();

        assert!(src.exists(), "source should still exist");
        assert_eq!(fs::read(&dst).unwrap(), b"data");
    }

    #[test]
    fn test_copy_file_creates_parent() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.txt");
        let dst = dir.path().join("nested/dir/b.txt");
        fs::write(&src, b"data").unwrap();

        copy_file(&src, &dst).unwrap();

        assert!(dst.exists());
    }

    // -----------------------------------------------------------------------
    // delete_file
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_file_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("a.txt");
        fs::write(&path, b"x").unwrap();

        delete_file(&path).unwrap();

        assert!(!path.exists());
    }

    #[test]
    fn test_delete_file_missing_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.txt");

        assert!(delete_file(&path).is_err());
    }

    // -----------------------------------------------------------------------
    // ensure_dir_exists
    // -----------------------------------------------------------------------

    #[test]
    fn test_ensure_dir_exists_creates_nested() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("a/b/c");

        ensure_dir_exists(&path).unwrap();

        assert!(path.is_dir());
    }

    #[test]
    fn test_ensure_dir_exists_idempotent() {
        let dir = tempdir().unwrap();
        ensure_dir_exists(dir.path()).unwrap();
        ensure_dir_exists(dir.path()).unwrap(); // second call should not error
    }

    // -----------------------------------------------------------------------
    // delete_dir_if_empty
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_dir_if_empty_removes_empty_dir() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("empty");
        fs::create_dir(&target).unwrap();

        assert!(delete_dir_if_empty(&target).unwrap());
        assert!(!target.exists());
    }

    #[test]
    fn test_delete_dir_if_empty_returns_false_for_nonempty() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("nonempty");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("file.txt"), b"x").unwrap();

        assert!(!delete_dir_if_empty(&target).unwrap());
        assert!(target.exists(), "directory should still exist");
    }

    // -----------------------------------------------------------------------
    // delete_dir_all
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_dir_all_removes_tree() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("tree");
        fs::create_dir_all(target.join("sub")).unwrap();
        fs::write(target.join("a.txt"), b"x").unwrap();
        fs::write(target.join("sub/b.txt"), b"y").unwrap();

        delete_dir_all(&target).unwrap();

        assert!(!target.exists());
    }

    #[test]
    fn test_delete_dir_all_missing_errors() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("nonexistent");

        assert!(delete_dir_all(&target).is_err());
    }

    // -----------------------------------------------------------------------
    // ensure_parent_exists
    // -----------------------------------------------------------------------

    #[test]
    fn test_ensure_parent_exists_creates_parents() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("a/b/c/file.txt");

        ensure_parent_exists(&path).unwrap();

        assert!(dir.path().join("a/b/c").is_dir());
    }

    #[test]
    fn test_ensure_parent_exists_no_parent_is_ok() {
        // A path with no parent (e.g. just a filename) should not error.
        ensure_parent_exists(Path::new("file.txt")).unwrap();
    }
}
