use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use walkdir::WalkDir;

// ---------------------------------------------------------------------------
// File operations
// ---------------------------------------------------------------------------

/// Move a file from `from` to `to`, creating parent directories as needed.
/// On the same filesystem this is an atomic rename; across filesystems it
/// falls back to copy-then-delete so cross-volume moves always succeed.
///
/// Note: on Windows, `fs::rename` fails if `to` already exists (unlike Unix,
/// which atomically replaces it). This is safe for our merge use case because
/// we only move files into destinations that don't yet exist, but callers
/// should be aware if that assumption ever changes.
pub fn move_file(from: &Path, to: &Path) -> Result<()> {
    ensure_parent_exists(to)?;
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(_) => {
            // rename failed — likely a cross-device move; fall back to copy-then-delete
            fs::copy(from, to)
                .with_context(|| format!("copying {} -> {} (cross-device move)", from.display(), to.display()))?;
            fs::remove_file(from)
                .with_context(|| format!("deleting source {} after cross-device move", from.display()))
        }
    }
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
    fs::remove_file(path).with_context(|| format!("deleting file {}", path.display()))
}

// ---------------------------------------------------------------------------
// Directory operations
// ---------------------------------------------------------------------------

/// Recursively delete `path` and everything inside it.
pub fn delete_dir_all(path: &Path) -> Result<()> {
    fs::remove_dir_all(path).with_context(|| format!("removing directory tree {}", path.display()))
}

/// Walk `root` bottom-up (deepest subdirectory first) and remove any empty
/// subdirectory. The root itself is never removed.
pub fn delete_empty_subdirs(root: &Path) -> Result<()> {
    let mut dirs: Vec<std::path::PathBuf> = WalkDir::new(root)
        .min_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_dir())
        .map(|e| e.into_path())
        .collect();

    // Deepest first so we remove children before parents
    dirs.sort_by(|a, b| b.components().count().cmp(&a.components().count()));

    for dir in &dirs {
        let Ok(mut entries) = fs::read_dir(dir) else {
            continue;
        };
        if entries.next().is_none() {
            match fs::remove_dir(dir) {
                Ok(()) => println!("  Removed empty dir: {}", dir.display()),
                Err(e) => eprintln!("  Warning: could not remove {}: {}", dir.display(), e),
            }
        }
    }
    Ok(())
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
    // delete_empty_subdirs
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_empty_subdirs_removes_empty_child() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let empty = root.join("empty_sub");
        fs::create_dir(&empty).unwrap();

        delete_empty_subdirs(root).unwrap();

        assert!(!empty.exists(), "empty subdir should be removed");
        assert!(root.exists(), "root should not be removed");
    }

    #[test]
    fn test_delete_empty_subdirs_keeps_nonempty_child() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let sub = root.join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("file.txt"), b"x").unwrap();

        delete_empty_subdirs(root).unwrap();

        assert!(sub.exists(), "non-empty subdir should remain");
    }

    #[test]
    fn test_delete_empty_subdirs_removes_nested_empty() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        // root/a/b — both empty
        fs::create_dir_all(root.join("a/b")).unwrap();

        delete_empty_subdirs(root).unwrap();

        assert!(!root.join("a/b").exists());
        assert!(!root.join("a").exists());
        assert!(root.exists());
    }

    #[test]
    fn test_delete_empty_subdirs_never_removes_root() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        // root is itself empty (no children at all)
        delete_empty_subdirs(root).unwrap();
        assert!(root.exists(), "root must never be deleted");
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
