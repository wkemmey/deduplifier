use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use walkdir::WalkDir;

use crate::{db, file_system, hashing, similar, utils};

const SIMILARITY_WARN_THRESHOLD: f64 = 0.85;

/// Stats returned after merging one source into canon.
pub struct MergeStats {
    pub moved: usize,
    pub deleted_dups: usize,
    pub skipped: usize,
}

/// Low-similarity sentinel: callers should check `stats.score` and decide
/// whether to proceed before calling `execute_merge`.
pub const SIMILARITY_THRESHOLD: f64 = SIMILARITY_WARN_THRESHOLD;

/// Compute similarity score between `canon` and `source` without moving any files.
pub fn similarity_score(
    conn: &Connection,
    canon: &Path,
    source: &Path,
) -> Result<(f64, usize, usize)> {
    let both: &[&Path] = &[canon, source];
    let dir_index = similar::build_dir_index(conn, both)?;
    let canon_str = canon.to_string_lossy().to_string();
    let source_str = source.to_string_lossy().to_string();
    let canon_files = similar::files_for_dir(&dir_index, &canon_str);
    let source_files = similar::files_for_dir(&dir_index, &source_str);
    let canon_hashes: HashSet<&str> = canon_files.values().map(|(_, h, _)| h.as_str()).collect();
    let source_hashes: HashSet<&str> = source_files.values().map(|(_, h, _)| h.as_str()).collect();
    let intersection = canon_hashes.intersection(&source_hashes).count();
    let union = canon_hashes.union(&source_hashes).count();
    let score = if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    };
    Ok((score, intersection, union))
}

/// Merge `source` into `canon`, resolving conflicts via `on_conflict`.
/// `on_conflict(rel, dest_abs, dest_mtime, src_abs, src_mtime) -> Result<ConflictChoice>`
pub fn execute_merge(
    conn: &Connection,
    canon: &Path,
    source: &Path,
    no_confirmation: bool,
    on_conflict: impl Fn(&Path, &Path, i64, &Path, i64) -> Result<ConflictChoice>,
) -> Result<MergeStats> {
    merge_one(conn, canon, source, no_confirmation, on_conflict)
}

// ---------------------------------------------------------------------------
// Per-source merge
// ---------------------------------------------------------------------------

fn merge_one(
    conn: &Connection,
    canon: &Path,
    source: &Path,
    no_confirmation: bool,
    on_conflict: impl Fn(&Path, &Path, i64, &Path, i64) -> Result<ConflictChoice>,
) -> Result<MergeStats> {
    let source_files_on_disk: Vec<PathBuf> = WalkDir::new(source)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();

    let mut moved = 0usize;
    let mut deleted_dups = 0usize;
    let mut skipped = 0usize;

    for src_abs in &source_files_on_disk {
        let rel = match src_abs.strip_prefix(source) {
            Ok(r) => r,
            Err(_) => continue,
        };

        let dest_abs = canon.join(rel);

        if dest_abs.exists() {
            let src_hash = hash_file(src_abs)?;
            let dest_hash = hash_file(&dest_abs)?;

            if src_hash == dest_hash {
                file_system::delete_file(src_abs)?;
                db::remove_file(conn, src_abs)?;
                deleted_dups += 1;
                continue;
            }

            let keep_source = if no_confirmation {
                utils::mtime(src_abs)? > utils::mtime(&dest_abs)?
            } else {
                let src_mtime = utils::mtime(src_abs)?;
                let dest_mtime = utils::mtime(&dest_abs)?;
                let choice = on_conflict(rel, &dest_abs, dest_mtime, src_abs, src_mtime)?;
                match choice {
                    ConflictChoice::KeepCanon => false,
                    ConflictChoice::KeepSource => true,
                    ConflictChoice::Skip => {
                        skipped += 1;
                        continue;
                    }
                }
            };

            if keep_source {
                file_system::copy_file(src_abs, &dest_abs)?;
                file_system::delete_file(src_abs)?;
                db::remove_file(conn, src_abs)?;
                db::update_file_hash(conn, &dest_abs, &src_hash)?;
            } else {
                file_system::delete_file(src_abs)?;
                db::remove_file(conn, src_abs)?;
            }
            moved += 1;
        } else {
            file_system::move_file(src_abs, &dest_abs)?;
            db::move_file(conn, src_abs, &dest_abs)?;
            moved += 1;
        }
    }

    file_system::delete_empty_subdirs(source)?;

    Ok(MergeStats {
        moved,
        deleted_dups,
        skipped,
    })
}

// ---------------------------------------------------------------------------
// Conflict choice type (used by ui::prompt_merge_conflict)
// ---------------------------------------------------------------------------

pub enum ConflictChoice {
    KeepCanon,
    KeepSource,
    Skip,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hash_file(path: &Path) -> Result<String> {
    hashing::compute_file_hash(path).with_context(|| format!("hashing {}", path.display()))
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
    use crate::db;
    use rusqlite::Connection;
    use std::fs;
    use tempfile::tempdir;

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        db::setup_schema(&conn).unwrap();
        conn
    }

    fn write_file(path: &Path, content: &[u8]) {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    #[test]
    fn test_merge_moves_unique_source_file() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        let source = dir.path().join("source");
        fs::create_dir_all(&canon).unwrap();
        fs::create_dir_all(&source).unwrap();
        let conn = open_test_db();

        let src_file = source.join("photo.jpg");
        write_file(&src_file, b"unique photo");
        db::upsert_file(&conn, &src_file, "hash_unique", 12, 0).unwrap();

        merge_one(&conn, &canon, &source, true, |_, _, _, _, _| unreachable!()).unwrap();

        assert!(canon.join("photo.jpg").exists(), "file should be in canon");
        assert!(!src_file.exists(), "file should be gone from source");
        // DB record should now point to canon
        assert!(db::get_file(&conn, &canon.join("photo.jpg"))
            .unwrap()
            .is_some());
        assert!(db::get_file(&conn, &src_file).unwrap().is_none());
    }

    #[test]
    fn test_merge_removes_true_duplicate() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        let source = dir.path().join("source");
        let conn = open_test_db();

        let content = b"identical content";
        write_file(&canon.join("photo.jpg"), content);
        write_file(&source.join("photo.jpg"), content);
        db::upsert_file(&conn, &source.join("photo.jpg"), "hash_same", 17, 0).unwrap();

        merge_one(&conn, &canon, &source, true, |_, _, _, _, _| unreachable!()).unwrap();

        assert!(canon.join("photo.jpg").exists());
        assert!(!source.join("photo.jpg").exists());
        // DB record for source should be gone
        assert!(db::get_file(&conn, &source.join("photo.jpg"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_merge_conflict_no_confirmation_keeps_newest() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        let source = dir.path().join("source");
        let conn = open_test_db();

        write_file(&canon.join("photo.jpg"), b"canon version");
        let src = source.join("photo.jpg");
        write_file(&src, b"source version");
        db::upsert_file(&conn, &src, "hash_source", 14, 0).unwrap();

        merge_one(&conn, &canon, &source, true, |_, _, _, _, _| unreachable!()).unwrap();

        assert!(canon.join("photo.jpg").exists());
        assert!(!src.exists());
    }

    #[test]
    fn test_merge_preserves_subdir_structure() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        let source = dir.path().join("source");
        fs::create_dir_all(&canon).unwrap();
        let conn = open_test_db();

        let src_file = source.join("2009").join("jan").join("img.jpg");
        write_file(&src_file, b"nested photo");
        db::upsert_file(&conn, &src_file, "hash_nested", 12, 0).unwrap();

        merge_one(&conn, &canon, &source, true, |_, _, _, _, _| unreachable!()).unwrap();

        assert!(canon.join("2009").join("jan").join("img.jpg").exists());
        assert!(!src_file.exists());
    }

    #[test]
    fn test_merge_deletes_empty_source_subdir() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        let source = dir.path().join("source");
        fs::create_dir_all(&canon).unwrap();
        let conn = open_test_db();

        let album = source.join("album");
        let src_file = album.join("img.jpg");
        write_file(&src_file, b"photo");
        db::upsert_file(&conn, &src_file, "hash_x", 5, 0).unwrap();

        merge_one(&conn, &canon, &source, true, |_, _, _, _, _| unreachable!()).unwrap();

        assert!(!album.exists(), "empty album subdir should be deleted");
    }

    #[test]
    fn test_merge_skips_self() {
        // execute_merge on canon==source should just do nothing (no files to move)
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        fs::create_dir_all(&canon).unwrap();
        let conn = open_test_db();
        // Merging into itself: source has no files, so nothing happens
        execute_merge(&conn, &canon, &canon, true, |_, _, _, _, _| unreachable!()).unwrap();
    }

    #[test]
    fn test_merge_db_record_moves_with_file() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        let source = dir.path().join("source");
        fs::create_dir_all(&canon).unwrap();
        let conn = open_test_db();

        let src_file = source.join("img.jpg");
        write_file(&src_file, b"data");
        db::upsert_file(&conn, &src_file, "hash_abc", 4, 0).unwrap();

        merge_one(&conn, &canon, &source, true, |_, _, _, _, _| unreachable!()).unwrap();

        let dest = canon.join("img.jpg");
        let rec = db::get_file(&conn, &dest).unwrap();
        assert!(rec.is_some(), "DB record should exist at new path");
        assert_eq!(rec.unwrap().hash, "hash_abc");
        assert!(
            db::get_file(&conn, &src_file).unwrap().is_none(),
            "old DB record should be gone"
        );
    }
}
