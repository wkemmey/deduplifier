use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use walkdir::WalkDir;

use crate::{db, file_system, hashing, similar, utils};

const SIMILARITY_WARN_THRESHOLD: f64 = 0.85;

/// Merge each source directory in `sources` into `canon` sequentially.
/// Requires `--delete` to have been validated by the caller.
/// `no_confirmation`: if true, resolve name conflicts automatically (keep newest);
///                    if false, prompt the user.
pub fn merge_into_canon(
    conn: &Connection,
    canon: &Path,
    sources: &[&Path],
    no_confirmation: bool,
) -> Result<()> {
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());

    for &source in sources {
        if source == canon {
            println!(
                "Skipping: source is the same as canon ({}).",
                source.display()
            );
            continue;
        }
        println!(
            "\n--- Merging {} into {} ---",
            source.display(),
            canon.display()
        );
        merge_one(conn, canon, source, no_confirmation, &mut reader)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-source merge
// ---------------------------------------------------------------------------

fn merge_one(
    conn: &Connection,
    canon: &Path,
    source: &Path,
    no_confirmation: bool,
    reader: &mut impl BufRead,
) -> Result<()> {
    // Build an in-memory index of both trees for scoring.
    // We pass both roots so the index covers exactly what we need.
    let both: &[&Path] = &[canon, source];
    let dir_index = similar::build_dir_index(conn, both)?;

    let canon_str = canon.to_string_lossy().to_string();
    let source_str = source.to_string_lossy().to_string();

    // Compute similarity score (Jaccard on file hashes, full recursive tree)
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

    println!(
        "  Similarity to canon: {:.1}%  ({} shared hashes, {} total unique)",
        score * 100.0,
        intersection,
        union,
    );

    if score < SIMILARITY_WARN_THRESHOLD {
        print!(
            "  Warning: similarity is below {:.0}%. Merge anyway? [y/N] > ",
            SIMILARITY_WARN_THRESHOLD * 100.0
        );
        io::stdout().flush()?;
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if !line.trim().eq_ignore_ascii_case("y") {
            println!("  Skipped.");
            return Ok(());
        }
    }

    // Walk the source tree and process every file
    // Snapshot the file list before we start moving things
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
        // Compute relative path from source root
        let rel = match src_abs.strip_prefix(source) {
            Ok(r) => r,
            Err(_) => {
                eprintln!(
                    "  Warning: could not compute relative path for {}",
                    src_abs.display()
                );
                continue;
            }
        };

        let dest_abs = canon.join(rel);

        if dest_abs.exists() {
            // Conflict: same relative path already exists in canon
            // First check if they're identical
            let src_hash = hash_file(src_abs)?;
            let dest_hash = hash_file(&dest_abs)?;

            if src_hash == dest_hash {
                // True duplicate — silently delete source
                file_system::delete_file(src_abs)?;
                db::remove_file(conn, src_abs)?;
                deleted_dups += 1;
                continue;
            }

            // Different content — resolve conflict
            let keep_source = if no_confirmation {
                utils::mtime(src_abs)? > utils::mtime(&dest_abs)?
            } else {
                let src_mtime = utils::mtime(src_abs)?;
                let dest_mtime = utils::mtime(&dest_abs)?;
                let choice =
                    prompt_conflict(reader, rel, &dest_abs, dest_mtime, src_abs, src_mtime)?;
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
                // Overwrite canon with source, delete source original
                file_system::copy_file(src_abs, &dest_abs)?;
                file_system::delete_file(src_abs)?;
                db::remove_file(conn, src_abs)?;
                db::update_file_hash(conn, &dest_abs, &src_hash)?;
                println!("  Conflict (kept source): {}", rel.display());
            } else {
                // Keep canon, delete source
                file_system::delete_file(src_abs)?;
                db::remove_file(conn, src_abs)?;
                println!("  Conflict (kept canon): {}", rel.display());
            }
            moved += 1;
        } else {
            // No conflict — move file into canon
            file_system::move_file(src_abs, &dest_abs)?;
            db::move_file(conn, src_abs, &dest_abs)?;
            println!("  Moved: {}", rel.display());
            moved += 1;
        }
    }

    println!(
        "  Done: {} file(s) moved/resolved, {} true duplicate(s) removed, {} skipped.",
        moved, deleted_dups, skipped
    );

    file_system::delete_empty_subdirs(source)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Conflict prompt
// ---------------------------------------------------------------------------

enum ConflictChoice {
    KeepCanon,
    KeepSource,
    Skip,
}

fn prompt_conflict(
    reader: &mut impl BufRead,
    rel: &Path,
    canon_path: &Path,
    canon_mtime: i64,
    source_path: &Path,
    source_mtime: i64,
) -> Result<ConflictChoice> {
    println!("  Conflict: {}", rel.display());
    println!(
        "    [1] keep canon  {} ({})",
        canon_path.display(),
        utils::fmt_mtime(canon_mtime)
    );
    println!(
        "    [2] keep source {} ({})",
        source_path.display(),
        utils::fmt_mtime(source_mtime)
    );
    loop {
        print!("    [1] keep canon  [2] keep source  [s] skip > ");
        io::stdout().flush()?;
        let mut line = String::new();
        reader.read_line(&mut line)?;
        match line.trim().to_ascii_lowercase().as_str() {
            "1" => return Ok(ConflictChoice::KeepCanon),
            "2" => return Ok(ConflictChoice::KeepSource),
            "s" => return Ok(ConflictChoice::Skip),
            _ => println!("    Please enter 1, 2, or s."),
        }
    }
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

    /// Returns a BufRead that answers "y" to any prompt.
    fn yes_reader() -> io::Cursor<&'static [u8]> {
        io::Cursor::new(b"y\n")
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

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

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

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

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

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

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

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

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

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

        assert!(!album.exists(), "empty album subdir should be deleted");
    }

    #[test]
    fn test_merge_skips_self() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        fs::create_dir_all(&canon).unwrap();
        let conn = open_test_db();

        let sources: &[&Path] = &[canon.as_path()];
        merge_into_canon(&conn, &canon, sources, true).unwrap();
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

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

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
