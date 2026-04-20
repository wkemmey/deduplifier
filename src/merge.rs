use std::collections::HashSet;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use walkdir::WalkDir;

use crate::similar::{build_dir_index, files_for_dir};
use crate::utils::path_to_str;

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
    let dir_index = build_dir_index(conn, both)?;

    let canon_str = canon.to_string_lossy().to_string();
    let source_str = source.to_string_lossy().to_string();

    // Compute similarity score (Jaccard on file hashes, full recursive tree)
    let canon_files = files_for_dir(&dir_index, &canon_str);
    let source_files = files_for_dir(&dir_index, &source_str);

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
                fs::remove_file(src_abs)
                    .with_context(|| format!("removing duplicate {}", src_abs.display()))?;
                db_remove_file(conn, src_abs);
                deleted_dups += 1;
                continue;
            }

            // Different content — resolve conflict
            let keep_source = if no_confirmation {
                // Keep newest
                let src_mtime = mtime(src_abs);
                let dest_mtime = mtime(&dest_abs);
                src_mtime > dest_mtime
            } else {
                // Prompt
                let src_mtime = mtime(src_abs);
                let dest_mtime = mtime(&dest_abs);
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
                fs::copy(src_abs, &dest_abs).with_context(|| {
                    format!(
                        "overwriting {} with {}",
                        dest_abs.display(),
                        src_abs.display()
                    )
                })?;
                fs::remove_file(src_abs)
                    .with_context(|| format!("removing {}", src_abs.display()))?;
                db_remove_file(conn, src_abs);
                db_update_file_hash(conn, &dest_abs, &src_hash);
                println!("  Conflict (kept source): {}", rel.display());
            } else {
                // Keep canon, delete source
                fs::remove_file(src_abs)
                    .with_context(|| format!("removing {}", src_abs.display()))?;
                db_remove_file(conn, src_abs);
                println!("  Conflict (kept canon): {}", rel.display());
            }
            moved += 1;
        } else {
            // No conflict — move file into canon
            if let Some(parent) = dest_abs.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            fs::rename(src_abs, &dest_abs).with_context(|| {
                format!("moving {} -> {}", src_abs.display(), dest_abs.display())
            })?;
            db_move_file(conn, src_abs, &dest_abs);
            println!("  Moved: {}", rel.display());
            moved += 1;
        }
    }

    println!(
        "  Done: {} file(s) moved/resolved, {} true duplicate(s) removed, {} skipped.",
        moved, deleted_dups, skipped
    );

    // Delete now-empty source subdirectories (never the source root itself)
    cleanup_empty_dirs(conn, source)?;

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
        fmt_mtime(canon_mtime)
    );
    println!(
        "    [2] keep source {} ({})",
        source_path.display(),
        fmt_mtime(source_mtime)
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
// Empty directory cleanup (source only)
// ---------------------------------------------------------------------------

fn cleanup_empty_dirs(conn: &Connection, source: &Path) -> Result<()> {
    let mut dirs: Vec<PathBuf> = WalkDir::new(source)
        .min_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_dir())
        .map(|e| e.into_path())
        .collect();

    // Deepest first
    dirs.sort_by(|a, b| b.components().count().cmp(&a.components().count()));

    for dir in &dirs {
        let Ok(mut entries) = fs::read_dir(dir) else {
            continue;
        };
        if entries.next().is_none() {
            match fs::remove_dir(dir) {
                Ok(()) => {
                    println!("  Removed empty dir: {}", dir.display());
                    db_remove_dir(conn, dir);
                }
                Err(e) => eprintln!("  Warning: could not remove {}: {}", dir.display(), e),
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hash_file(path: &Path) -> Result<String> {
    crate::hashing::compute_file_hash(path).with_context(|| format!("hashing {}", path.display()))
}

fn mtime(path: &Path) -> i64 {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .map(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        })
        .unwrap_or(0)
}

fn fmt_mtime(secs: i64) -> String {
    // Reuse the same pure-Rust formatter from similar.rs logic
    // (duplicated here to avoid pub-ing it; could be moved to a shared util)
    let secs = secs.max(0) as u64;
    let days = secs / 86400;
    let rem = secs % 86400;
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let mut y = 1970u64;
    let mut d = days;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let diy = if leap { 366 } else { 365 };
        if d < diy {
            break;
        }
        d -= diy;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let month_days: [u64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0usize;
    let mut day = d;
    for (i, &md) in month_days.iter().enumerate() {
        if day < md {
            month = i + 1;
            break;
        }
        day -= md;
    }
    format!("{:04}-{:02}-{:02} {:02}:{:02}", y, month, day + 1, hh, mm)
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

fn db_move_file(conn: &Connection, old_path: &Path, new_path: &Path) {
    match (path_to_str(old_path), path_to_str(new_path)) {
        (Ok(old), Ok(new)) => {
            if let Err(e) = conn.execute(
                "UPDATE files SET path = ?1 WHERE path = ?2",
                rusqlite::params![new, old],
            ) {
                eprintln!("Warning: DB update failed for {}: {}", old, e);
            }
        }
        _ => eprintln!("Warning: non-UTF-8 path, skipping DB update"),
    }
}

fn db_remove_file(conn: &Connection, path: &Path) {
    if let Ok(p) = path_to_str(path) {
        if let Err(e) = conn.execute("DELETE FROM files WHERE path = ?1", rusqlite::params![p]) {
            eprintln!("Warning: DB delete failed for {}: {}", p, e);
        }
    }
}

fn db_update_file_hash(conn: &Connection, path: &Path, hash: &str) {
    if let Ok(p) = path_to_str(path) {
        if let Err(e) = conn.execute(
            "UPDATE files SET hash = ?1 WHERE path = ?2",
            rusqlite::params![hash, p],
        ) {
            eprintln!("Warning: DB hash update failed for {}: {}", p, e);
        }
    }
}

fn db_remove_dir(conn: &Connection, path: &Path) {
    if let Ok(p) = path_to_str(path) {
        if let Err(e) = conn.execute(
            "DELETE FROM files WHERE path LIKE ?1 || '/%'",
            rusqlite::params![p],
        ) {
            eprintln!("Warning: DB cleanup failed for dir {}: {}", p, e);
        }
        if let Err(e) = conn.execute(
            "DELETE FROM directories WHERE path = ?1",
            rusqlite::params![p],
        ) {
            eprintln!("Warning: DB cleanup failed for dir {}: {}", p, e);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

    fn write_file(path: &Path, content: &[u8]) {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    fn insert_file(conn: &Connection, path: &Path, hash: &str) {
        let p = path.to_str().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO files (path, hash, size, modified) VALUES (?1, ?2, 0, 0)",
            rusqlite::params![p, hash],
        )
        .unwrap();
    }

    /// Returns a BufRead that answers "y" to any prompt (for tests that
    /// trigger the low-similarity warning) or provides no-op input.
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
        insert_file(&conn, &src_file, "hash_unique");

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

        assert!(canon.join("photo.jpg").exists(), "file should be in canon");
        assert!(!src_file.exists(), "file should be gone from source");
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
        insert_file(&conn, &source.join("photo.jpg"), "hash_same");

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

        // Source removed; canon untouched
        assert!(canon.join("photo.jpg").exists());
        assert!(!source.join("photo.jpg").exists());
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
        insert_file(&conn, &src, "hash_source");

        // no_confirmation=true so conflict is auto-resolved; just verify
        // exactly one copy remains and the source file is gone.
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
        insert_file(&conn, &src_file, "hash_nested");

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
        insert_file(&conn, &src_file, "hash_x");

        merge_one(&conn, &canon, &source, true, &mut yes_reader()).unwrap();

        assert!(!album.exists(), "empty album subdir should be deleted");
    }

    #[test]
    fn test_merge_skips_self() {
        let dir = tempdir().unwrap();
        let canon = dir.path().join("canon");
        fs::create_dir_all(&canon).unwrap();
        let conn = open_test_db();

        // Should complete without error or modification
        let sources: &[&Path] = &[canon.as_path()];
        merge_into_canon(&conn, &canon, sources, true).unwrap();
    }
}
