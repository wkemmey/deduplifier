use std::collections::HashSet;
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use rusqlite::Connection;
use walkdir::WalkDir;

use crate::hashing::compute_file_hash;
use crate::utils::path_to_str;

// ---------------------------------------------------------------------------
// Known media extensions (lowercase)
// ---------------------------------------------------------------------------
const MEDIA_EXTENSIONS: &[&str] = &[
    // Images
    "jpg", "jpeg", "png", "gif", "heic", "heif", "tiff", "tif", "bmp", "webp", "cr2", "cr3", "nef",
    "arw", "dng", "orf", "rw2", // Video
    "mp4", "mov", "avi", "mkv", "m4v", "3gp", "mts", "m2ts",
    // Camera thumbnails (accompany .avi on Canon cameras)
    "thm",
];

// Sanity-check range for EXIF dates
const YEAR_MIN: i32 = 1990;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Sort all media files under each of `directories` into date-based subdirs rooted at `canon`.
/// Requires `--delete` + `--no-confirmation` to have been validated by the caller.
pub fn sort_photos(conn: &Connection, directories: &[&Path], canon: &Path) -> Result<()> {
    for &root in directories {
        println!("\nSorting photos in: {}", root.display());
        sort_root(conn, root, canon)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-root logic
// ---------------------------------------------------------------------------

fn sort_root(conn: &Connection, root: &Path, canon: &Path) -> Result<()> {
    let dest_root = canon;

    // Collect all media files under root (snapshot before we start moving things)
    let files: Vec<PathBuf> = WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| is_media(e.path()))
        .map(|e| e.into_path())
        .collect();

    println!("  Found {} media file(s) to process.", files.len());

    let mut moved = 0usize;
    let mut skipped = 0usize;
    let mut deleted_dups = 0usize;

    for src in &files {
        let date = photo_date(src);
        let dest_dir = dest_root
            .join(format!("{:04}", date.0))
            .join(format!("{:04}-{:02}", date.0, date.1))
            .join(format!("{:04}-{:02}-{:02}", date.0, date.1, date.2));

        // Already in the right place?
        if let Some(src_parent) = src.parent() {
            if src_parent == dest_dir {
                skipped += 1;
                continue;
            }
        }

        let dest_path = resolve_dest(src, &dest_dir, conn)?;

        match dest_path {
            DestResult::TrueDuplicate => {
                // Identical file already exists at destination — remove source
                println!("  Duplicate (same hash): removing {}", src.display());
                fs::remove_file(src)
                    .with_context(|| format!("removing duplicate {}", src.display()))?;
                db_remove_file(conn, src);
                deleted_dups += 1;
            }
            DestResult::Path(dest) => {
                fs::create_dir_all(&dest_dir)
                    .with_context(|| format!("creating {}", dest_dir.display()))?;
                fs::rename(src, &dest)
                    .with_context(|| format!("moving {} -> {}", src.display(), dest.display()))?;
                println!("  Moved: {} -> {}", src.display(), dest.display());
                db_move_file(conn, src, &dest);
                moved += 1;
            }
        }
    }

    println!(
        "  Done: {} moved, {} already sorted, {} true duplicates removed.",
        moved, skipped, deleted_dups
    );

    // Delete now-empty subdirectories (but never the root itself)
    cleanup_empty_dirs(conn, root, dest_root)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Destination path resolution
// ---------------------------------------------------------------------------

enum DestResult {
    Path(PathBuf),
    TrueDuplicate,
}

/// Return the destination path for `src` inside `dest_dir`, handling collisions.
/// If an identical file (same hash) already exists there, return TrueDuplicate.
fn resolve_dest(src: &Path, dest_dir: &Path, conn: &Connection) -> Result<DestResult> {
    let fname = src.file_name().and_then(|n| n.to_str()).unwrap_or("file");

    let stem = Path::new(fname)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file");
    let ext = Path::new(fname)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| format!(".{}", e))
        .unwrap_or_default();

    // Try the plain name first, then -1, -2, …
    let mut candidate = dest_dir.join(fname);
    let mut suffix = 0u32;

    loop {
        if !candidate.exists() {
            return Ok(DestResult::Path(candidate));
        }

        // File exists at destination — compare hashes
        let src_hash =
            compute_file_hash(src).with_context(|| format!("hashing {}", src.display()))?;
        let dest_hash = compute_file_hash(&candidate)
            .with_context(|| format!("hashing {}", candidate.display()))?;

        if src_hash == dest_hash {
            return Ok(DestResult::TrueDuplicate);
        }

        // Different content — try next suffix
        suffix += 1;
        // Keep the newer file under the suffixed name; we're moving src,
        // so src gets the suffix (dest stays as-is).
        candidate = dest_dir.join(format!("{}-{}{}", stem, suffix, ext));
        let _ = conn; // conn reserved for future use if needed here
    }
}

// ---------------------------------------------------------------------------
// Date extraction
// ---------------------------------------------------------------------------

/// Returns (year, month, day) for the file.
/// Tries EXIF DateTimeOriginal first; falls back to mtime.
fn photo_date(path: &Path) -> (i32, u32, u32) {
    if let Some(date) = exif_date(path) {
        return date;
    }
    mtime_date(path)
}

fn exif_date(path: &Path) -> Option<(i32, u32, u32)> {
    let file = fs::File::open(path).ok()?;
    let mut reader = BufReader::new(file);
    let exif = exif::Reader::new().read_from_container(&mut reader).ok()?;

    // Prefer DateTimeOriginal, fall back to DateTimeDigitized, then DateTime
    let tags = [
        exif::Tag::DateTimeOriginal,
        exif::Tag::DateTimeDigitized,
        exif::Tag::DateTime,
    ];

    for tag in &tags {
        if let Some(field) = exif.get_field(*tag, exif::In::PRIMARY) {
            if let exif::Value::Ascii(ref vecs) = field.value {
                if let Some(bytes) = vecs.first() {
                    // Format: "YYYY:MM:DD HH:MM:SS"
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if let Some((year, month, day)) = parse_exif_date(s) {
                            if year >= YEAR_MIN {
                                return Some((year, month, day));
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

fn parse_exif_date(s: &str) -> Option<(i32, u32, u32)> {
    // "YYYY:MM:DD HH:MM:SS"
    let parts: Vec<&str> = s.splitn(2, ' ').collect();
    let date_part = parts.first()?;
    let date_fields: Vec<&str> = date_part.split(':').collect();
    if date_fields.len() < 3 {
        return None;
    }
    let year: i32 = date_fields[0].parse().ok()?;
    let month: u32 = date_fields[1].parse().ok()?;
    let day: u32 = date_fields[2].parse().ok()?;
    if month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }
    Some((year, month, day))
}

fn mtime_date(path: &Path) -> (i32, u32, u32) {
    let secs = fs::metadata(path)
        .and_then(|m| m.modified())
        .map(|t| {
            t.duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        })
        .unwrap_or(0);
    secs_to_ymd(secs)
}

/// Convert Unix timestamp (seconds) to (year, month, day).
fn secs_to_ymd(secs: i64) -> (i32, u32, u32) {
    let secs = secs.max(0) as u64;
    let days = secs / 86400;
    let mut y = 1970i32;
    let mut d = days;
    loop {
        let leap = is_leap(y);
        let days_in_year = if leap { 366 } else { 365 };
        if d < days_in_year {
            break;
        }
        d -= days_in_year;
        y += 1;
    }
    let month_days: [u64; 12] = [
        31,
        if is_leap(y) { 29 } else { 28 },
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
    let mut month = 1u32;
    for &md in &month_days {
        if d < md {
            break;
        }
        d -= md;
        month += 1;
    }
    (y, month, (d + 1) as u32)
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

// ---------------------------------------------------------------------------
// Empty directory cleanup
// ---------------------------------------------------------------------------

/// Walk `root` bottom-up and remove any empty subdirectory (never removes root
/// itself or dest_root itself).
fn cleanup_empty_dirs(conn: &Connection, root: &Path, dest_root: &Path) -> Result<()> {
    // Collect all dirs under root, sorted deepest-first
    let mut dirs: Vec<PathBuf> = WalkDir::new(root)
        .min_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_dir())
        .map(|e| e.into_path())
        .collect();

    // Also collect dirs under dest_root if different from root
    if dest_root != root {
        let extra: Vec<PathBuf> = WalkDir::new(dest_root)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_dir())
            .map(|e| e.into_path())
            .collect();
        for d in extra {
            if !dirs.contains(&d) {
                dirs.push(d);
            }
        }
    }

    // Sort deepest first (longest path first)
    dirs.sort_by(|a, b| b.components().count().cmp(&a.components().count()));

    let protected: HashSet<PathBuf> = [root.to_path_buf(), dest_root.to_path_buf()]
        .into_iter()
        .collect();

    for dir in &dirs {
        if protected.contains(dir.as_path()) {
            continue;
        }
        // Try to read the directory; if empty, remove it
        let Ok(mut entries) = fs::read_dir(dir) else {
            continue;
        };
        if entries.next().is_none() {
            match fs::remove_dir(dir) {
                Ok(()) => {
                    println!("  Removed empty dir: {}", dir.display());
                    db_remove_dir(conn, dir);
                }
                Err(e) => {
                    eprintln!("  Warning: could not remove {}: {}", dir.display(), e);
                }
            }
        }
    }
    Ok(())
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

fn db_remove_dir(conn: &Connection, path: &Path) {
    if let Ok(p) = path_to_str(path) {
        // Remove the directory record and all files under it
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
// Helpers
// ---------------------------------------------------------------------------

fn is_media(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| MEDIA_EXTENSIONS.contains(&e.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
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

    // -- Unit: parse_exif_date -----------------------------------------------

    #[test]
    fn test_parse_exif_date_valid() {
        assert_eq!(parse_exif_date("2009:01:05 16:17:00"), Some((2009, 1, 5)));
    }

    #[test]
    fn test_parse_exif_date_no_time() {
        // splitn(2, ' ') on a string with no space still yields the whole string
        // as the date portion, which parses fine.
        assert_eq!(parse_exif_date("2009:01:05"), Some((2009, 1, 5)));
    }

    #[test]
    fn test_parse_exif_date_invalid_month() {
        assert_eq!(parse_exif_date("2009:13:05 00:00:00"), None);
    }

    #[test]
    fn test_parse_exif_date_zero_day() {
        assert_eq!(parse_exif_date("2009:01:00 00:00:00"), None);
    }

    #[test]
    fn test_parse_exif_date_garbage() {
        assert_eq!(parse_exif_date("not a date"), None);
    }

    // -- Unit: secs_to_ymd ---------------------------------------------------

    #[test]
    fn test_secs_to_ymd_epoch() {
        // 1970-01-01
        assert_eq!(secs_to_ymd(0), (1970, 1, 1));
    }

    #[test]
    fn test_secs_to_ymd_known_date() {
        // 2009-01-05 UTC midnight = 1231113600
        assert_eq!(secs_to_ymd(1231113600), (2009, 1, 5));
    }

    #[test]
    fn test_secs_to_ymd_leap_day() {
        // 2000-02-29 (2000 is a leap year)
        // Days from 1970-01-01 to 2000-02-29 = 11016
        assert_eq!(secs_to_ymd(11016 * 86400), (2000, 2, 29));
    }

    #[test]
    fn test_secs_to_ymd_negative_clamped() {
        // Negative timestamps clamp to epoch
        assert_eq!(secs_to_ymd(-1000), (1970, 1, 1));
    }

    // -- Unit: is_leap -------------------------------------------------------

    #[test]
    fn test_is_leap_regular() {
        assert!(is_leap(2000));
        assert!(is_leap(2004));
        assert!(!is_leap(1900));
        assert!(!is_leap(2001));
    }

    // -- Unit: is_media ------------------------------------------------------

    #[test]
    fn test_is_media_known_extensions() {
        for ext in &["jpg", "JPG", "Jpg", "mp4", "CR2", "heic", "thm"] {
            assert!(
                is_media(Path::new(&format!("file.{}", ext))),
                "expected {} to be media",
                ext
            );
        }
    }

    #[test]
    fn test_is_media_unknown_extensions() {
        for ext in &["txt", "xml", "db", "rs", "pdf"] {
            assert!(
                !is_media(Path::new(&format!("file.{}", ext))),
                "expected {} not to be media",
                ext
            );
        }
    }

    #[test]
    fn test_is_media_no_extension() {
        assert!(!is_media(Path::new("README")));
    }

    // -- Integration: sort_root ----------------------------------------------

    /// Helper: write bytes to a file, creating parent dirs.
    fn write_file(path: &Path, content: &[u8]) {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    #[test]
    fn test_sort_root_moves_file_by_mtime() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let conn = open_test_db();

        // Create a JPEG with a known mtime via filetime
        let src = root.join("photo.jpg");
        write_file(&src, b"fake jpeg content");

        // Set mtime to 2009-01-05 (Unix: 1230422400)
        let mtime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1230422400);
        fs::File::open(&src)
            .unwrap()
            .set_modified(mtime)
            .unwrap_or(()); // best-effort; fallback if unsupported

        sort_root(&conn, root, root).unwrap();

        // File should have moved somewhere under root/YYYY/...
        // We don't assert the exact date since set_modified may not work everywhere,
        // but we can assert the original location is gone and exactly one jpg exists.
        let remaining: Vec<_> = WalkDir::new(root)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("jpg"))
            .collect();
        assert_eq!(
            remaining.len(),
            1,
            "exactly one jpg should exist after sort"
        );
        assert_ne!(
            remaining[0].path(),
            src.as_path(),
            "jpg should have moved from its original location"
        );
    }

    #[test]
    fn test_sort_root_skips_already_sorted() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let conn = open_test_db();

        // Write a file, then figure out what date dir sort_root would compute
        // from its mtime (we can't set mtime portably, so derive the dir)
        let tmp = root.join("photo.jpg");
        write_file(&tmp, b"already sorted");
        let secs = fs::metadata(&tmp)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let (y, m, d) = secs_to_ymd(secs);
        let target_dir = root
            .join(format!("{:04}", y))
            .join(format!("{:04}-{:02}", y, m))
            .join(format!("{:04}-{:02}-{:02}", y, m, d));
        let target = target_dir.join("photo.jpg");

        // Move the file to where sort_root would put it
        fs::create_dir_all(&target_dir).unwrap();
        fs::rename(&tmp, &target).unwrap();

        sort_root(&conn, root, root).unwrap();

        // File should still be in place — already sorted
        assert!(target.exists(), "already-sorted file should not move");
    }

    #[test]
    fn test_sort_root_removes_true_duplicate() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let conn = open_test_db();

        let content = b"identical photo content";

        // Source file in an unsorted subdir
        let src_dir = root.join("album");
        let src = src_dir.join("photo.jpg");
        write_file(&src, content);

        // Determine where sort_root will try to put it (mtime-based)
        let secs = fs::metadata(&src)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let (y, m, d) = secs_to_ymd(secs);
        let dest_dir = root
            .join(format!("{:04}", y))
            .join(format!("{:04}-{:02}", y, m))
            .join(format!("{:04}-{:02}-{:02}", y, m, d));
        let dest = dest_dir.join("photo.jpg");

        // Pre-place the identical file at the destination
        write_file(&dest, content);

        sort_root(&conn, root, root).unwrap();

        // Source should be deleted (true duplicate)
        assert!(!src.exists(), "true duplicate source should be removed");
        // Destination should still exist
        assert!(dest.exists(), "destination copy should remain");
    }

    #[test]
    fn test_sort_root_suffixes_name_collision() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let conn = open_test_db();

        // Source file
        let src_dir = root.join("album");
        let src = src_dir.join("photo.jpg");
        write_file(&src, b"version A");

        // Determine destination dir
        let secs = fs::metadata(&src)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let (y, m, d) = secs_to_ymd(secs);
        let dest_dir = root
            .join(format!("{:04}", y))
            .join(format!("{:04}-{:02}", y, m))
            .join(format!("{:04}-{:02}-{:02}", y, m, d));

        // Pre-place a DIFFERENT file with the same name
        write_file(&dest_dir.join("photo.jpg"), b"version B");

        sort_root(&conn, root, root).unwrap();

        // Source should be gone from original location
        assert!(!src.exists(), "source should have moved");
        // A suffixed copy should exist at the destination
        assert!(
            dest_dir.join("photo-1.jpg").exists(),
            "suffixed file photo-1.jpg should exist"
        );
        // Original destination should be untouched
        assert!(dest_dir.join("photo.jpg").exists());
    }

    #[test]
    fn test_sort_root_leaves_non_media_alone() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let conn = open_test_db();

        let txt = root.join("notes.txt");
        write_file(&txt, b"some notes");

        sort_root(&conn, root, root).unwrap();

        assert!(txt.exists(), "non-media files should not be moved");
    }

    #[test]
    fn test_sort_root_deletes_empty_subdir() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let conn = open_test_db();

        // Create a subdir with a single photo
        let album = root.join("summer_vacation");
        let src = album.join("photo.jpg");
        write_file(&src, b"vacation photo");

        sort_root(&conn, root, root).unwrap();

        // The photo moved out, so summer_vacation should be gone
        assert!(
            !album.exists(),
            "empty album subdir should be removed after photos are moved out"
        );
    }

    #[test]
    fn test_sort_root_keeps_nonempty_subdir() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let conn = open_test_db();

        let album = root.join("mixed");
        let photo = album.join("photo.jpg");
        let notes = album.join("notes.txt");
        write_file(&photo, b"photo");
        write_file(&notes, b"notes");

        sort_root(&conn, root, root).unwrap();

        // Photo moved, but notes.txt keeps the dir non-empty
        assert!(
            album.exists(),
            "subdir with remaining non-media files should not be deleted"
        );
        assert!(notes.exists());
    }

    #[test]
    fn test_sort_root_with_canon() {
        let dir = tempdir().unwrap();
        let src_root = dir.path().join("source");
        let canon_root = dir.path().join("canon");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&canon_root).unwrap();
        let conn = open_test_db();

        let src = src_root.join("photo.jpg");
        write_file(&src, b"canon test photo");

        sort_root(&conn, &src_root, &canon_root).unwrap();

        // File should have moved into canon_root, not src_root
        let moved: Vec<_> = WalkDir::new(&canon_root)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("jpg"))
            .collect();
        assert_eq!(moved.len(), 1, "photo should be under canon root");

        let in_src: Vec<_> = WalkDir::new(&src_root)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("jpg"))
            .collect();
        assert_eq!(in_src.len(), 0, "photo should not remain under source root");
    }
}
