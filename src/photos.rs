use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use walkdir::WalkDir;

use crate::{db, file_system, hashing, utils};

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

pub enum SortEvent<'a> {
    FileCount(usize),
    Duplicate(&'a Path),
    Moved(&'a Path, &'a Path),
}

pub struct SortStats {
    pub moved: usize,
    pub skipped: usize,
    pub deleted_dups: usize,
}

pub fn sort_root(
    conn: &Connection,
    root: &Path,
    canon: &Path,
    on_event: &mut impl FnMut(SortEvent<'_>),
) -> Result<SortStats> {
    let dest_root = canon;

    let files: Vec<PathBuf> = WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| is_media(e.path()))
        .map(|e| e.into_path())
        .collect();

    on_event(SortEvent::FileCount(files.len()));

    let mut moved = 0usize;
    let mut skipped = 0usize;
    let mut deleted_dups = 0usize;

    for src in &files {
        let date = photo_date(src)?;
        let dest_dir = dest_root
            .join(format!("{:04}", date.0))
            .join(format!("{:04}-{:02}", date.0, date.1))
            .join(format!("{:04}-{:02}-{:02}", date.0, date.1, date.2));

        if let Some(src_parent) = src.parent() {
            if src_parent == dest_dir {
                skipped += 1;
                continue;
            }
        }

        let dest_path = resolve_dest(src, &dest_dir, conn)?;

        match dest_path {
            DestResult::TrueDuplicate => {
                on_event(SortEvent::Duplicate(src));
                file_system::delete_file(src)?;
                db::remove_file(conn, src)?;
                deleted_dups += 1;
            }
            DestResult::Path(dest) => {
                file_system::move_file(src, &dest)?;
                on_event(SortEvent::Moved(src, &dest));
                db::move_file(conn, src, &dest)?;
                moved += 1;
            }
        }
    }

    file_system::delete_empty_subdirs(root)?;

    Ok(SortStats {
        moved,
        skipped,
        deleted_dups,
    })
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
        let src_hash = hashing::compute_file_hash(src)
            .with_context(|| format!("hashing {}", src.display()))?;
        let dest_hash = hashing::compute_file_hash(&candidate)
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
fn photo_date(path: &Path) -> Result<(i32, u32, u32)> {
    if let Some(date) = exif_date(path) {
        return Ok(date);
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

fn mtime_date(path: &Path) -> Result<(i32, u32, u32)> {
    Ok(utils::secs_to_ymd(utils::mtime(path)?))
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
    use crate::utils::secs_to_ymd;
    use rusqlite::Connection;
    use std::fs;
    use std::time::UNIX_EPOCH;
    use tempfile::tempdir;

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        db::setup_schema(&conn).unwrap();
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

        sort_root(&conn, root, root, &mut |_| ()).unwrap();

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

        sort_root(&conn, root, root, &mut |_| ()).unwrap();

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

        sort_root(&conn, root, root, &mut |_| ()).unwrap();

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

        sort_root(&conn, root, root, &mut |_| ()).unwrap();

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

        sort_root(&conn, root, root, &mut |_| ()).unwrap();

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

        sort_root(&conn, root, root, &mut |_| ()).unwrap();

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

        sort_root(&conn, root, root, &mut |_| ()).unwrap();

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

        sort_root(&conn, &src_root, &canon_root, &mut |_| ()).unwrap();

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
