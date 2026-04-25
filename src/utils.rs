use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

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

/// Return the modification time of `path` as a Unix timestamp (seconds).
pub fn mtime(path: &Path) -> Result<i64> {
    let modified = fs::metadata(path)
        .with_context(|| format!("reading metadata for {}", path.display()))?
        .modified()
        .with_context(|| format!("reading mtime for {}", path.display()))?;
    let secs = modified
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("mtime of {} predates the Unix epoch", path.display()))?
        .as_secs() as i64;
    Ok(secs)
}

/// Format a Unix timestamp as a human-readable date/time string (`YYYY-MM-DD HH:MM`).
pub fn fmt_mtime(secs: i64) -> String {
    let secs_u = secs.max(0) as u64;
    let rem = secs_u % 86400;
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let (y, month, day) = secs_to_ymd(secs);
    format!("{:04}-{:02}-{:02} {:02}:{:02}", y, month, day, hh, mm)
}

/// Convert a Unix timestamp (seconds) to `(year, month, day)`.
pub fn secs_to_ymd(secs: i64) -> (i32, u32, u32) {
    let secs = secs.max(0) as u64;
    let days = secs / 86400;
    let mut y = 1970i32;
    let mut d = days;
    loop {
        let diy = if is_leap(y) { 366 } else { 365 };
        if d < diy {
            break;
        }
        d -= diy;
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

/// Returns true if `y` is a leap year.
pub fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
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

    #[test]
    fn test_path_to_str_valid() {
        let path = std::path::Path::new("/some/valid/path.txt");
        assert_eq!(path_to_str(path).unwrap(), "/some/valid/path.txt");
    }

    #[test]
    fn test_secs_to_ymd_epoch() {
        assert_eq!(secs_to_ymd(0), (1970, 1, 1));
    }

    #[test]
    fn test_secs_to_ymd_known_date() {
        // 2009-01-05 UTC midnight = 1231113600
        assert_eq!(secs_to_ymd(1231113600), (2009, 1, 5));
    }

    #[test]
    fn test_secs_to_ymd_leap_day() {
        // 2000-02-29: days from epoch = 11016
        assert_eq!(secs_to_ymd(11016 * 86400), (2000, 2, 29));
    }

    #[test]
    fn test_secs_to_ymd_negative_clamped() {
        assert_eq!(secs_to_ymd(-1000), (1970, 1, 1));
    }

    #[test]
    fn test_is_leap() {
        assert!(is_leap(2000));
        assert!(is_leap(2004));
        assert!(!is_leap(1900));
        assert!(!is_leap(2001));
    }

    #[test]
    fn test_fmt_mtime_epoch() {
        assert_eq!(fmt_mtime(0), "1970-01-01 00:00");
    }

    #[test]
    fn test_fmt_mtime_known_date() {
        // 2009-01-05 16:17:00 UTC = 1231172220
        assert_eq!(fmt_mtime(1231172220), "2009-01-05 16:17");
    }

    #[test]
    fn test_fmt_mtime_negative_clamped() {
        assert_eq!(fmt_mtime(-999), "1970-01-01 00:00");
    }

    #[test]
    fn test_mtime_returns_nonzero_for_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.txt");
        std::fs::write(&path, b"x").unwrap();
        assert!(mtime(&path).unwrap() > 0);
    }

    #[test]
    fn test_mtime_returns_err_for_missing_file() {
        assert!(mtime(std::path::Path::new("/no/such/file")).is_err());
    }
}
