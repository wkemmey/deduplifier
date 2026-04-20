use anyhow::Result;
use std::path::Path;

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
}
