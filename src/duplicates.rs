use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

use crate::db;

/// A single directory instance that is a member of a duplicate group.
pub struct DirEntry {
    pub path: String,
    pub size: i64,
}

/// A set of directories that all share the same hash, along with aggregate metadata.
pub struct DuplicateGroup {
    pub hash: String,
    pub max_size: i64,
    pub members: Vec<DirEntry>,
}

/// A group of files that share the same hash (i.e. exact duplicates).
pub struct DuplicateFileGroup {
    pub hash: String,
    pub count: i64,
    pub total_size: i64,
    pub files: Vec<db::FileRecord>,
}

pub fn find_duplicate_files(conn: &Connection) -> Result<Vec<DuplicateFileGroup>> {
    let groups = db::duplicate_file_groups(conn)?;
    let mut result = Vec::new();
    for group in groups {
        let files = db::files_with_hash(conn, &group.hash)?;
        result.push(DuplicateFileGroup {
            hash: group.hash,
            count: group.count,
            total_size: group.size,
            files,
        });
    }
    Ok(result)
}

/// From a list of duplicate directory groups, fetch paths for each group,
/// filter to only members under `scanned_dirs` (if any), drop groups with
/// fewer than 2 remaining members, and then partition into top-level groups
/// (those not entirely contained within another duplicate group) vs. covered
/// sub-groups (which will be skipped to avoid double-deletion).
/// Returns `(top_level, covered_count)`.
pub fn build_top_level_groups(
    conn: &Connection,
    duplicate_group_hashes: &[db::DuplicateGroupHash],
    scanned_dirs: &[&Path],
) -> Result<(Vec<DuplicateGroup>, usize)> {
    let mut groups: Vec<DuplicateGroup> = Vec::new();

    for group in duplicate_group_hashes {
        // find dirs with matching hash
        let all_dirs_with_hash: Vec<DirEntry> = db::directories_with_hash(conn, &group.hash)?
            .into_iter()
            .map(|r| DirEntry {
                path: r.path,
                size: r.size,
            })
            .collect();

        // filter to only those under scanned_dirs
        let members: Vec<DirEntry> = all_dirs_with_hash
            .into_iter()
            .filter(|e| {
                let candidate = Path::new(&e.path);
                scanned_dirs.iter().any(|root| candidate.starts_with(root))
            })
            .collect();

        // drop groups with fewer than 2 members after filtering
        // (not duplicate if only 1 in scanned scope)
        if members.len() >= 2 {
            groups.push(DuplicateGroup {
                hash: group.hash.clone(),
                max_size: group.size,
                members,
            });
        }
    }

    // Build a flat set of all paths that appear in any duplicate group
    let all_dup_paths: HashSet<&str> = groups
        .iter()
        .flat_map(|g| g.members.iter().map(|e| e.path.as_str()))
        .collect();

    // A group is "covered" if every member is a strict subdirectory of some
    // other path in all_dup_paths — meaning a parent duplicate already subsumes it.
    let is_covered = |members: &[DirEntry]| -> bool {
        members.iter().all(|e| {
            let candidate = Path::new(&e.path);
            all_dup_paths.iter().any(|other| {
                let other_path = Path::new(other);
                other_path != candidate && candidate.starts_with(other_path)
            })
        })
    };

    // Collect covered hashes before dropping the closure that borrows all_dup_paths
    let covered_hashes: HashSet<String> = groups
        .iter()
        .filter(|g| is_covered(&g.members))
        .map(|g| g.hash.clone())
        .collect();
    let covered_count = covered_hashes.len();

    let top_level: Vec<DuplicateGroup> = groups
        .into_iter()
        .filter(|g| !covered_hashes.contains(&g.hash))
        .collect();

    Ok((top_level, covered_count))
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

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        db::setup_schema(&conn).unwrap();
        conn
    }

    fn insert_file(conn: &Connection, path: &str, hash: &str, size: i64) {
        db::upsert_file(conn, Path::new(path), hash, size, 0).unwrap();
    }

    fn insert_dir(conn: &Connection, path: &str, hash: &str, size: i64) {
        db::upsert_directory(conn, Path::new(path), hash, size).unwrap();
    }

    // -----------------------------------------------------------------------
    // find_duplicate_files — tested via db::duplicate_file_groups so we verify
    // the query logic, not just that the function doesn't panic
    // -----------------------------------------------------------------------

    #[test]
    fn test_find_duplicate_files_none_when_db_empty() {
        let conn = open_test_db();
        let groups = db::duplicate_file_groups(&conn).unwrap();
        assert!(groups.is_empty());
        find_duplicate_files(&conn).unwrap();
    }

    #[test]
    fn test_find_duplicate_files_detects_correct_groups() {
        let conn = open_test_db();
        insert_file(&conn, "/a/file.txt", "hash_dup", 100);
        insert_file(&conn, "/b/file.txt", "hash_dup", 100);
        insert_file(&conn, "/c/unique.txt", "hash_unique", 50);

        let groups = db::duplicate_file_groups(&conn).unwrap();
        assert_eq!(groups.len(), 1, "only one duplicate group");
        assert_eq!(groups[0].hash, "hash_dup");
        assert_eq!(groups[0].count, 2); // count
        assert_eq!(groups[0].size, 200); // total_size

        find_duplicate_files(&conn).unwrap();
    }

    #[test]
    fn test_find_duplicate_files_unique_files_not_reported() {
        let conn = open_test_db();
        insert_file(&conn, "/a/file.txt", "hash_a", 100);
        insert_file(&conn, "/b/file.txt", "hash_b", 100);

        let groups = db::duplicate_file_groups(&conn).unwrap();
        assert!(
            groups.is_empty(),
            "different hashes should not appear as duplicates"
        );
    }

    // -----------------------------------------------------------------------
    // build_top_level_groups — tests the filtering and covered-group logic
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_top_level_groups_basic() {
        let conn = open_test_db();
        insert_dir(&conn, "/a/photos", "hash1", 1024);
        insert_dir(&conn, "/b/photos", "hash1", 1024);

        let groups = db::duplicate_directory_groups(&conn).unwrap();
        let (top_level, covered_count) =
            build_top_level_groups(&conn, &groups, &[Path::new("/a"), Path::new("/b")]).unwrap();

        assert_eq!(top_level.len(), 1);
        assert_eq!(covered_count, 0);
        assert_eq!(top_level[0].members.len(), 2);
    }

    #[test]
    fn test_build_top_level_groups_covered_subdirs_excluded() {
        // /a and /b are duplicate parents; /a/sub and /b/sub are duplicate children.
        // The child group should be "covered" and excluded from top_level.
        let conn = open_test_db();
        insert_dir(&conn, "/a", "parent_hash", 2048);
        insert_dir(&conn, "/b", "parent_hash", 2048);
        insert_dir(&conn, "/a/sub", "child_hash", 512);
        insert_dir(&conn, "/b/sub", "child_hash", 512);

        let groups = db::duplicate_directory_groups(&conn).unwrap();
        let (top_level, covered_count) =
            build_top_level_groups(&conn, &groups, &[Path::new("/a"), Path::new("/b")]).unwrap();

        assert_eq!(top_level.len(), 1, "only parent group should be top-level");
        assert_eq!(top_level[0].hash, "parent_hash");
        assert_eq!(covered_count, 1, "child group should be covered");
    }

    #[test]
    fn test_build_top_level_groups_scanned_dirs_filter() {
        // Two duplicate dirs, but only one is under the scanned root.
        // After filtering, the group has only 1 member and should be dropped.
        let conn = open_test_db();
        insert_dir(&conn, "/scanned/photos", "hash1", 1024);
        insert_dir(&conn, "/other/photos", "hash1", 1024);

        let groups = db::duplicate_directory_groups(&conn).unwrap();
        let scanned = Path::new("/scanned");
        let (top_level, _) = build_top_level_groups(&conn, &groups, &[scanned]).unwrap();

        assert!(
            top_level.is_empty(),
            "group with only 1 member in scanned scope should be dropped"
        );
    }

    #[test]
    fn test_build_top_level_groups_scanned_dirs_keeps_both_in_scope() {
        let conn = open_test_db();
        insert_dir(&conn, "/scanned/a/photos", "hash1", 1024);
        insert_dir(&conn, "/scanned/b/photos", "hash1", 1024);

        let groups = db::duplicate_directory_groups(&conn).unwrap();
        let scanned = Path::new("/scanned");
        let (top_level, _) = build_top_level_groups(&conn, &groups, &[scanned]).unwrap();

        assert_eq!(
            top_level.len(),
            1,
            "both members in scope — group should appear"
        );
        assert_eq!(top_level[0].members.len(), 2);
    }

    // -----------------------------------------------------------------------
    // find_duplicate_files — returns data, does not mutate DB
    // -----------------------------------------------------------------------

    #[test]
    fn test_find_duplicate_files_returns_empty_when_no_duplicates() {
        let conn = open_test_db();
        let groups = find_duplicate_files(&conn).unwrap();
        assert!(groups.is_empty());
    }

    #[test]
    fn test_find_duplicate_files_returns_group_with_files() {
        let conn = open_test_db();
        insert_file(&conn, "/a/file.txt", "hash_dup", 100);
        insert_file(&conn, "/b/file.txt", "hash_dup", 100);
        let groups = find_duplicate_files(&conn).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].count, 2);
        assert_eq!(groups[0].files.len(), 2);
    }
}
