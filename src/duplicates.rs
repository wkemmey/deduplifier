use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

use crate::{db, file_system};

/// A single directory instance that is a member of a duplicate group.
struct DirEntry {
    path: String,
    size: i64,
}

/// A set of directories that all share the same hash, along with aggregate metadata.
struct DuplicateGroup {
    hash: String,
    max_size: i64,
    members: Vec<DirEntry>,
}

pub fn find_duplicate_files(conn: &Connection) -> Result<()> {
    let groups = db::duplicate_file_groups(conn)?;

    if groups.is_empty() {
        println!("No duplicate files found.");
        return Ok(());
    }

    for group in groups {
        let hash_display = if group.hash.len() >= 16 {
            &group.hash[..16]
        } else {
            &group.hash
        };
        println!(
            "\nDuplicate files (hash: {}, count: {}, total size: {} bytes):",
            hash_display, group.count, group.size
        );

        for record in db::files_with_hash(conn, &group.hash)? {
            println!("  - {} ({} bytes)", record.path, record.size);
        }
    }

    Ok(())
}

/// From a list of duplicate directory groups, fetch paths for each group,
/// filter to only members under `scanned_dirs` (if any), drop groups with
/// fewer than 2 remaining members, and then partition into top-level groups
/// (those not entirely contained within another duplicate group) vs. covered
/// sub-groups (which will be skipped to avoid double-deletion).
/// Returns `(top_level, covered_count)`.
fn build_top_level_groups(
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

pub fn find_duplicate_directories(
    conn: &Connection,
    delete: bool,
    canon: Option<&Path>,
    no_confirmation: bool,
    scanned_dirs: &[&Path],
) -> Result<()> {
    // Collect all duplicate groups upfront so we can iterate interactively
    let duplicate_group_hashes = db::duplicate_directory_groups(conn)?;

    if duplicate_group_hashes.is_empty() {
        println!("No duplicate directories found.");
        return Ok(());
    }

    let (top_level_groups, covered_count) =
        build_top_level_groups(conn, &duplicate_group_hashes, scanned_dirs)?;

    println!(
        "Found {} set(s) of duplicate directories ({} are subdirectories of other duplicates and will be skipped).",
        top_level_groups.len(),
        covered_count,
    );

    let stdin = io::stdin();

    for group in &top_level_groups {
        let hash_display = if group.hash.len() >= 16 {
            &group.hash[..16]
        } else {
            &group.hash
        };
        println!(
            "\nDuplicate directories (hash: {}…, count: {}, size: {} bytes each):",
            hash_display,
            group.members.len(),
            group.max_size
        );

        for (i, entry) in group.members.iter().enumerate() {
            println!("  [{}] {} ({} bytes)", i + 1, entry.path, entry.size);
        }

        if !delete {
            continue;
        }

        let dirs = &group.members;

        // Determine if --canon auto-selects a keeper
        let auto_keep: Option<usize> = if let Some(canon_path) = canon {
            dirs.iter()
                .position(|e| Path::new(&e.path).starts_with(canon_path))
        } else {
            None
        };

        // If --no-confirmation is set and multiple members are under canon, we can't
        // safely auto-select — deleting within canon silently would defeat its purpose.
        // When confirming individually, the user can handle it themselves.
        if no_confirmation {
            if let Some(canon_path) = canon {
                let canon_count = dirs
                    .iter()
                    .filter(|e| Path::new(&e.path).starts_with(canon_path))
                    .count();
                if canon_count > 1 {
                    println!(
                        "  Warning: {} members are under --canon ({}); skipping this group.",
                        canon_count,
                        canon_path.display()
                    );
                    println!();
                    continue;
                }
            }
        }

        let keep_idx: usize = if let Some(idx) = auto_keep {
            println!(
                "  Auto-selecting [{}] as canonical: {}",
                idx + 1,
                dirs[idx].path
            );
            idx
        } else {
            print!("  Keep which? (1-{}, or 's' to skip): ", dirs.len());
            io::stdout().flush()?;
            let mut line = String::new();
            stdin.lock().read_line(&mut line)?;
            let trimmed = line.trim();
            if trimmed.eq_ignore_ascii_case("s") {
                println!("  Skipped.");
                continue;
            }
            match trimmed.parse::<usize>() {
                Ok(n) if n >= 1 && n <= dirs.len() => n - 1,
                _ => {
                    println!("  Invalid choice, skipping.");
                    continue;
                }
            }
        };

        // List what will be deleted and ask for type-to-confirm
        let to_delete: Vec<&str> = dirs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != keep_idx)
            .map(|(_, e)| e.path.as_str())
            .collect();

        println!("  Keeping:  {}", dirs[keep_idx].path);
        println!("  Will permanently delete:");
        for path in &to_delete {
            println!("    - {}", path);
        }

        for path in &to_delete {
            // When --no-confirmation is set and canon drove the choice, skip the prompt
            let confirmed = if no_confirmation && auto_keep.is_some() {
                println!("  Deleting '{}' (--no-confirmation)", path);
                true
            } else {
                print!("  Confirm deletion of '{}' [y/N] > ", path);
                io::stdout().flush()?;
                let mut confirmation = String::new();
                stdin.lock().read_line(&mut confirmation)?;
                if !confirmation.trim().eq_ignore_ascii_case("y") {
                    println!("  Skipped.");
                    false
                } else {
                    true
                }
            };

            if !confirmed {
                continue;
            }

            // Delete the directory from disk
            let dir_path = Path::new(path);
            if dir_path.exists() {
                file_system::delete_dir_all(dir_path)?;
                println!("  Deleted '{}'.", path);
            } else {
                println!("  '{}' no longer exists on disk, skipping.", path);
            }

            // Remove from database: the directory itself and all files/subdirs under it
            db::remove_tree(conn, dir_path)?;
            println!("  Removed '{}' and its contents from the database.", path);
        }

        println!(); // blank line between groups
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
    // find_duplicate_directories — integration (delete=false, no prompt)
    // -----------------------------------------------------------------------

    #[test]
    fn test_find_duplicate_dirs_no_duplicates() {
        let conn = open_test_db();
        find_duplicate_directories(&conn, false, None, false, &[Path::new("/")]).unwrap();
    }

    #[test]
    fn test_find_duplicate_dirs_with_duplicates_no_delete() {
        let conn = open_test_db();
        insert_dir(&conn, "/a/photos", "deadbeef", 1024);
        insert_dir(&conn, "/b/photos", "deadbeef", 1024);
        // delete=false — no prompt, just display
        find_duplicate_directories(
            &conn,
            false,
            None,
            false,
            &[Path::new("/a"), Path::new("/b")],
        )
        .unwrap();
        // Both dirs should still be in the DB
        let dirs = db::directories_with_hash(&conn, "deadbeef").unwrap();
        assert_eq!(dirs.len(), 2, "dirs should be untouched when delete=false");
    }
}
