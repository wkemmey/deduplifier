use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::HashSet;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;

pub fn find_duplicate_files(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT hash, COUNT(*) as count, SUM(size) as total_size 
         FROM files 
         GROUP BY hash 
         HAVING count > 1
         ORDER BY total_size DESC",
    )?;

    let duplicates = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;

    let mut found_any = false;
    for dup in duplicates {
        let (hash, count, total_size) = dup?;
        found_any = true;
        let hash_display = if hash.len() >= 16 { &hash[..16] } else { &hash };
        println!(
            "\nDuplicate files (hash: {}, count: {}, total size: {} bytes):",
            hash_display, count, total_size
        );

        let mut file_stmt = conn.prepare("SELECT path, size FROM files WHERE hash = ?1")?;
        let files = file_stmt.query_map(params![hash], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        for file in files {
            let (path, size) = file?;
            println!("  - {} ({} bytes)", path, size);
        }
    }

    if !found_any {
        println!("No duplicate files found.");
    }

    Ok(())
}

pub fn find_duplicate_directories(
    conn: &Connection,
    delete: bool,
    canon: Option<&Path>,
    no_confirmation: bool,
    scanned_dirs: &[&Path],
) -> Result<()> {
    // Collect all duplicate groups upfront so we can iterate interactively
    let mut stmt = conn.prepare(
        "SELECT hash, COUNT(*) as count, MAX(size) as max_size
         FROM directories
         WHERE hash != 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
         GROUP BY hash
         HAVING count > 1
         ORDER BY max_size DESC",
    )?;

    let duplicate_groups: Vec<(String, i64, i64)> = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;

    if duplicate_groups.is_empty() {
        println!("No duplicate directories found.");
        return Ok(());
    }

    // For each group, fetch its member paths upfront, then filter to only
    // members that fall under one of the scanned directories.
    // Groups with fewer than 2 members after filtering are skipped entirely —
    // the out-of-scope members stay in the DB untouched for a future run.
    let mut groups_with_paths: Vec<(String, i64, i64, Vec<(String, i64)>)> = Vec::new();
    for (hash, count, max_size) in &duplicate_groups {
        let mut dir_stmt =
            conn.prepare("SELECT path, size FROM directories WHERE hash = ?1 ORDER BY path")?;
        let all_dirs: Vec<(String, i64)> = dir_stmt
            .query_map(params![hash], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;

        // Keep only members that are under one of the scanned directories
        let dirs: Vec<(String, i64)> = if scanned_dirs.is_empty() {
            all_dirs
        } else {
            all_dirs
                .into_iter()
                .filter(|(p, _)| {
                    let candidate = Path::new(p);
                    scanned_dirs.iter().any(|root| candidate.starts_with(root))
                })
                .collect()
        };

        if dirs.len() >= 2 {
            groups_with_paths.push((hash.clone(), *count, *max_size, dirs));
        }
    }

    // Build a flat set of all paths that appear in any duplicate group
    let all_dup_paths: HashSet<&str> = groups_with_paths
        .iter()
        .flat_map(|(_, _, _, dirs)| dirs.iter().map(|(p, _)| p.as_str()))
        .collect();

    // A group is "covered" if every one of its members is a strict subdirectory
    // of some other path in all_dup_paths (i.e. not the path itself)
    let is_covered = |dirs: &[(String, i64)]| -> bool {
        dirs.iter().all(|(p, _)| {
            let candidate = Path::new(p);
            all_dup_paths.iter().any(|other| {
                let other_path = Path::new(other);
                other_path != candidate && candidate.starts_with(other_path)
            })
        })
    };

    let top_level_groups: Vec<&(String, i64, i64, Vec<(String, i64)>)> = groups_with_paths
        .iter()
        .filter(|(_, _, _, dirs)| !is_covered(dirs))
        .collect();

    println!(
        "Found {} set(s) of duplicate directories ({} are subdirectories of other duplicates and will be skipped).",
        top_level_groups.len(),
        groups_with_paths.len() - top_level_groups.len(),
    );

    let stdin = io::stdin();

    for (hash, count, max_size, dirs) in &top_level_groups {
        let hash_display = if hash.len() >= 16 { &hash[..16] } else { hash };
        println!(
            "\nDuplicate directories (hash: {}…, count: {}, size: {} bytes each):",
            hash_display, count, max_size
        );

        for (i, (path, size)) in dirs.iter().enumerate() {
            println!("  [{}] {} ({} bytes)", i + 1, path, size);
        }

        if !delete {
            continue;
        }

        // Determine if --canon auto-selects a keeper
        let auto_keep: Option<usize> = if let Some(canon_path) = canon {
            dirs.iter()
                .position(|(p, _)| Path::new(p).starts_with(canon_path))
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
                    .filter(|(p, _)| Path::new(p).starts_with(canon_path))
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
                dirs[idx].0
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
            .map(|(_, (p, _))| p.as_str())
            .collect();

        println!("  Keeping:  {}", dirs[keep_idx].0);
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
                fs::remove_dir_all(dir_path)?;
                println!("  Deleted '{}'.", path);
            } else {
                println!("  '{}' no longer exists on disk, skipping.", path);
            }

            // Remove from database: the directory itself and all files/subdirs under it
            let prefix = format!("{}%", path);
            conn.execute("DELETE FROM files WHERE path LIKE ?1", params![prefix])?;
            conn.execute(
                "DELETE FROM directories WHERE path LIKE ?1",
                params![prefix],
            )?;
            println!("  Removed '{}' and its contents from the database.", path);
        }

        println!(); // blank line between groups
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::setup_schema;
    use rusqlite::Connection;

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        setup_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn test_find_duplicate_files_none() {
        // No files in DB — should complete without error
        let conn = open_test_db();
        find_duplicate_files(&conn).unwrap();
    }

    #[test]
    fn test_find_duplicate_files_detects_duplicates() {
        let conn = open_test_db();
        // Insert two files with the same hash
        conn.execute_batch(
            "INSERT INTO files VALUES ('/a/file.txt', 'abc123', 100, 0);
             INSERT INTO files VALUES ('/b/file.txt', 'abc123', 100, 0);",
        )
        .unwrap();
        // Should complete without error (we can't easily capture stdout in unit tests,
        // but we verify it doesn't panic or error)
        find_duplicate_files(&conn).unwrap();
    }

    #[test]
    fn test_find_duplicate_dirs_none() {
        let conn = open_test_db();
        find_duplicate_directories(&conn, false, None, false, &[]).unwrap();
    }

    #[test]
    fn test_find_duplicate_dirs_detects_duplicates() {
        let conn = open_test_db();
        // Insert two directories with the same non-empty hash
        conn.execute_batch(
            "INSERT INTO directories VALUES ('/a/photos', 'deadbeef01234567', 1024);
             INSERT INTO directories VALUES ('/b/photos', 'deadbeef01234567', 1024);",
        )
        .unwrap();
        // delete=false so no interactive prompt is triggered.
        // Empty scanned_dirs means no filtering — both members are visible.
        find_duplicate_directories(&conn, false, None, false, &[]).unwrap();
    }
}
