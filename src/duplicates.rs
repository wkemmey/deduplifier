use anyhow::Result;
use rusqlite::{params, Connection};
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

    let stdin = io::stdin();

    for (hash, count, max_size) in &duplicate_groups {
        // Fetch the directories in this group
        let mut dir_stmt =
            conn.prepare("SELECT path, size FROM directories WHERE hash = ?1 ORDER BY path")?;
        let dirs: Vec<(String, i64)> = dir_stmt
            .query_map(params![hash], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;

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
            print!("  Confirm deletion of '{}' [y/N] > ", path);
            io::stdout().flush()?;
            let mut confirmation = String::new();
            stdin.lock().read_line(&mut confirmation)?;
            if !confirmation.trim().eq_ignore_ascii_case("y") {
                println!("  Skipped.");
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
        find_duplicate_directories(&conn, false, None).unwrap();
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
        // delete=false so no interactive prompt is triggered
        find_duplicate_directories(&conn, false, None).unwrap();
    }
}
