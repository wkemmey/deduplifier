use anyhow::Result;
use clap::Parser;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(name = "deduplifier")]
#[command(about = "Scan directories, compute hashes, and find duplicates", long_about = None)]
struct Args {
    /// directories to scan
    #[arg(required = true)]
    directories: Vec<PathBuf>,

    /// database file path
    #[arg(short, long, default_value = "deduplifier.db")]
    database: PathBuf,

    /// also list duplicate files (in addition to duplicate directories)
    #[arg(long)]
    files: bool,

    /// interactively delete duplicate directories
    #[arg(long)]
    delete: bool,

    /// canonical directory: when a duplicate exists under this path, auto-select it as the one to keep
    #[arg(long)]
    canon: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct FileEntry {
    path: String,
    hash: String,
    size: u64,
}

/// Convert a Path to a &str, returning a clear error if the path contains invalid UTF-8.
/// Files with invalid paths are skipped (not added to the DB) so the scan can continue.
fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        anyhow::anyhow!(
            "Path contains invalid UTF-8 characters: {}",
            path.to_string_lossy()
        )
    })
}

fn main() -> Result<()> {
    let args = Args::parse();

    let conn = init_database(&args.database)?;
    let mut total_invalid_paths = 0usize;

    for directory in &args.directories {
        if !directory.exists() {
            eprintln!(
                "Warning: Directory {:?} does not exist, skipping",
                directory
            );
            continue;
        }

        println!("Counting files in directory: {:?}", directory);
        let total_files = count_files(directory)?;
        println!("Found {} files to process", total_files);

        println!("Scanning directory: {:?}", directory);
        let invalid = scan_directory(&conn, directory, total_files)?;
        total_invalid_paths += invalid;
        println!(""); // New line after progress
    }

    if total_invalid_paths > 0 {
        eprintln!(
            "\nWarning: {} file path(s) with invalid UTF-8 were skipped during this scan.",
            total_invalid_paths
        );
        eprintln!("Please rename these files and re-run to get duplicate results.");
        return Ok(());
    }

    println!("\n=== Finding Duplicate Directories ===");
    find_duplicate_directories(&conn, args.delete, args.canon.as_deref())?;

    if args.files {
        println!("\n=== Finding Duplicate Files ===");
        find_duplicate_files(&conn)?;
    }

    Ok(())
}

fn count_files(root: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if entry.path().is_file() {
            count += 1;
        }
    }
    Ok(count)
}

fn init_database(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL,
            modified INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS directories (
            path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_hash ON files(hash)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dir_hash ON directories(hash)",
        [],
    )?;

    Ok(conn)
}

fn compute_file_hash(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

fn should_update_file(conn: &Connection, path: &Path, modified: SystemTime) -> Result<bool> {
    let path_str = path_to_str(path)?;

    let mut stmt = conn.prepare("SELECT modified FROM files WHERE path = ?1")?;

    let result: Result<i64, rusqlite::Error> = stmt.query_row(params![path_str], |row| row.get(0));

    match result {
        Ok(stored_modified) => {
            let modified_secs = modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;
            Ok(modified_secs != stored_modified)
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(true),
        Err(e) => Err(e.into()),
    }
}

fn scan_directory(conn: &Connection, root: &Path, total_files: usize) -> Result<usize> {
    let mut files_by_dir: HashMap<PathBuf, Vec<FileEntry>> = HashMap::new();
    let mut processed = 0;
    let mut invalid_paths = 0usize;

    // Create a temp table to track all files seen in this scan
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS visited_files (path TEXT PRIMARY KEY);
         DELETE FROM visited_files;",
    )?;

    // First pass: scan all files
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            processed += 1;
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("<unknown>");
            // \r - return to the start of the line
            // \x1B[K - clear everything from cursor to end of line
            print!("\r\x1B[K{}/{} - {}", processed, total_files, file_name);
            io::stdout().flush()?;

            let metadata = fs::metadata(path)?;
            let modified = metadata.modified()?;
            let size = metadata.len();

            let path_str = match path_to_str(path) {
                Ok(s) => s.to_string(),
                Err(e) => {
                    eprintln!("\nWarning: skipping file with invalid UTF-8 path: {}", e);
                    invalid_paths += 1;
                    continue;
                }
            };
            conn.execute(
                "INSERT OR IGNORE INTO visited_files (path) VALUES (?1)",
                params![path_str],
            )?;

            // Check if we need to update this file
            if should_update_file(conn, path, modified)? {
                match compute_file_hash(path) {
                    Ok(hash) => {
                        let modified_secs =
                            modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;

                        conn.execute(
                            "INSERT OR REPLACE INTO files (path, hash, size, modified) VALUES (?1, ?2, ?3, ?4)",
                            params![path_str, hash, size as i64, modified_secs],
                        )?;

                        if let Some(parent) = path.parent() {
                            files_by_dir
                                .entry(parent.to_path_buf())
                                .or_insert_with(Vec::new)
                                .push(FileEntry {
                                    path: path_str,
                                    hash,
                                    size,
                                });
                        }
                    }
                    Err(e) => {
                        eprintln!("Error hashing file {:?}: {}", path, e);
                    }
                }
            } else {
                // File hasn't changed, load from database
                let mut stmt = conn.prepare("SELECT hash, size FROM files WHERE path = ?1")?;
                let (hash, size): (String, i64) =
                    stmt.query_row(params![path_str], |row| Ok((row.get(0)?, row.get(1)?)))?;

                if let Some(parent) = path.parent() {
                    files_by_dir
                        .entry(parent.to_path_buf())
                        .or_insert_with(Vec::new)
                        .push(FileEntry {
                            path: path_str,
                            hash,
                            size: size as u64,
                        });
                }
            }
        }
    }

    // Find files in DB under root that were not seen in this scan
    let root_str = path_to_str(root)?.to_string();
    let stale_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE path LIKE ?1 AND path NOT IN (SELECT path FROM visited_files)",
        params![format!("{}%", root_str)],
        |row| row.get(0),
    )?;

    if stale_count > 0 {
        println!(
            "\n{} file(s) in the database no longer exist on disk under {:?}.",
            stale_count, root
        );
        print!("Delete them from the database? [y/N] ");
        io::stdout().flush()?;

        let stdin = io::stdin();
        let mut line = String::new();
        stdin.lock().read_line(&mut line)?;
        if line.trim().eq_ignore_ascii_case("y") {
            conn.execute(
                "DELETE FROM files WHERE path LIKE ?1 AND path NOT IN (SELECT path FROM visited_files)",
                params![format!("{}%", root_str)],
            )?;
            println!("Deleted {} stale file(s) from the database.", stale_count);
        } else {
            println!("Skipped deletion of stale entries.");
        }
    }

    // Second pass: compute directory hashes bottom-up
    let mut dir_entries: Vec<PathBuf> = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.path().to_path_buf())
        .collect();

    // Sort by depth (deepest first) to ensure bottom-up processing
    dir_entries.sort_by(|a, b| b.components().count().cmp(&a.components().count()));

    for dir_path in dir_entries {
        compute_directory_hash(conn, &dir_path, &files_by_dir)?;
    }

    Ok(invalid_paths)
}

fn compute_directory_hash(
    conn: &Connection,
    dir_path: &Path,
    files_by_dir: &HashMap<PathBuf, Vec<FileEntry>>,
) -> Result<()> {
    let mut items = Vec::new();

    // Get immediate child files
    if let Some(files) = files_by_dir.get(dir_path) {
        for file in files {
            // Use just the filename, not the full path
            if let Some(filename) = Path::new(&file.path).file_name() {
                items.push((
                    filename.to_string_lossy().to_string(),
                    file.hash.clone(),
                    file.size,
                ));
            }
        }
    }

    // Get immediate child directories from database
    let dir_path_str = path_to_str(dir_path)?.to_string();
    let mut stmt = conn.prepare(
        "SELECT path, hash, size FROM directories WHERE path LIKE ?1 AND path NOT LIKE ?2",
    )?;
    let pattern1 = format!("{}%", dir_path_str);
    let pattern2 = format!("{}%/%", dir_path_str);
    let dir_iter = stmt.query_map(params![pattern1, pattern2], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;

    for dir in dir_iter {
        let (path, hash, size) = dir?;
        let child_path = PathBuf::from(&path);
        if let Some(parent) = child_path.parent() {
            if parent == dir_path {
                // Use just the directory name, not the full path
                if let Some(dirname) = child_path.file_name() {
                    items.push((dirname.to_string_lossy().to_string(), hash, size as u64));
                }
            }
        }
    }

    // Sort items by name for repeatability
    items.sort_by(|a, b| a.0.cmp(&b.0));

    // Compute combined hash using only relative names and content hashes
    let mut hasher = Sha256::new();
    let mut total_size = 0u64;
    for (name, hash, size) in &items {
        hasher.update(name.as_bytes());
        hasher.update(b":");
        hasher.update(hash.as_bytes());
        hasher.update(b"\n");
        total_size += size;
    }
    let result = hasher.finalize();
    let dir_hash = format!("{:x}", result);

    conn.execute(
        "INSERT OR REPLACE INTO directories (path, hash, size) VALUES (?1, ?2, ?3)",
        params![dir_path_str, dir_hash, total_size as i64],
    )?;

    Ok(())
}

fn find_duplicate_files(conn: &Connection) -> Result<()> {
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

fn find_duplicate_directories(conn: &Connection, delete: bool, canon: Option<&Path>) -> Result<()> {
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
            print!(
                "  Type the directory name to confirm deletion of\n  '{}'\n  > ",
                path
            );
            io::stdout().flush()?;
            let mut confirmation = String::new();
            stdin.lock().read_line(&mut confirmation)?;
            if confirmation.trim() != *path {
                println!("  Name did not match — skipping deletion of '{}'.", path);
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
    }

    Ok(())
}
