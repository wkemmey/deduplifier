use anyhow::Result;
use clap::Parser;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(name = "deduplifier")]
#[command(about = "Scan directories, compute hashes, and find duplicates", long_about = None)]
struct Args {
    /// Directories to scan
    #[arg(required = true)]
    directories: Vec<PathBuf>,

    /// Database file path
    #[arg(short, long, default_value = "deduplifier.db")]
    database: PathBuf,
}

#[derive(Debug, Clone)]
struct FileEntry {
    path: String,
    hash: String,
    size: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    let conn = init_database(&args.database)?;
    
    for directory in &args.directories {
        if !directory.exists() {
            eprintln!("Warning: Directory {:?} does not exist, skipping", directory);
            continue;
        }
        
        println!("Scanning directory: {:?}", directory);
        scan_directory(&conn, directory)?;
    }
    
    println!("\n=== Finding Duplicate Files ===");
    find_duplicate_files(&conn)?;
    
    println!("\n=== Finding Duplicate Directories ===");
    find_duplicate_directories(&conn)?;
    
    Ok(())
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
    let contents = fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(&contents);
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

fn should_update_file(conn: &Connection, path: &Path, modified: SystemTime) -> Result<bool> {
    let path_str = path.to_string_lossy().to_string();
    
    let mut stmt = conn.prepare(
        "SELECT modified FROM files WHERE path = ?1"
    )?;
    
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

fn scan_directory(conn: &Connection, root: &Path) -> Result<()> {
    let mut files_by_dir: HashMap<PathBuf, Vec<FileEntry>> = HashMap::new();
    
    // First pass: scan all files
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            let metadata = fs::metadata(path)?;
            let modified = metadata.modified()?;
            let size = metadata.len();
            
            // Check if we need to update this file
            if should_update_file(conn, path, modified)? {
                match compute_file_hash(path) {
                    Ok(hash) => {
                        let path_str = path.to_string_lossy().to_string();
                        let modified_secs = modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;
                        
                        conn.execute(
                            "INSERT OR REPLACE INTO files (path, hash, size, modified) VALUES (?1, ?2, ?3, ?4)",
                            params![path_str, hash, size as i64, modified_secs],
                        )?;
                        
                        if let Some(parent) = path.parent() {
                            files_by_dir.entry(parent.to_path_buf()).or_insert_with(Vec::new).push(FileEntry {
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
                let path_str = path.to_string_lossy().to_string();
                let mut stmt = conn.prepare("SELECT hash, size FROM files WHERE path = ?1")?;
                let (hash, size): (String, i64) = stmt.query_row(params![path_str], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
                
                if let Some(parent) = path.parent() {
                    files_by_dir.entry(parent.to_path_buf()).or_insert_with(Vec::new).push(FileEntry {
                        path: path_str,
                        hash,
                        size: size as u64,
                    });
                }
            }
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
    
    Ok(())
}

fn compute_directory_hash(conn: &Connection, dir_path: &Path, files_by_dir: &HashMap<PathBuf, Vec<FileEntry>>) -> Result<()> {
    let mut items = Vec::new();
    
    // Get immediate child files
    if let Some(files) = files_by_dir.get(dir_path) {
        for file in files {
            items.push((file.path.clone(), file.hash.clone(), file.size));
        }
    }
    
    // Get immediate child directories from database
    let dir_path_str = dir_path.to_string_lossy().to_string();
    let mut stmt = conn.prepare("SELECT path, hash, size FROM directories")?;
    let dir_iter = stmt.query_map([], |row| {
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
                items.push((path, hash, size as u64));
            }
        }
    }
    
    // Sort items by name for repeatability
    items.sort_by(|a, b| a.0.cmp(&b.0));
    
    // Compute combined hash
    let mut hasher = Sha256::new();
    let mut total_size = 0u64;
    for (path, hash, size) in &items {
        hasher.update(path.as_bytes());
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
         ORDER BY total_size DESC"
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
        println!("\nDuplicate files (hash: {}, count: {}, total size: {} bytes):", 
                 &hash[..16], count, total_size);
        
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

fn find_duplicate_directories(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT hash, COUNT(*) as count, AVG(size) as avg_size 
         FROM directories 
         GROUP BY hash 
         HAVING count > 1
         ORDER BY avg_size DESC"
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
        let (hash, count, avg_size) = dup?;
        found_any = true;
        println!("\nDuplicate directories (hash: {}, count: {}, avg size: {} bytes):", 
                 &hash[..16], count, avg_size);
        
        let mut dir_stmt = conn.prepare("SELECT path, size FROM directories WHERE hash = ?1")?;
        let dirs = dir_stmt.query_map(params![hash], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        
        for dir in dirs {
            let (path, size) = dir?;
            println!("  - {} ({} bytes)", path, size);
        }
    }
    
    if !found_any {
        println!("No duplicate directories found.");
    }
    
    Ok(())
}
