mod db;
mod duplicates;
mod fs;
mod scan;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use db::init_database;
use duplicates::{find_duplicate_directories, find_duplicate_files};
use fs::count_files;
use scan::scan_directory;

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
        let invalid = scan_directory(&conn, directory, total_files, true)?;
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
