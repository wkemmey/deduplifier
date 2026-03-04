mod db;
mod duplicates;
mod hashing;
mod scan;

use anyhow::Result;
use clap::Parser;
use std::path::{Path, PathBuf};

use db::init_database;
use duplicates::{find_duplicate_directories, find_duplicate_files};
use hashing::count_files;
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

    /// skip per-deletion confirmation prompts when --canon has auto-selected the keeper
    #[arg(long)]
    no_confirmation: bool,
}

/// Build the ordered list of directories to scan: canon first (if provided and not already
/// present), then the rest. Canon is first so its hashes are in the DB before we scan others.
pub fn build_scan_list<'a>(
    directories: &'a [PathBuf],
    canon: Option<&'a PathBuf>,
) -> Vec<&'a PathBuf> {
    let mut list: Vec<&PathBuf> = Vec::new();
    if let Some(c) = canon {
        list.push(c);
    }
    for dir in directories {
        if !list.contains(&dir) {
            list.push(dir);
        }
    }
    list
}

fn main() -> Result<()> {
    let args = Args::parse();

    let conn = init_database(&args.database)?;
    let mut total_invalid_paths = 0usize;

    let all_directories = build_scan_list(&args.directories, args.canon.as_ref());

    for directory in &all_directories {
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

    println!("\n=== Finding duplicate directories ===");
    let scanned_paths: Vec<&Path> = all_directories.iter().map(|p| p.as_path()).collect();
    find_duplicate_directories(
        &conn,
        args.delete,
        args.canon.as_deref(),
        args.no_confirmation,
        &scanned_paths,
    )?;

    if args.files {
        println!("\n=== Finding duplicate files ===");
        find_duplicate_files(&conn)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn test_build_scan_list_no_canon() {
        let dirs = vec![p("/a"), p("/b")];
        let list = build_scan_list(&dirs, None);
        assert_eq!(list, vec![&p("/a"), &p("/b")]);
    }

    #[test]
    fn test_build_scan_list_canon_prepended() {
        // Canon not in directories list — should appear first
        let dirs = vec![p("/other")];
        let canon = p("/canon");
        let list = build_scan_list(&dirs, Some(&canon));
        assert_eq!(list, vec![&p("/canon"), &p("/other")]);
    }

    #[test]
    fn test_build_scan_list_canon_already_in_dirs() {
        // Canon already listed in directories — should not be duplicated
        let dirs = vec![p("/canon"), p("/other")];
        let canon = p("/canon");
        let list = build_scan_list(&dirs, Some(&canon));
        assert_eq!(list, vec![&p("/canon"), &p("/other")]);
    }

    #[test]
    fn test_build_scan_list_canon_is_first() {
        // Even if canon appears last in directories, it should be first in the scan list
        let dirs = vec![p("/other"), p("/canon")];
        let canon = p("/canon");
        let list = build_scan_list(&dirs, Some(&canon));
        assert_eq!(list[0], &p("/canon"));
    }
}
