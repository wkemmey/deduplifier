mod db;
mod duplicates;
mod file_system;
mod hashing;
mod merge;
mod photos;
mod scan;
mod similar;
mod utils;

use anyhow::Result;
use clap::Parser;
use std::path::{Path, PathBuf};

use db::init_database;
use duplicates::{find_duplicate_directories, find_duplicate_files};
use hashing::count_files;
use merge::merge_into_canon;
use photos::sort_photos;
use scan::scan_directory;
use similar::find_similar_directories;

#[derive(Parser, Debug)]
#[command(name = "deduplifier")]
#[command(about = "Scan directories, compute hashes, and find duplicates", long_about = None)]
struct Args {
    /// directories to scan
    #[arg(required = true)]
    directories: Vec<PathBuf>,

    // ── Main operation (exactly one required) ────────────────────────────────
    /// find duplicate directories and optionally delete them (see --delete)
    #[arg(long)]
    dup_dirs: bool,

    /// find duplicate files
    #[arg(long)]
    dup_files: bool,

    /// find and interactively merge similar (but non-identical) directories;
    /// optionally specify a similarity threshold (0.0–1.0, default 0.85)
    #[arg(long, value_name = "THRESHOLD")]
    similarity: Option<Option<f64>>,

    /// merge two directory trees together (not yet implemented)
    #[arg(long)]
    merge: bool,

    /// sort photos into a date-based folder hierarchy (not yet implemented)
    #[arg(long)]
    sort_photos: bool,

    // ── Common options ────────────────────────────────────────────────────────
    /// database file path
    #[arg(long, default_value = "deduplifier.db")]
    database: PathBuf,

    /// canonical directory: auto-selects the keeper for duplicates; required by --sort-photos as the root for date-based dirs
    #[arg(long)]
    canon: Option<PathBuf>,

    /// interactively delete duplicate directories (used with --dup-dirs)
    #[arg(long)]
    delete: bool,

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

    // Exactly one main operation must be specified.
    let op_count = [
        args.dup_dirs,
        args.dup_files,
        args.similarity.is_some(),
        args.merge,
        args.sort_photos,
    ]
    .iter()
    .filter(|&&b| b)
    .count();
    if op_count == 0 {
        eprintln!(
            "Error: specify one of --dup-dirs, --dup-files, --similarity, --merge, or --sort-photos."
        );
        std::process::exit(1);
    }
    if op_count > 1 {
        eprintln!(
            "Error: --dup-dirs, --dup-files, --similarity, --merge, and --sort-photos are mutually exclusive."
        );
        std::process::exit(1);
    }

    let conn = init_database(&args.database)?;
    let mut total_invalid_paths = 0usize;

    let all_directories: Vec<&Path> = build_scan_list(&args.directories, args.canon.as_ref())
        .into_iter()
        .map(|p| p.as_path())
        .collect();

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
        eprintln!("Please rename these files and re-run to continue.");
        return Ok(());
    }

    if args.dup_dirs {
        println!("\n=== Finding duplicate directories ===");
        find_duplicate_directories(
            &conn,
            args.delete,
            args.canon.as_deref(),
            args.no_confirmation,
            &all_directories,
        )?
    } else if args.dup_files {
        println!("\n=== Finding duplicate files ===");
        find_duplicate_files(&conn)?;
    } else if let Some(threshold_opt) = args.similarity {
        let threshold = threshold_opt.unwrap_or(0.85);
        println!(
            "\n=== Finding similar (near-duplicate) directories (threshold: {:.0}%) ===",
            threshold * 100.0
        );
        find_similar_directories(&conn, threshold, &all_directories, true)?
    } else if args.merge {
        if !args.delete {
            eprintln!(
                "Error: --merge requires --delete, because it will delete files without prompting."
            );
            std::process::exit(1);
        }
        let canon = match args.canon.as_deref() {
            Some(c) => c,
            None => {
                eprintln!("Error: --merge requires --canon to specify the merge target.");
                std::process::exit(1);
            }
        };
        // sources = everything except canon
        let sources: Vec<&Path> = all_directories
            .iter()
            .copied()
            .filter(|&p| p != canon)
            .collect();
        println!("\n=== Merging directories into canon ===");
        merge_into_canon(&conn, canon, &sources, args.no_confirmation)?;
    } else if args.sort_photos {
        if !args.delete || !args.no_confirmation {
            eprintln!(
                "Error: --sort-photos requires both --delete and --no-confirmation, \
                 because it will move and delete files without prompting."
            );
            std::process::exit(1);
        }
        let canon = match args.canon.as_deref() {
            Some(c) => c,
            None => {
                eprintln!("Error: --sort-photos requires --canon to specify where date-based directories will be created.");
                std::process::exit(1);
            }
        };
        println!("\n=== Sorting photos into date-based directories ===");
        sort_photos(&conn, &all_directories, canon)?;
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
