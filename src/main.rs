mod db;
mod duplicates;
mod file_system;
mod hashing;
mod merge;
mod photos;
mod scan;
mod similar;
mod ui;
mod utils;

use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Parser;

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

    /// merge two directory trees together
    #[arg(long)]
    merge: bool,

    /// sort photos into a date-based folder hierarchy
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

    let conn = db::init_database(&args.database)?;

    let all_directories: Vec<&Path> = build_scan_list(&args.directories, args.canon.as_ref())
        .into_iter()
        .map(|p| p.as_path())
        .collect();

    ui::run_scan(&conn, &all_directories)?;

    enum Op<'a> {
        DupDirs,
        DupFiles,
        Similarity(f64),
        Merge { canon: &'a Path },
        SortPhotos { canon: &'a Path },
    }

    let op = if args.dup_dirs {
        Op::DupDirs
    } else if args.dup_files {
        Op::DupFiles
    } else if let Some(threshold_opt) = args.similarity {
        Op::Similarity(threshold_opt.unwrap_or(0.85))
    } else if args.merge {
        if !args.delete {
            eprintln!(
                "Error: --merge requires --delete, because it will delete files without prompting."
            );
            std::process::exit(1);
        }
        let canon = args.canon.as_deref().unwrap_or_else(|| {
            eprintln!("Error: --merge requires --canon to specify the merge target.");
            std::process::exit(1);
        });
        Op::Merge { canon }
    } else {
        // sort_photos (op_count check above guarantees exactly one op)
        if !args.delete || !args.no_confirmation {
            eprintln!(
                "Error: --sort-photos requires both --delete and --no-confirmation, \
                 because it will move and delete files without prompting."
            );
            std::process::exit(1);
        }
        let canon = args.canon.as_deref().unwrap_or_else(|| {
            eprintln!("Error: --sort-photos requires --canon to specify where date-based directories will be created.");
            std::process::exit(1);
        });
        Op::SortPhotos { canon }
    };

    match op {
        Op::DupDirs => {
            ui::show_section("Finding duplicate directories");
            ui::run_dup_dirs(
                &conn,
                args.delete,
                args.canon.as_deref(),
                args.no_confirmation,
                &all_directories,
            )?;
        }
        Op::DupFiles => {
            ui::show_section("Finding duplicate files");
            ui::run_dup_files(&conn)?;
        }
        Op::Similarity(threshold) => {
            ui::show_similarity_section(threshold);
            ui::run_similar(&conn, threshold, &all_directories, true)?;
        }
        Op::Merge { canon } => {
            let sources: Vec<&Path> = all_directories
                .iter()
                .copied()
                .filter(|&p| p != canon)
                .collect();
            ui::show_section("Merging directories into canon");
            ui::run_merge(&conn, canon, &sources, args.no_confirmation)?;
        }
        Op::SortPhotos { canon } => {
            ui::show_section("Sorting photos into date-based directories");
            ui::run_sort_photos(&conn, &all_directories, canon)?;
        }
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
    fn test_build_scan_list_empty_dirs_no_canon() {
        let dirs: Vec<PathBuf> = vec![];
        let list = build_scan_list(&dirs, None);
        assert!(list.is_empty());
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
