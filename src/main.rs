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
    #[arg(required = true, long_help = "\
Directories to scan. All provided directories are walked recursively, \
every file is hashed (SHA-256), and the results are stored in the database. \
On subsequent runs only files whose modification time has changed are re-hashed, \
so rescans of large trees are fast. You may list as many directories as you like; \
they are scanned in the order given (with --canon, if provided, always scanned first).")]
    directories: Vec<PathBuf>,

    // ── Main operation (exactly one required) ────────────────────────────────
    /// find duplicate directories and optionally delete them (see --delete)
    #[arg(long, long_help = "\
Find duplicate directories and optionally delete them (see --delete). \
Two directories are considered duplicates when their combined file hashes are \
identical — meaning they contain exactly the same set of files with the same \
content, regardless of filenames inside. Only top-level duplicate groups are \
reported; subdirectories that are already covered by a parent duplicate are \
suppressed. Use --delete to enter an interactive deletion session, and --canon \
to automatically designate one copy as the keeper.")]
    dup_dirs: bool,

    /// find duplicate files
    #[arg(long, long_help = "\
Find duplicate files across all scanned directories. Files are grouped by \
content hash; any hash that appears more than once is reported along with every \
path that holds that content and the total wasted space. This operation does not \
delete anything — it is read-only and safe to run at any time.")]
    dup_files: bool,

    /// find and interactively merge similar (but non-identical) directories;
    /// optionally specify a similarity threshold (0.0–1.0, default 0.85)
    #[arg(long, value_name = "THRESHOLD", long_help = "\
Find and interactively merge similar but non-identical directories. Similarity \
is measured as the fraction of files (by hash) that two directories share. The \
optional threshold (0.0–1.0, default 0.85) sets the minimum similarity required \
to flag a pair. For each flagged pair you are shown the overlap and can choose \
to merge them: unique files from the source are copied into the destination, and \
true duplicates in the source are deleted. After a merge you should re-run \
without --similarity to detect any new exact duplicates that were created.")]
    similarity: Option<Option<f64>>,

    /// merge two directory trees together
    #[arg(long, long_help = "\
Merge two or more directory trees into --canon. Every file found under the \
source directories (any directory that is not --canon) is moved into the \
matching subdirectory path under --canon. If a file with identical content \
already exists in --canon it is treated as a true duplicate and deleted from \
the source instead of being moved. Files with the same name but different \
content are renamed with a numeric suffix to avoid collisions. The database \
is updated to reflect every move and deletion.")]
    merge: bool,

    /// sort photos into a date-based folder hierarchy
    #[arg(long, long_help = "\
Sort media files into a date-based folder hierarchy under --canon. Each file \
is placed into YYYY/YYYY-MM/YYYY-MM-DD/ subdirectories derived first from its \
EXIF DateTimeOriginal tag (falling back to DateTimeDigitized, then DateTime), \
and finally from the file's modification time if no valid EXIF date is found. \
Files that are already in the correct destination are skipped. True duplicates \
(identical content already present at the destination) are deleted. Name \
collisions between files with different content are resolved by appending a \
numeric suffix. Requires --delete and --no-confirmation because it moves and \
deletes files without prompting.")]
    sort_photos: bool,

    // ── Common options ────────────────────────────────────────────────────────
    /// database file path
    #[arg(long, default_value = "deduplifier.db", long_help = "\
Path to the SQLite database file used to cache file hashes and directory \
metadata between runs. Defaults to deduplifier.db in the current working \
directory. Specify a custom path to maintain separate databases for different \
sets of directories, or to keep the database next to the files being managed.")]
    database: PathBuf,

    /// canonical directory: auto-selects the keeper for duplicates; required by --sort-photos as the root for date-based dirs
    #[arg(long, long_help = "\
Designates one directory as the canonical copy. With --dup-dirs and --delete, \
when a duplicate group contains a directory under --canon, that copy is \
automatically kept and the others are deleted without prompting (unless the \
group has no canon member, in which case you are still asked). With --merge, \
all other directories are merged into --canon. With --sort-photos, date-based \
subdirectories are created inside --canon and all media files are moved there. \
Canon is always scanned first so its hashes are in the database before any \
other directory is processed.")]
    canon: Option<PathBuf>,

    /// interactively delete duplicate directories (used with --dup-dirs)
    #[arg(long, long_help = "\
Enable interactive deletion when used with --dup-dirs. For each duplicate \
group you are shown the directories involved and asked which one to keep; the \
rest are deleted along with their contents. If --canon is provided and one of \
the duplicates lives under it, that copy is selected automatically and you are \
only prompted to confirm (unless --no-confirmation is also given).")]
    delete: bool,

    /// skip per-deletion confirmation prompts when --canon has auto-selected the keeper
    #[arg(long, long_help = "\
Skip the per-deletion confirmation prompt in cases where --canon has \
unambiguously identified the keeper. Without this flag you are still asked to \
confirm each auto-selected deletion. With this flag those deletions proceed \
silently. You are still prompted for groups where no canon member exists. \
Required by --sort-photos, which always operates non-interactively.")]
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
