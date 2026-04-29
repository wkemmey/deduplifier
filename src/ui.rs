use std::io::{self, BufRead, Write};
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

use crate::{db, duplicates, file_system, hashing, merge, photos, scan, similar, utils};

// ---------------------------------------------------------------------------
// Scan progress
// ---------------------------------------------------------------------------

pub fn scan_progress(processed: usize, total: usize, file_name: &str) {
    // \r - return to start of line; \x1B[K - clear to end of line
    print!("\r\x1B[K{}/{} - {}", processed, total, file_name);
    let _ = io::stdout().flush();
}

pub fn show_checking_stale() {
    println!("\nChecking for stale database entries (this may take several minutes for large directories)...");
    let _ = io::stdout().flush();
}

pub fn prompt_delete_stale(stale_count: i64, root: &Path) -> Result<bool> {
    println!(
        "\n{} file(s) in the database no longer exist on disk under {:?}.",
        stale_count, root
    );
    print!("Delete them from the database? [y/N] ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    Ok(line.trim().eq_ignore_ascii_case("y"))
}

pub fn show_deleted_stale(count: i64) {
    println!("Deleted {} stale file(s) from the database.", count);
}

pub fn show_skipped_stale() {
    println!("Skipped deletion of stale entries.");
}

// ---------------------------------------------------------------------------
// Main-level progress / section headers
// ---------------------------------------------------------------------------

pub fn show_counting_files(dir: &Path) {
    println!("Counting files in directory: {:?}", dir);
}

pub fn show_file_count(count: usize) {
    println!("Found {} files to process", count);
}

pub fn show_scanning_dir(dir: &Path) {
    println!("Scanning directory: {:?}", dir);
}

pub fn run_scan(conn: &Connection, directories: &[&Path]) -> Result<()> {
    let mut total_invalid_paths = 0usize;
    for &directory in directories {
        if !directory.exists() {
            eprintln!(
                "Warning: Directory {:?} does not exist, skipping",
                directory
            );
            continue;
        }
        show_counting_files(directory);
        let total_files = hashing::count_files(directory)?;
        show_file_count(total_files);
        show_scanning_dir(directory);
        let result =
            scan::scan_directory(conn, directory, total_files, |processed, total, name| {
                scan_progress(processed, total, name);
            })?;
        total_invalid_paths += result.invalid_paths;
        show_scan_newline();
        if result.stale_count > 0 {
            show_checking_stale();
            let root = std::path::Path::new(&result.root_str);
            if prompt_delete_stale(result.stale_count, root)? {
                db::delete_stale_files(conn, &result.root_str)?;
                show_deleted_stale(result.stale_count);
            } else {
                show_skipped_stale();
            }
        }
    }
    if total_invalid_paths > 0 {
        eprintln!(
            "\nWarning: {} file path(s) with invalid UTF-8 were skipped during this scan.",
            total_invalid_paths
        );
        eprintln!("Please rename these files and re-run to continue.");
        std::process::exit(1);
    }
    Ok(())
}

pub fn show_scan_newline() {
    println!(); // blank line after progress bar
}

// ---------------------------------------------------------------------------
// Driving functions (run_*) — call logic, handle prompts, drive the loop
// ---------------------------------------------------------------------------

pub fn run_dup_files(conn: &Connection) -> Result<()> {
    let groups = duplicates::find_duplicate_files(conn)?;
    if groups.is_empty() {
        show_no_duplicate_files();
        return Ok(());
    }
    for group in &groups {
        show_duplicate_file_group(&group.hash, group.count, group.total_size, &group.files);
    }
    Ok(())
}

pub fn run_dup_dirs(
    conn: &Connection,
    delete: bool,
    canon: Option<&Path>,
    no_confirmation: bool,
    scanned_dirs: &[&Path],
) -> Result<()> {
    let duplicate_group_hashes = db::duplicate_directory_groups(conn)?;
    if duplicate_group_hashes.is_empty() {
        show_no_duplicate_dirs();
        return Ok(());
    }
    let (top_level_groups, covered_count) =
        duplicates::build_top_level_groups(conn, &duplicate_group_hashes, scanned_dirs)?;
    show_dup_dirs_summary(top_level_groups.len(), covered_count);
    for group in &top_level_groups {
        show_dup_dir_group(group);
        if !delete {
            continue;
        }
        let dirs = &group.members;
        let auto_keep: Option<usize> = if let Some(canon_path) = canon {
            dirs.iter()
                .position(|e| std::path::Path::new(&e.path).starts_with(canon_path))
        } else {
            None
        };
        if no_confirmation {
            if let Some(canon_path) = canon {
                let canon_count = dirs
                    .iter()
                    .filter(|e| std::path::Path::new(&e.path).starts_with(canon_path))
                    .count();
                if canon_count > 1 {
                    show_dup_dir_canon_conflict_warning(canon_count, canon_path);
                    continue;
                }
            }
        }
        let keep_idx: usize = if let Some(idx) = auto_keep {
            show_dup_dir_auto_keep(idx, &dirs[idx].path);
            idx
        } else {
            match prompt_keep_which(dirs.len())? {
                None => continue,
                Some(idx) => idx,
            }
        };
        let to_delete: Vec<&str> = dirs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != keep_idx)
            .map(|(_, e)| e.path.as_str())
            .collect();
        show_dup_dir_deletion_plan(&dirs[keep_idx].path, &to_delete);
        for path in &to_delete {
            let auto_confirmed = no_confirmation && auto_keep.is_some();
            if !prompt_confirm_deletion(path, auto_confirmed)? {
                continue;
            }
            let dir_path = std::path::Path::new(path);
            if dir_path.exists() {
                file_system::delete_dir_all(dir_path)?;
                show_dup_dir_deleted(path);
            } else {
                show_dup_dir_missing(path);
            }
            db::remove_tree(conn, dir_path)?;
            show_dup_dir_db_removed(path);
        }
        show_dup_dir_group_end();
    }
    Ok(())
}

pub fn run_merge(
    conn: &Connection,
    canon: &Path,
    sources: &[&Path],
    no_confirmation: bool,
) -> Result<()> {
    for &source in sources {
        if source == canon {
            show_merge_skipped_self(source);
            continue;
        }
        show_merge_header(source, canon);
        let (score, intersection, union) = merge::similarity_score(conn, canon, source)?;
        show_merge_similarity(score, intersection, union);
        if score < merge::SIMILARITY_THRESHOLD && !no_confirmation {
            if !prompt_low_similarity(merge::SIMILARITY_THRESHOLD)? {
                show_merge_skipped();
                continue;
            }
        }
        let stats = merge::execute_merge(
            conn,
            canon,
            source,
            no_confirmation,
            |rel, dest_abs, dest_mtime, src_abs, src_mtime| {
                let choice = prompt_merge_conflict(rel, dest_abs, dest_mtime, src_abs, src_mtime)?;
                Ok(choice)
            },
        )?;
        show_merge_summary(stats.moved, stats.deleted_dups, stats.skipped);
    }
    Ok(())
}

pub fn run_similar(
    conn: &Connection,
    threshold: f64,
    scanned_dirs: &[&Path],
    do_merge: bool,
) -> Result<()> {
    let pairs =
        similar::compute_similar_pairs(conn, threshold, scanned_dirs, |event| match event {
            similar::SimilarProgress::LoadingIndex => show_loading_file_index(),
            similar::SimilarProgress::FindingLeaves => show_finding_leaf_dirs(),
            similar::SimilarProgress::LeafCount(n) => show_leaf_dir_count(n),
            similar::SimilarProgress::CheckingCandidates(n) => show_checking_candidates(n),
            similar::SimilarProgress::CandidateProgress {
                checked,
                total,
                found,
            } => {
                show_candidate_progress(checked, total, found);
            }
            similar::SimilarProgress::Done => show_candidate_progress_end(),
        })?;

    if pairs.is_empty() {
        show_no_similar_pairs();
        return Ok(());
    }

    show_similar_pairs_header(pairs.len(), threshold);

    for pair in &pairs {
        show_similar_pair(pair);

        if !do_merge {
            show_similar_pair_end();
            continue;
        }

        show_similar_pair_merge_plan(pair);

        let resolution = match prompt_similar_resolution()? {
            None => {
                show_similar_skipped();
                continue;
            }
            Some(r) => r,
        };

        let copied = similar::merge_into(
            pair,
            resolution,
            &mut |event| match event {
                similar::MergeEvent::CopiedOnlyInA(rel) => show_similar_copied_only_in_a(rel),
                similar::MergeEvent::CopiedOnlyInB(rel) => show_similar_copied_only_in_b(rel),
                similar::MergeEvent::KeptNewer(rel) => show_similar_kept_newer(rel),
                similar::MergeEvent::KeptOlder(rel) => show_similar_kept_older(rel),
            },
            &mut |fname, side_old, date_old, side_new, date_new| {
                prompt_similar_file_conflict(fname, side_old, date_old, side_new, date_new)
            },
        )?;
        show_similar_merge_complete(copied);
    }
    Ok(())
}

/// Print "\n=== {name} ===" section header.
pub fn show_section(name: &str) {
    println!("\n=== {} ===", name);
}

pub fn show_similarity_section(threshold: f64) {
    println!(
        "\n=== Finding similar (near-duplicate) directories (threshold: {:.0}%) ===",
        threshold * 100.0
    );
}

// ---------------------------------------------------------------------------
// Duplicate files
// ---------------------------------------------------------------------------

pub fn show_no_duplicate_files() {
    println!("No duplicate files found.");
}

pub fn show_duplicate_file_group(
    hash: &str,
    count: i64,
    total_size: i64,
    records: &[db::FileRecord],
) {
    let hash_display = if hash.len() >= 16 { &hash[..16] } else { hash };
    println!(
        "\nDuplicate files (hash: {}, count: {}, total size: {} bytes):",
        hash_display, count, total_size
    );
    for record in records {
        println!("  - {} ({} bytes)", record.path, record.size);
    }
}

// ---------------------------------------------------------------------------
// Duplicate directories
// ---------------------------------------------------------------------------

pub fn show_no_duplicate_dirs() {
    println!("No duplicate directories found.");
}

pub fn show_dup_dirs_summary(top_level: usize, covered: usize) {
    println!(
        "Found {} set(s) of duplicate directories ({} are subdirectories of other duplicates and will be skipped).",
        top_level, covered,
    );
}

pub fn show_dup_dir_group(group: &duplicates::DuplicateGroup) {
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
}

pub fn show_dup_dir_canon_conflict_warning(canon_count: usize, canon_path: &Path) {
    println!(
        "  Warning: {} members are under --canon ({}); skipping this group.",
        canon_count,
        canon_path.display()
    );
    println!();
}

pub fn show_dup_dir_auto_keep(idx: usize, path: &str) {
    println!("  Auto-selecting [{}] as canonical: {}", idx + 1, path);
}

/// Prompt the user to choose which directory to keep.
/// Returns `Some(0-based index)` or `None` to skip this group.
pub fn prompt_keep_which(count: usize) -> Result<Option<usize>> {
    print!("  Keep which? (1-{}, or 's' to skip): ", count);
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    let trimmed = line.trim();
    if trimmed.eq_ignore_ascii_case("s") {
        println!("  Skipped.");
        return Ok(None);
    }
    match trimmed.parse::<usize>() {
        Ok(n) if n >= 1 && n <= count => Ok(Some(n - 1)),
        _ => {
            println!("  Invalid choice, skipping.");
            Ok(None)
        }
    }
}

pub fn show_dup_dir_deletion_plan(keep_path: &str, to_delete: &[&str]) {
    println!("  Keeping:  {}", keep_path);
    println!("  Will permanently delete:");
    for path in to_delete {
        println!("    - {}", path);
    }
}

/// Confirm deletion of `path`.
/// `auto_confirmed` — true when `--no-confirmation` + canon drove the choice;
/// prints a notice and returns `true` without prompting.
pub fn prompt_confirm_deletion(path: &str, auto_confirmed: bool) -> Result<bool> {
    if auto_confirmed {
        println!("  Deleting '{}' (--no-confirmation)", path);
        return Ok(true);
    }
    print!("  Confirm deletion of '{}' [y/N] > ", path);
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    if !line.trim().eq_ignore_ascii_case("y") {
        println!("  Skipped.");
        Ok(false)
    } else {
        Ok(true)
    }
}

pub fn show_dup_dir_deleted(path: &str) {
    println!("  Deleted '{}'.", path);
}

pub fn show_dup_dir_missing(path: &str) {
    println!("  '{}' no longer exists on disk, skipping.", path);
}

pub fn show_dup_dir_db_removed(path: &str) {
    println!("  Removed '{}' and its contents from the database.", path);
}

pub fn show_dup_dir_group_end() {
    println!();
}

// ---------------------------------------------------------------------------
// Merge (merge.rs)
// ---------------------------------------------------------------------------

pub fn show_merge_skipped_self(path: &Path) {
    println!(
        "Skipping: source is the same as canon ({}).",
        path.display()
    );
}

pub fn show_merge_header(source: &Path, canon: &Path) {
    println!(
        "\n--- Merging {} into {} ---",
        source.display(),
        canon.display()
    );
}

pub fn show_merge_similarity(score: f64, intersection: usize, union: usize) {
    println!(
        "  Similarity to canon: {:.1}%  ({} shared hashes, {} total unique)",
        score * 100.0,
        intersection,
        union,
    );
}

pub fn prompt_low_similarity(threshold: f64) -> Result<bool> {
    print!(
        "  Warning: similarity is below {:.0}%. Merge anyway? [y/N] > ",
        threshold * 100.0
    );
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    Ok(line.trim().eq_ignore_ascii_case("y"))
}

pub fn show_merge_skipped() {
    println!("  Skipped.");
}

pub fn prompt_merge_conflict(
    rel: &Path,
    canon_path: &Path,
    canon_mtime: i64,
    source_path: &Path,
    source_mtime: i64,
) -> Result<merge::ConflictChoice> {
    println!("  Conflict: {}", rel.display());
    println!(
        "    [1] keep canon  {} ({})",
        canon_path.display(),
        utils::fmt_mtime(canon_mtime)
    );
    println!(
        "    [2] keep source {} ({})",
        source_path.display(),
        utils::fmt_mtime(source_mtime)
    );
    loop {
        print!("    [1] keep canon  [2] keep source  [s] skip > ");
        io::stdout().flush()?;
        let mut line = String::new();
        io::stdin().lock().read_line(&mut line)?;
        match line.trim().to_ascii_lowercase().as_str() {
            "1" => return Ok(merge::ConflictChoice::KeepCanon),
            "2" => return Ok(merge::ConflictChoice::KeepSource),
            "s" => return Ok(merge::ConflictChoice::Skip),
            _ => println!("    Please enter 1, 2, or s."),
        }
    }
}

pub fn show_merge_summary(moved: usize, deleted_dups: usize, skipped: usize) {
    println!(
        "  Done: {} file(s) moved/resolved, {} true duplicate(s) removed, {} skipped.",
        moved, deleted_dups, skipped
    );
}

// ---------------------------------------------------------------------------
// Similar directories (similar.rs)
// ---------------------------------------------------------------------------

pub fn show_loading_file_index() {
    println!("Loading file index into memory...");
}

pub fn show_finding_leaf_dirs() {
    println!("Finding leaf directories...");
}

pub fn show_leaf_dir_count(count: usize) {
    println!("Found {} leaf directories.", count);
}

pub fn show_checking_candidates(count: usize) {
    println!(
        "Checking {} candidate leaf pairs (same name, similar file count)...",
        count
    );
}

pub fn show_candidate_progress(checked: usize, total: usize, found: usize) {
    print!(
        "\r\x1B[K  {}/{} pairs checked, {} similar found",
        checked + 1,
        total,
        found
    );
    let _ = io::stdout().flush();
}

pub fn show_candidate_progress_end() {
    println!();
}

pub fn show_no_similar_pairs() {
    println!("No similar (but non-identical) directory pairs found.");
}

pub fn show_similar_pairs_header(count: usize, threshold: f64) {
    println!(
        "\nFound {} similar directory pair(s) (similarity >= {:.0}%):\n",
        count,
        threshold * 100.0
    );
}

pub fn show_similar_pair(pair: &similar::SimilarPair) {
    println!(
        "Similar directories ({:.1}% match, {} identical, {} conflict(s), {} only-in-A, {} only-in-B):",
        pair.score * 100.0,
        pair.identical,
        pair.conflicts.len(),
        pair.only_in_a.len(),
        pair.only_in_b.len(),
    );
    println!("  [A] {} ({} files)", pair.a.path, pair.a.file_count);
    println!("  [B] {} ({} files)", pair.b.path, pair.b.file_count);

    if !pair.only_in_a.is_empty() {
        println!("  Only in A ({}):", pair.only_in_a.len());
        for (rel, _) in pair.only_in_a.iter().take(10) {
            println!("    + {}", rel);
        }
        if pair.only_in_a.len() > 10 {
            println!("    ... and {} more", pair.only_in_a.len() - 10);
        }
    }

    if !pair.only_in_b.is_empty() {
        println!("  Only in B ({}):", pair.only_in_b.len());
        for (rel, _) in pair.only_in_b.iter().take(10) {
            println!("    + {}", rel);
        }
        if pair.only_in_b.len() > 10 {
            println!("    ... and {} more", pair.only_in_b.len() - 10);
        }
    }

    let newer_in_a = pair
        .conflicts
        .iter()
        .filter(|(_, _, _, ma, mb)| ma >= mb)
        .count();
    let newer_in_b = pair.conflicts.len() - newer_in_a;
    if !pair.conflicts.is_empty() {
        println!(
            "  Conflicts ({} total — {} newer in A, {} newer in B):",
            pair.conflicts.len(),
            newer_in_a,
            newer_in_b,
        );
        for (rel, _, _, mod_a, mod_b) in pair.conflicts.iter().take(5) {
            let (newer, date) = if mod_a >= mod_b {
                ("A", utils::fmt_mtime(*mod_a))
            } else {
                ("B", utils::fmt_mtime(*mod_b))
            };
            println!("    ~ {} (newer [{}]: {})", rel, newer, date);
        }
        if pair.conflicts.len() > 5 {
            println!("    ... and {} more", pair.conflicts.len() - 5);
        }
    }
}

pub fn show_similar_pair_merge_plan(pair: &similar::SimilarPair) {
    let newer_in_a = pair
        .conflicts
        .iter()
        .filter(|(_, _, _, ma, mb)| ma >= mb)
        .count();
    let newer_in_b = pair.conflicts.len() - newer_in_a;
    let files_to_copy = pair.only_in_a.len() + pair.only_in_b.len() + pair.conflicts.len();
    println!(
        "  Will copy up to {} file(s) to make both sides identical.",
        files_to_copy
    );
    println!(
        "    [A] {} ({} files, {} exclusive, {} newer in conflicts)",
        pair.a.path,
        pair.a.file_count,
        pair.only_in_a.len(),
        newer_in_a,
    );
    println!(
        "    [B] {} ({} files, {} exclusive, {} newer in conflicts)",
        pair.b.path,
        pair.b.file_count,
        pair.only_in_b.len(),
        newer_in_b,
    );
}

pub fn prompt_similar_resolution() -> Result<Option<similar::ConflictResolution>> {
    loop {
        print!("  Resolve conflicts with: [1] keep old  [2] keep new  [3] ask file by file  [s] skip > ");
        io::stdout().flush()?;
        let mut line = String::new();
        io::stdin().lock().read_line(&mut line)?;
        match line.trim().to_ascii_lowercase().as_str() {
            "1" => return Ok(Some(similar::ConflictResolution::KeepOld)),
            "2" => return Ok(Some(similar::ConflictResolution::KeepNew)),
            "3" => return Ok(Some(similar::ConflictResolution::AskPerFile)),
            "s" => return Ok(None),
            _ => println!("  Please enter 1, 2, 3, or s."),
        }
    }
}

pub fn show_similar_skipped() {
    println!("  Skipped.");
    println!();
}

pub fn show_similar_pair_end() {
    println!();
}

pub fn show_similar_copied_only_in_a(rel: &str) {
    println!("    Copied only-in-A into B: {}", rel);
}

pub fn show_similar_copied_only_in_b(rel: &str) {
    println!("    Copied only-in-B into A: {}", rel);
}

pub fn prompt_similar_file_conflict(
    fname: &str,
    side_old: &str,
    date_old: &str,
    side_new: &str,
    date_new: &str,
) -> Result<similar::FileConflictChoice> {
    println!(
        "    {} (old [{}]: {}  new [{}]: {})",
        fname, side_old, date_old, side_new, date_new
    );
    loop {
        print!("    [1] keep old  [2] keep new  [4] keep all old  [5] keep all new > ");
        io::stdout().flush()?;
        let mut line = String::new();
        io::stdin().lock().read_line(&mut line)?;
        match line.trim() {
            "1" => return Ok(similar::FileConflictChoice::KeepOld),
            "2" => return Ok(similar::FileConflictChoice::KeepNew),
            "4" => return Ok(similar::FileConflictChoice::KeepAllOld),
            "5" => return Ok(similar::FileConflictChoice::KeepAllNew),
            _ => println!("    Please enter 1, 2, 4, or 5."),
        }
    }
}

pub fn show_similar_kept_newer(rel: &str) {
    println!("    Kept newer: {}", rel);
}

pub fn show_similar_kept_older(rel: &str) {
    println!("    Kept older: {}", rel);
}

pub fn show_similar_merge_complete(copied: usize) {
    println!("  Merge complete: {} file(s) copied.", copied);
    println!("  Note: re-run without --similarity to detect exact duplicates and delete them.");
    println!();
}

// ---------------------------------------------------------------------------
// Photo sort (photos.rs)
// ---------------------------------------------------------------------------

pub fn show_sort_root_header(root: &Path) {
    println!("\nSorting photos in: {}", root.display());
}

pub fn show_sort_file_count(count: usize) {
    println!("  Found {} media file(s) to process.", count);
}

pub fn show_sort_duplicate(src: &Path) {
    println!("  Duplicate (same hash): removing {}", src.display());
}

pub fn show_sort_moved(src: &Path, dest: &Path) {
    println!("  Moved: {} -> {}", src.display(), dest.display());
}

pub fn show_sort_summary(moved: usize, skipped: usize, deleted_dups: usize) {
    println!(
        "  Done: {} moved, {} already sorted, {} true duplicates removed.",
        moved, skipped, deleted_dups
    );
}

pub fn run_sort_photos(conn: &Connection, directories: &[&Path], canon: &Path) -> Result<()> {
    for &root in directories {
        show_sort_root_header(root);
        let stats = photos::sort_root(conn, root, canon, &mut |event| match event {
            photos::SortEvent::FileCount(n) => show_sort_file_count(n),
            photos::SortEvent::Duplicate(src) => show_sort_duplicate(src),
            photos::SortEvent::Moved(src, dest) => show_sort_moved(src, dest),
        })?;
        show_sort_summary(stats.moved, stats.skipped, stats.deleted_dups);
    }
    Ok(())
}
