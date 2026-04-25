use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

use crate::{file_system, utils};

/// One side of a similar-directory pair, with its diff relative to the other side.
#[derive(Debug)]
struct DirSide {
    path: String,
    file_count: usize,
}

/// A pair of directories that are similar but not identical, along with the diff.
#[derive(Debug)]
struct SimilarPair {
    a: DirSide,
    b: DirSide,
    /// Relative paths present in both A and B with identical hashes — nothing to do.
    identical: usize,
    /// Relative paths present in both but with different hashes: (rel_path, hash_a, hash_b, modified_a, modified_b)
    conflicts: Vec<(String, String, String, i64, i64)>,
    /// Relative paths only in A: (rel_path, abs_path_in_a)
    only_in_a: Vec<(String, String)>,
    /// Relative paths only in B: (rel_path, abs_path_in_b)
    only_in_b: Vec<(String, String)>,
    /// Jaccard-like similarity score
    score: f64,
}

// ---------------------------------------------------------------------------
// In-memory index types
// ---------------------------------------------------------------------------

/// rel_path -> (abs_path, hash, modified)
pub type FileMap = HashMap<String, (String, String, i64)>;

/// dir_path -> FileMap  (only for directories under scanned roots)
pub type DirIndex = HashMap<String, FileMap>;

/// Load every file under the scanned roots into a nested map keyed first by
/// its immediate parent directory path, then by relative path within that dir.
/// One DB query replaces hundreds of per-pair LIKE queries.
pub fn build_dir_index(conn: &Connection, scanned_dirs: &[&Path]) -> Result<DirIndex> {
    let mut stmt = conn.prepare("SELECT path, hash, modified FROM files ORDER BY path")?;
    let rows: Vec<(String, String, i64)> = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;

    let mut index: DirIndex = HashMap::new();
    for (abs_path, hash, modified) in rows {
        // Filter to scanned roots if any are specified
        if !scanned_dirs.is_empty() {
            let p = Path::new(&abs_path);
            if !scanned_dirs.iter().any(|root| p.starts_with(root)) {
                continue;
            }
        }
        // Attribute the file to its immediate parent directory
        if let Some(parent) = Path::new(&abs_path).parent() {
            let dir_str = parent.to_string_lossy().to_string();
            let rel = Path::new(&abs_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            index
                .entry(dir_str)
                .or_default()
                .insert(rel, (abs_path, hash, modified));
        }
    }
    Ok(index)
}

/// Query the DB for all leaf directories under the given scanned roots.
/// A leaf directory is one that has no subdirectory entries in `directories`
/// whose path starts with `that_dir/`.
fn find_leaf_directories(conn: &Connection, scanned_dirs: &[&Path]) -> Result<Vec<String>> {
    let all_dirs: Vec<String> = {
        let mut stmt = conn.prepare("SELECT path FROM directories ORDER BY path")?;
        let rows: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        rows
    };

    // Filter to only those under a scanned root
    let under_scan: Vec<String> = all_dirs
        .into_iter()
        .filter(|p| {
            if scanned_dirs.is_empty() {
                return true;
            }
            scanned_dirs
                .iter()
                .any(|root| Path::new(p).starts_with(root))
        })
        .collect();

    // Build a set for O(1) child-prefix checks
    let path_set: HashSet<&str> = under_scan.iter().map(|p| p.as_str()).collect();

    // A directory is a leaf if no other path in the set starts with `dir/`
    let leaves: Vec<String> = under_scan
        .iter()
        .filter(|p| {
            let prefix = format!("{}/", p);
            !path_set.iter().any(|other| other.starts_with(&prefix))
        })
        .cloned()
        .collect();

    Ok(leaves)
}

/// Compare two directories using the preloaded in-memory index.
/// `dir_index` maps dir_path -> (rel_filename -> (abs, hash, modified)).
/// For a leaf this is just the immediate files; for a non-leaf (walked-up
/// ancestor) we need to include all files recursively — handled by
/// `files_for_dir` which aggregates across all children in the index.
pub fn files_for_dir<'a>(
    dir_index: &'a DirIndex,
    dir_path: &str,
) -> HashMap<String, &'a (String, String, i64)> {
    let prefix = format!("{}/", dir_path);
    let mut result: HashMap<String, &'a (String, String, i64)> = HashMap::new();
    for (indexed_dir, files) in dir_index {
        // Include the dir itself and all recursive children
        if indexed_dir == dir_path || indexed_dir.starts_with(&prefix) {
            let strip_len = dir_path.len() + 1; // +1 for the '/'
            for (rel, entry) in files {
                // Build full relative path from dir_path root
                let full_rel = if indexed_dir == dir_path {
                    rel.clone()
                } else {
                    format!("{}/{}", &indexed_dir[strip_len..], rel)
                };
                result.insert(full_rel, entry);
            }
        }
    }
    result
}

fn compare_dirs_mem<'a>(
    dir_index: &'a DirIndex,
    path_a: &str,
    path_b: &str,
) -> Option<SimilarPair> {
    let files_a = files_for_dir(dir_index, path_a);
    let files_b = files_for_dir(dir_index, path_b);

    if files_a.is_empty() && files_b.is_empty() {
        return None;
    }

    let keys_a: HashSet<&str> = files_a.keys().map(|s| s.as_str()).collect();
    let keys_b: HashSet<&str> = files_b.keys().map(|s| s.as_str()).collect();

    let shared_keys: HashSet<&str> = keys_a.intersection(&keys_b).copied().collect();
    let total_unique = keys_a.union(&keys_b).count();

    if total_unique == 0 {
        return None;
    }

    let mut identical = 0usize;
    let mut conflicts: Vec<(String, String, String, i64, i64)> = Vec::new();

    for rel in &shared_keys {
        let (_, hash_a, mod_a) = files_a[*rel];
        let (_, hash_b, mod_b) = files_b[*rel];
        if hash_a == hash_b {
            identical += 1;
        } else {
            conflicts.push((
                rel.to_string(),
                hash_a.clone(),
                hash_b.clone(),
                *mod_a,
                *mod_b,
            ));
        }
    }

    let only_in_a: Vec<(String, String)> = keys_a
        .difference(&keys_b)
        .map(|rel| {
            let (abs, _, _) = files_a[*rel];
            (rel.to_string(), abs.clone())
        })
        .collect();

    let only_in_b: Vec<(String, String)> = keys_b
        .difference(&keys_a)
        .map(|rel| {
            let (abs, _, _) = files_b[*rel];
            (rel.to_string(), abs.clone())
        })
        .collect();

    let score = shared_keys.len() as f64 / total_unique as f64;

    Some(SimilarPair {
        a: DirSide {
            path: path_a.to_string(),
            file_count: files_a.len(),
        },
        b: DirSide {
            path: path_b.to_string(),
            file_count: files_b.len(),
        },
        identical,
        conflicts,
        only_in_a,
        only_in_b,
        score,
    })
}

/// Walk up both paths simultaneously while the parents remain similar and are
/// within the scanned roots. Uses the preloaded sets/index — no DB queries.
fn walk_up_similar_mem(
    dir_index: &DirIndex,
    all_dir_paths: &HashSet<String>,
    mut path_a: String,
    mut path_b: String,
    threshold: f64,
    scanned_dirs: &[&Path],
) -> (String, String) {
    loop {
        let parent_a = match Path::new(&path_a).parent() {
            Some(p) => p.to_string_lossy().to_string(),
            None => break,
        };
        let parent_b = match Path::new(&path_b).parent() {
            Some(p) => p.to_string_lossy().to_string(),
            None => break,
        };

        // Stop if either current path is already a scanned root
        if !scanned_dirs.is_empty() {
            let a_is_root = scanned_dirs.iter().any(|r| Path::new(&path_a) == *r);
            let b_is_root = scanned_dirs.iter().any(|r| Path::new(&path_b) == *r);
            if a_is_root || b_is_root {
                break;
            }
            // Also stop if the parent would be above a scanned root
            let a_parent_above = scanned_dirs
                .iter()
                .any(|r| Path::new(&path_a).starts_with(r) && !Path::new(&parent_a).starts_with(r));
            let b_parent_above = scanned_dirs
                .iter()
                .any(|r| Path::new(&path_b).starts_with(r) && !Path::new(&parent_b).starts_with(r));
            if a_parent_above || b_parent_above {
                break;
            }
        }

        // Parents must be known directories
        if !all_dir_paths.contains(&parent_a) || !all_dir_paths.contains(&parent_b) {
            break;
        }

        match compare_dirs_mem(dir_index, &parent_a, &parent_b) {
            Some(pair) if pair.score >= threshold => {
                path_a = parent_a;
                path_b = parent_b;
            }
            _ => break,
        }
    }

    (path_a, path_b)
}

enum ConflictResolution {
    KeepOld,
    KeepNew,
    AskPerFile,
}

/// Perform the merge: copy files that are only in one side to the other side,
/// and for conflicts apply the given resolution strategy.
/// Returns the number of files copied.
fn merge_into(
    pair: &SimilarPair,
    resolution: ConflictResolution,
    stdin: &io::Stdin,
) -> Result<usize> {
    let mut copied = 0;

    // Files only in A → copy into B
    for (rel, src_abs) in &pair.only_in_a {
        let dst_abs = format!("{}/{}", pair.b.path, rel);
        file_system::copy_file(Path::new(src_abs), Path::new(&dst_abs))?;
        println!("    Copied only-in-A into B: {}", rel);
        copied += 1;
    }

    // Files only in B → copy into A
    for (rel, src_abs) in &pair.only_in_b {
        let dst_abs = format!("{}/{}", pair.a.path, rel);
        file_system::copy_file(Path::new(src_abs), Path::new(&dst_abs))?;
        println!("    Copied only-in-B into A: {}", rel);
        copied += 1;
    }

    // Conflicts: apply resolution strategy
    // blanket overrides per-file choice when user picks [4] or [5]
    let mut blanket: Option<ConflictResolution> = None;

    for (rel, _hash_a, _hash_b, mod_a, mod_b) in &pair.conflicts {
        let path_a = format!("{}/{}", pair.a.path, rel);
        let path_b = format!("{}/{}", pair.b.path, rel);

        // Determine which path is older/newer
        let (older_path, newer_path) = if mod_a <= mod_b {
            (&path_a, &path_b)
        } else {
            (&path_b, &path_a)
        };

        let keep_newer = match &blanket {
            Some(ConflictResolution::KeepOld) => false,
            Some(ConflictResolution::KeepNew) => true,
            Some(ConflictResolution::AskPerFile) => unreachable!(),
            None => {
                match &resolution {
                    ConflictResolution::KeepOld => false,
                    ConflictResolution::KeepNew => true,
                    ConflictResolution::AskPerFile => {
                        // Show file info and prompt
                        let fname = Path::new(rel)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(rel.as_str());
                        let (date_old, date_new, side_old, side_new) = if mod_a <= mod_b {
                            (utils::fmt_mtime(*mod_a), utils::fmt_mtime(*mod_b), "A", "B")
                        } else {
                            (utils::fmt_mtime(*mod_b), utils::fmt_mtime(*mod_a), "B", "A")
                        };
                        println!(
                            "    {} (old [{}]: {}  new [{}]: {})",
                            fname, side_old, date_old, side_new, date_new
                        );
                        loop {
                            print!("    [1] keep old  [2] keep new  [4] keep all old  [5] keep all new > ");
                            io::stdout().flush()?;
                            let mut line = String::new();
                            stdin.lock().read_line(&mut line)?;
                            match line.trim() {
                                "1" => break false,
                                "2" => break true,
                                "4" => {
                                    blanket = Some(ConflictResolution::KeepOld);
                                    break false;
                                }
                                "5" => {
                                    blanket = Some(ConflictResolution::KeepNew);
                                    break true;
                                }
                                _ => println!("    Please enter 1, 2, 4, or 5."),
                            }
                        }
                    }
                }
            }
        };

        // Copy winning version to the losing side
        if keep_newer {
            file_system::copy_file(Path::new(newer_path), Path::new(older_path))?;
            println!("    Kept newer: {}", rel);
        } else {
            file_system::copy_file(Path::new(older_path), Path::new(newer_path))?;
            println!("    Kept older: {}", rel);
        }
        copied += 1;
    }

    Ok(copied)
}

/// Pure computation: find all similar-but-not-identical directory pairs at or
/// above `threshold`, sorted by score descending. No I/O or prompting.
fn compute_similar_pairs(
    conn: &Connection,
    threshold: f64,
    scanned_dirs: &[&Path],
) -> Result<Vec<SimilarPair>> {
    // ------------------------------------------------------------------
    // Phase 1: load everything into memory (2 queries total)
    // ------------------------------------------------------------------
    println!("Loading file index into memory...");
    let dir_index = build_dir_index(conn, scanned_dirs)?;

    // Set of all known directory paths (for walk-up parent checks)
    let all_dir_paths: HashSet<String> = {
        let mut stmt = conn.prepare("SELECT path FROM directories")?;
        let paths: HashSet<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<HashSet<String>>>()?;
        paths
    };

    // ------------------------------------------------------------------
    // Phase 2: find leaf directories
    // ------------------------------------------------------------------
    println!("Finding leaf directories...");
    let leaves = find_leaf_directories(conn, scanned_dirs)?;
    println!("Found {} leaf directories.", leaves.len());

    // ------------------------------------------------------------------
    // Phase 3: build candidate pairs (same name, file count within 20%)
    // ------------------------------------------------------------------
    let mut by_name: HashMap<String, Vec<String>> = HashMap::new();
    for path in &leaves {
        if let Some(name) = Path::new(path).file_name().and_then(|n| n.to_str()) {
            by_name
                .entry(name.to_string())
                .or_default()
                .push(path.clone());
        }
    }

    // File count for each leaf is just the size of its entry in dir_index
    // (leaves have no children, so this equals their total recursive count too)
    let mut candidate_pairs: Vec<(String, String)> = Vec::new();
    for (_name, paths) in &by_name {
        if paths.len() < 2 {
            continue;
        }
        for i in 0..paths.len() {
            for j in (i + 1)..paths.len() {
                let count_i = dir_index.get(&paths[i]).map(|m| m.len()).unwrap_or(0);
                let count_j = dir_index.get(&paths[j]).map(|m| m.len()).unwrap_or(0);
                if count_i == 0 && count_j == 0 {
                    continue;
                }
                let max_c = count_i.max(count_j) as f64;
                let min_c = count_i.min(count_j) as f64;
                if min_c / max_c >= 0.80 {
                    candidate_pairs.push((paths[i].clone(), paths[j].clone()));
                }
            }
        }
    }

    let total_candidates = candidate_pairs.len();
    println!(
        "Checking {} candidate leaf pairs (same name, similar file count)...",
        total_candidates
    );

    // ------------------------------------------------------------------
    // Phase 4: score pairs, walk up, deduplicate
    // ------------------------------------------------------------------
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    // Track leaves that have been absorbed into a higher-level ancestor pair
    let mut absorbed_leaves: HashSet<String> = HashSet::new();
    let mut similar_pairs: Vec<SimilarPair> = Vec::new();

    for (checked, (leaf_a, leaf_b)) in candidate_pairs.iter().enumerate() {
        // Progress indicator
        if checked % 50 == 0 || checked + 1 == total_candidates {
            print!(
                "\r\x1B[K  {}/{} pairs checked, {} similar found",
                checked + 1,
                total_candidates,
                similar_pairs.len()
            );
            io::stdout().flush()?;
        }

        // Skip if both leaves are already absorbed by an ancestor result
        if absorbed_leaves.contains(leaf_a) && absorbed_leaves.contains(leaf_b) {
            continue;
        }

        let pair = match compare_dirs_mem(&dir_index, leaf_a, leaf_b) {
            Some(p) if p.score >= threshold => p,
            _ => continue,
        };

        // Walk up to find the highest similar ancestor
        let (top_a, top_b) = walk_up_similar_mem(
            &dir_index,
            &all_dir_paths,
            leaf_a.clone(),
            leaf_b.clone(),
            threshold,
            scanned_dirs,
        );

        // Normalise pair key
        let key = if top_a <= top_b {
            (top_a.clone(), top_b.clone())
        } else {
            (top_b.clone(), top_a.clone())
        };
        if seen_pairs.contains(&key) {
            // Still absorb these leaves even though the pair is already recorded
            absorbed_leaves.insert(leaf_a.clone());
            absorbed_leaves.insert(leaf_b.clone());
            continue;
        }
        seen_pairs.insert(key);

        // Mark both leaves (and any leaves under the top-level ancestors) as absorbed
        absorbed_leaves.insert(leaf_a.clone());
        absorbed_leaves.insert(leaf_b.clone());

        // Re-compare at the top level
        let top_pair = if top_a == *leaf_a && top_b == *leaf_b {
            pair // didn't walk up, reuse
        } else {
            match compare_dirs_mem(&dir_index, &top_a, &top_b) {
                Some(p) if p.score >= threshold => p,
                _ => continue,
            }
        };

        // Skip exact duplicates (score 1.0, no conflicts, nothing only in one side) —
        // they produce a no-op merge and are already handled by duplicate detection.
        if top_pair.only_in_a.is_empty()
            && top_pair.only_in_b.is_empty()
            && top_pair.conflicts.is_empty()
        {
            continue;
        }

        similar_pairs.push(top_pair);
    }
    println!(); // end progress line

    // Sort by score descending
    similar_pairs.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

    Ok(similar_pairs)
}

pub fn find_similar_directories(
    conn: &Connection,
    threshold: f64,
    scanned_dirs: &[&Path],
    merge: bool,
) -> Result<()> {
    let similar_pairs = compute_similar_pairs(conn, threshold, scanned_dirs)?;

    if similar_pairs.is_empty() {
        println!("No similar (but non-identical) directory pairs found.");
        return Ok(());
    }

    println!(
        "\nFound {} similar directory pair(s) (similarity >= {:.0}%):\n",
        similar_pairs.len(),
        threshold * 100.0
    );

    let stdin = io::stdin();

    for pair in &similar_pairs {
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

        if !merge {
            println!();
            continue;
        }

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

        let resolution_opt: Option<ConflictResolution> = loop {
            print!("  Resolve conflicts with: [1] keep old  [2] keep new  [3] ask file by file  [s] skip > ");
            io::stdout().flush()?;
            let mut line = String::new();
            stdin.lock().read_line(&mut line)?;
            match line.trim().to_ascii_lowercase().as_str() {
                "1" => break Some(ConflictResolution::KeepOld),
                "2" => break Some(ConflictResolution::KeepNew),
                "3" => break Some(ConflictResolution::AskPerFile),
                "s" => break None,
                _ => println!("  Please enter 1, 2, 3, or s."),
            }
        };

        let resolution = match resolution_opt {
            None => {
                println!("  Skipped.");
                println!();
                continue;
            }
            Some(r) => r,
        };

        let copied = merge_into(pair, resolution, &stdin)?;
        println!("  Merge complete: {} file(s) copied.", copied);
        println!("  Note: re-run without --similarity to detect exact duplicates and delete them.");
        println!();
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
    use crate::db::setup_schema;
    use rusqlite::Connection;

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        setup_schema(&conn).unwrap();
        conn
    }

    /// Insert the two standard test directories and `shared_count` files that
    /// exist (with the same hash) in both `/a/photos` and `/b/photos`.
    /// Files are named `img1.jpg` … `img{shared_count}.jpg`.
    /// Returns the connection so callers can add extra rows to create the
    /// specific scenario they are testing.
    fn setup_two_photo_dirs(conn: &Connection, shared_count: usize) {
        let mut batch = String::from(
            "INSERT INTO directories VALUES ('/a/photos', 'hashA', 5000);
             INSERT INTO directories VALUES ('/b/photos', 'hashB', 5000);",
        );
        for i in 1..=shared_count {
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/a/photos/img{}.jpg', 'fh{}', 500, 1000);",
                i, i
            ));
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/b/photos/img{}.jpg', 'fh{}', 500, 1000);",
                i, i
            ));
        }
        conn.execute_batch(&batch).unwrap();
    }

    #[test]
    fn test_find_similar_empty_db() {
        // Smoke test: empty database should return Ok(()) without panicking.
        let conn = open_test_db();
        let pairs = compute_similar_pairs(&conn, 0.9, &[]).unwrap();
        assert!(pairs.is_empty());
    }

    #[test]
    fn test_find_similar_identical_leaves_not_reported() {
        // Base: 1 shared file → both dirs are identical; should not be reported.
        let conn = open_test_db();
        setup_two_photo_dirs(&conn, 1);
        let pairs = compute_similar_pairs(&conn, 0.9, &[]).unwrap();
        assert!(
            pairs.is_empty(),
            "identical dirs should not be reported as similar"
        );
    }

    #[test]
    fn test_find_similar_detects_near_duplicate() {
        // Base: 9 shared files, then add 1 file only in A.
        // Score = 9/10 = 0.90 ≥ threshold 0.85 → should be reported.
        let conn = open_test_db();
        setup_two_photo_dirs(&conn, 9);
        conn.execute_batch(
            "INSERT INTO files VALUES ('/a/photos/Thumbs.db', 'thumbhash', 10, 1000);",
        )
        .unwrap();
        let pairs = compute_similar_pairs(&conn, 0.85, &[]).unwrap();
        assert_eq!(pairs.len(), 1, "expected exactly one similar pair");
        assert!(pairs[0].score >= 0.85, "score should meet threshold");
        assert_eq!(pairs[0].only_in_a.len(), 1, "Thumbs.db should be only-in-A");
        assert!(pairs[0].only_in_b.is_empty());
        assert!(pairs[0].conflicts.is_empty());
    }

    #[test]
    fn test_build_dir_index_groups_by_parent() {
        let conn = open_test_db();
        conn.execute_batch(
            "INSERT INTO directories VALUES ('/a/photos', 'dh1', 5000);
             INSERT INTO files VALUES ('/a/photos/img1.jpg', 'fh1', 100, 1000);
             INSERT INTO files VALUES ('/a/photos/img2.jpg', 'fh2', 200, 2000);",
        )
        .unwrap();
        let index = build_dir_index(&conn, &[]).unwrap();
        let dir = index.get("/a/photos").expect("dir should be indexed");
        assert_eq!(dir.len(), 2);
        let (abs, hash, mtime) = &dir["img1.jpg"];
        assert_eq!(abs, "/a/photos/img1.jpg");
        assert_eq!(hash, "fh1");
        assert_eq!(*mtime, 1000);
    }

    #[test]
    fn test_build_dir_index_filters_to_scanned_roots() {
        let conn = open_test_db();
        conn.execute_batch(
            "INSERT INTO directories VALUES ('/a/photos', 'dh1', 5000);
             INSERT INTO directories VALUES ('/b/photos', 'dh2', 5000);
             INSERT INTO files VALUES ('/a/photos/img1.jpg', 'fh1', 100, 1000);
             INSERT INTO files VALUES ('/b/photos/img1.jpg', 'fh2', 100, 1000);",
        )
        .unwrap();
        let root = std::path::Path::new("/a");
        let index = build_dir_index(&conn, &[root]).unwrap();
        assert!(index.contains_key("/a/photos"), "/a/photos should be indexed");
        assert!(!index.contains_key("/b/photos"), "/b/photos should be filtered out");
    }

    #[test]
    fn test_files_for_dir_includes_subdirs_recursively() {
        let conn = open_test_db();
        conn.execute_batch(
            "INSERT INTO directories VALUES ('/a', 'dh0', 5000);
             INSERT INTO directories VALUES ('/a/sub', 'dh1', 5000);
             INSERT INTO files VALUES ('/a/root.txt', 'fh0', 50, 1000);
             INSERT INTO files VALUES ('/a/sub/child.txt', 'fh1', 50, 1000);",
        )
        .unwrap();
        let index = build_dir_index(&conn, &[]).unwrap();
        let files = files_for_dir(&index, "/a");
        assert!(files.contains_key("root.txt"), "root-level file should be included");
        assert!(files.contains_key("sub/child.txt"), "subdirectory file should be included with relative path");
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_find_similar_detects_conflict() {
        // 9 shared identical files + 1 file with same name but different hash → conflict.
        // Score = 10/10 = 1.0 but there IS a conflict, so it should be reported.
        let conn = open_test_db();
        setup_two_photo_dirs(&conn, 9);
        conn.execute_batch(
            "INSERT INTO files VALUES ('/a/photos/conflict.jpg', 'hashA_conflict', 500, 1000);
             INSERT INTO files VALUES ('/b/photos/conflict.jpg', 'hashB_conflict', 500, 2000);",
        )
        .unwrap();
        let pairs = compute_similar_pairs(&conn, 0.85, &[]).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].conflicts.len(), 1, "expected one conflict");
        let (rel, hash_a, hash_b, mod_a, mod_b) = &pairs[0].conflicts[0];
        assert_eq!(rel, "conflict.jpg");
        assert_eq!(hash_a, "hashA_conflict");
        assert_eq!(hash_b, "hashB_conflict");
        assert_eq!(*mod_a, 1000);
        assert_eq!(*mod_b, 2000);
    }

    #[test]
    fn test_find_similar_below_threshold_not_reported() {
        // Base: 1 shared file, then add 9 files unique to each side.
        // Score = 1/19 ≈ 0.05 < threshold 0.9 → should not be reported.
        let conn = open_test_db();
        setup_two_photo_dirs(&conn, 1);
        let mut batch = String::new();
        for i in 1..=9 {
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/a/photos/only_a{}.jpg', 'fhA{}', 500, 1000);",
                i, i
            ));
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/b/photos/only_b{}.jpg', 'fhB{}', 500, 1000);",
                i, i
            ));
        }
        conn.execute_batch(&batch).unwrap();
        let pairs = compute_similar_pairs(&conn, 0.9, &[]).unwrap();
        assert!(
            pairs.is_empty(),
            "dirs below threshold should not be reported"
        );
    }
}
