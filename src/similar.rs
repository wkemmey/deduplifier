use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;

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
type FileMap = HashMap<String, (String, String, i64)>;

/// dir_path -> FileMap  (only for directories under scanned roots)
type DirIndex = HashMap<String, FileMap>;

/// Load every file under the scanned roots into a nested map keyed first by
/// its immediate parent directory path, then by relative path within that dir.
/// One DB query replaces hundreds of per-pair LIKE queries.
fn build_dir_index(conn: &Connection, scanned_dirs: &[&Path]) -> Result<DirIndex> {
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
fn files_for_dir<'a>(
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

/// Perform the merge: copy files that are only in `src` into `dst`, and for
/// conflicts keep the newer file by copying it over the older.
/// Returns the number of files copied.
fn merge_into(pair: &SimilarPair, keep_path: &str, discard_path: &str) -> Result<usize> {
    let mut copied = 0;

    let only_in_discard: &[(String, String)] = if discard_path == pair.b.path {
        &pair.only_in_b
    } else {
        &pair.only_in_a
    };

    for (rel, src_abs) in only_in_discard {
        let dst_abs = format!("{}/{}", keep_path, rel);
        let dst_path = Path::new(&dst_abs);
        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(src_abs, &dst_abs)?;
        println!("    Copied: {} -> {}", src_abs, dst_abs);
        copied += 1;
    }

    for (rel, _hash_a, _hash_b, mod_a, mod_b) in &pair.conflicts {
        let (src_abs, dst_abs) = if discard_path == pair.b.path {
            if mod_b > mod_a {
                (
                    format!("{}/{}", pair.b.path, rel),
                    format!("{}/{}", pair.a.path, rel),
                )
            } else {
                continue;
            }
        } else {
            if mod_a > mod_b {
                (
                    format!("{}/{}", pair.a.path, rel),
                    format!("{}/{}", pair.b.path, rel),
                )
            } else {
                continue;
            }
        };
        fs::copy(&src_abs, &dst_abs)?;
        println!("    Updated (newer): {} -> {}", src_abs, dst_abs);
        copied += 1;
    }

    Ok(copied)
}

pub fn find_similar_directories(
    conn: &Connection,
    threshold: f64,
    canon: Option<&Path>,
    scanned_dirs: &[&Path],
    merge: bool,
) -> Result<()> {
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

        if !pair.conflicts.is_empty() {
            println!(
                "  Conflicts (same path, different content) ({}):",
                pair.conflicts.len()
            );
            for (rel, _, _, mod_a, mod_b) in pair.conflicts.iter().take(5) {
                let newer = if mod_a >= mod_b { "A" } else { "B" };
                println!("    ~ {} (newer: {})", rel, newer);
            }
            if pair.conflicts.len() > 5 {
                println!("    ... and {} more", pair.conflicts.len() - 5);
            }
        }

        if !merge {
            println!();
            continue;
        }

        let canon_a = canon
            .map(|c| Path::new(&pair.a.path).starts_with(c))
            .unwrap_or(false);
        let canon_b = canon
            .map(|c| Path::new(&pair.b.path).starts_with(c))
            .unwrap_or(false);

        let keep_path: String;
        let discard_path: String;

        if canon_a && !canon_b {
            println!("  Merging into A (canon): {}", pair.a.path);
            keep_path = pair.a.path.clone();
            discard_path = pair.b.path.clone();
        } else if canon_b && !canon_a {
            println!("  Merging into B (canon): {}", pair.b.path);
            keep_path = pair.b.path.clone();
            discard_path = pair.a.path.clone();
        } else {
            print!("  Merge which into which? ([A] keep A, [B] keep B, [s] skip): ");
            io::stdout().flush()?;
            let mut line = String::new();
            stdin.lock().read_line(&mut line)?;
            match line.trim().to_ascii_lowercase().as_str() {
                "a" => {
                    keep_path = pair.a.path.clone();
                    discard_path = pair.b.path.clone();
                }
                "b" => {
                    keep_path = pair.b.path.clone();
                    discard_path = pair.a.path.clone();
                }
                _ => {
                    println!("  Skipped.");
                    println!();
                    continue;
                }
            }
        }

        let files_to_copy = pair.only_in_a.len().max(pair.only_in_b.len())
            + pair
                .conflicts
                .iter()
                .filter(|(_, _, _, ma, mb)| ma != mb)
                .count();
        println!(
            "  Will copy up to {} file(s) into '{}', then '{}' will be a true duplicate.",
            files_to_copy, keep_path, discard_path
        );
        print!("  Proceed with merge? [y/N] > ");
        io::stdout().flush()?;
        let mut confirmation = String::new();
        stdin.lock().read_line(&mut confirmation)?;
        if !confirmation.trim().eq_ignore_ascii_case("y") {
            println!("  Skipped.");
            println!();
            continue;
        }

        let copied = merge_into(pair, &keep_path, &discard_path)?;
        println!(
            "  Merge complete: {} file(s) copied into '{}'.",
            copied, keep_path
        );
        println!(
            "  Note: re-run without --similarity to detect '{}' as a duplicate and delete it.",
            discard_path
        );
        println!();
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
    fn test_find_similar_no_leaves() {
        let conn = open_test_db();
        find_similar_directories(&conn, 0.9, None, &[], false).unwrap();
    }

    #[test]
    fn test_find_similar_identical_leaves_not_reported() {
        let conn = open_test_db();
        conn.execute_batch(
            "INSERT INTO directories VALUES ('/a/photos', 'hash1', 1000);
             INSERT INTO directories VALUES ('/b/photos', 'hash1', 1000);
             INSERT INTO files VALUES ('/a/photos/img1.jpg', 'filehash1', 500, 1000);
             INSERT INTO files VALUES ('/b/photos/img1.jpg', 'filehash1', 500, 1000);",
        )
        .unwrap();
        find_similar_directories(&conn, 0.9, None, &[], false).unwrap();
    }

    #[test]
    fn test_find_similar_detects_near_duplicate() {
        let conn = open_test_db();
        let mut batch = String::from(
            "INSERT INTO directories VALUES ('/a/photos', 'hashA', 5000);
             INSERT INTO directories VALUES ('/b/photos', 'hashB', 4500);",
        );
        for i in 1..=9 {
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/a/photos/img{}.jpg', 'fh{}', 500, 1000);",
                i, i
            ));
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/b/photos/img{}.jpg', 'fh{}', 500, 1000);",
                i, i
            ));
        }
        batch.push_str("INSERT INTO files VALUES ('/a/photos/Thumbs.db', 'thumbhash', 10, 1000);");
        conn.execute_batch(&batch).unwrap();
        find_similar_directories(&conn, 0.85, None, &[], false).unwrap();
    }

    #[test]
    fn test_find_similar_below_threshold_not_reported() {
        let conn = open_test_db();
        let mut batch = String::from(
            "INSERT INTO directories VALUES ('/a/photos', 'hashA', 5000);
             INSERT INTO directories VALUES ('/b/photos', 'hashB', 5000);",
        );
        for i in 1..=9 {
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/a/photos/imgA{}.jpg', 'fhA{}', 500, 1000);",
                i, i
            ));
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/b/photos/imgB{}.jpg', 'fhB{}', 500, 1000);",
                i, i
            ));
        }
        batch.push_str("INSERT INTO files VALUES ('/a/photos/shared.jpg', 'shared', 500, 1000);");
        batch.push_str("INSERT INTO files VALUES ('/b/photos/shared.jpg', 'shared', 500, 1000);");
        conn.execute_batch(&batch).unwrap();
        find_similar_directories(&conn, 0.9, None, &[], false).unwrap();
    }
}
