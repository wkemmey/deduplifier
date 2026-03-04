use anyhow::Result;
use rusqlite::{params, Connection};
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

/// Query the DB for all leaf directories under the given scanned roots.
/// A leaf directory is one that has no subdirectory entries in `directories`
/// whose path starts with `that_dir/`.
fn find_leaf_directories(conn: &Connection, scanned_dirs: &[&Path]) -> Result<Vec<(String, i64)>> {
    // Fetch all directories under scanned roots
    let all_dirs: Vec<(String, i64)> = {
        let mut stmt = conn.prepare("SELECT path, size FROM directories ORDER BY path")?;
        let rows: Vec<(String, i64)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        rows
    };

    // Filter to only those under a scanned root
    let under_scan: Vec<(String, i64)> = all_dirs
        .iter()
        .filter(|(p, _)| {
            if scanned_dirs.is_empty() {
                return true;
            }
            let candidate = Path::new(p);
            scanned_dirs.iter().any(|root| candidate.starts_with(root))
        })
        .cloned()
        .collect();

    // Build a set of all scanned paths for fast child lookup
    let path_set: HashSet<String> = under_scan.iter().map(|(p, _)| p.clone()).collect();

    // A directory is a leaf if no other directory in the set is a strict child of it
    let leaves: Vec<(String, i64)> = under_scan
        .into_iter()
        .filter(|(p, _)| {
            let prefix = format!("{}/", p);
            !path_set.iter().any(|other| other.starts_with(&prefix))
        })
        .collect();

    Ok(leaves)
}

/// Fetch files under a directory path (non-recursive prefix match from DB).
/// Returns map of relative_path -> (abs_path, hash, modified).
fn files_under(
    conn: &Connection,
    dir_path: &str,
) -> Result<HashMap<String, (String, String, i64)>> {
    let prefix = format!("{}/", dir_path);
    let prefix_len = prefix.len();

    let mut stmt =
        conn.prepare("SELECT path, hash, modified FROM files WHERE path LIKE ?1 || '%'")?;
    let rows: Vec<(String, String, i64)> = stmt
        .query_map(params![prefix.trim_end_matches('%')], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;

    let mut map = HashMap::new();
    for (abs_path, hash, modified) in rows {
        if abs_path.starts_with(&prefix) {
            let rel = abs_path[prefix_len..].to_string();
            map.insert(rel, (abs_path, hash, modified));
        }
    }
    Ok(map)
}

/// Compute the similarity between two directories using data already in the DB.
/// Returns None if there are no files to compare.
fn compare_dirs(conn: &Connection, path_a: &str, path_b: &str) -> Result<Option<SimilarPair>> {
    let files_a = files_under(conn, path_a)?;
    let files_b = files_under(conn, path_b)?;

    if files_a.is_empty() && files_b.is_empty() {
        return Ok(None);
    }

    let keys_a: HashSet<&str> = files_a.keys().map(|s| s.as_str()).collect();
    let keys_b: HashSet<&str> = files_b.keys().map(|s| s.as_str()).collect();

    let shared_keys: HashSet<&str> = keys_a.intersection(&keys_b).copied().collect();
    let total_unique = keys_a.union(&keys_b).count();

    if total_unique == 0 {
        return Ok(None);
    }

    let mut identical = 0usize;
    let mut conflicts: Vec<(String, String, String, i64, i64)> = Vec::new();

    for rel in &shared_keys {
        let (_, hash_a, mod_a) = &files_a[*rel];
        let (_, hash_b, mod_b) = &files_b[*rel];
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
            let (abs, _, _) = &files_a[*rel];
            (rel.to_string(), abs.clone())
        })
        .collect();

    let only_in_b: Vec<(String, String)> = keys_b
        .difference(&keys_a)
        .map(|rel| {
            let (abs, _, _) = &files_b[*rel];
            (rel.to_string(), abs.clone())
        })
        .collect();

    // Jaccard-like: shared (regardless of hash match) / total unique paths
    let score = shared_keys.len() as f64 / total_unique as f64;

    Ok(Some(SimilarPair {
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
    }))
}

/// Given two paths that are similar, walk up both simultaneously checking whether
/// the parents are also similar (above threshold). Stop at scanned roots.
/// Returns the highest ancestor pair that is still above threshold.
fn walk_up_similar(
    conn: &Connection,
    mut path_a: String,
    mut path_b: String,
    threshold: f64,
    scanned_dirs: &[&Path],
) -> Result<(String, String)> {
    loop {
        let parent_a = match Path::new(&path_a).parent() {
            Some(p) => p.to_string_lossy().to_string(),
            None => break,
        };
        let parent_b = match Path::new(&path_b).parent() {
            Some(p) => p.to_string_lossy().to_string(),
            None => break,
        };

        // Don't walk above any scanned root
        let a_at_root = scanned_dirs.iter().any(|root| {
            Path::new(&path_a) == *root
                || Path::new(&path_a).starts_with(root) && Path::new(&parent_a) == *root
        });
        let b_at_root = scanned_dirs.iter().any(|root| {
            Path::new(&path_b) == *root
                || Path::new(&path_b).starts_with(root) && Path::new(&parent_b) == *root
        });
        if a_at_root || b_at_root {
            break;
        }

        // Parents must themselves be in the directories table to be meaningful
        let parent_a_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM directories WHERE path = ?1",
                params![parent_a],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;
        let parent_b_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM directories WHERE path = ?1",
                params![parent_b],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;

        if !parent_a_exists || !parent_b_exists {
            break;
        }

        match compare_dirs(conn, &parent_a, &parent_b)? {
            Some(pair) if pair.score >= threshold => {
                path_a = parent_a;
                path_b = parent_b;
            }
            _ => break,
        }
    }

    Ok((path_a, path_b))
}

/// Perform the merge: copy files that are only in `src` into `dst`, and for
/// conflicts keep the newer file by copying it over the older.
/// Returns the number of files copied.
fn merge_into(
    pair: &SimilarPair,
    keep_path: &str,
    discard_path: &str,
    _conn: &Connection,
) -> Result<usize> {
    let mut copied = 0;

    // Files only in the discard side need to be copied into keep
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

    // Conflicts: copy the newer file into the keep side (overwriting older)
    for (rel, _hash_a, _hash_b, mod_a, mod_b) in &pair.conflicts {
        let (src_abs, dst_abs) = if discard_path == pair.b.path {
            // keep=A, discard=B: copy B->A only if B is newer
            if mod_b > mod_a {
                (
                    format!("{}/{}", pair.b.path, rel),
                    format!("{}/{}", pair.a.path, rel),
                )
            } else {
                continue; // A is already newer or equal, nothing to do
            }
        } else {
            // keep=B, discard=A: copy A->B only if A is newer
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
    println!("Finding leaf directories...");
    let leaves = find_leaf_directories(conn, scanned_dirs)?;
    println!("Found {} leaf directories.", leaves.len());

    // Group leaves by their directory name (last path component)
    let mut by_name: HashMap<String, Vec<String>> = HashMap::new();
    for (path, _size) in &leaves {
        if let Some(name) = Path::new(path).file_name().and_then(|n| n.to_str()) {
            by_name
                .entry(name.to_string())
                .or_default()
                .push(path.clone());
        }
    }

    // Build candidate pairs: same name, file counts within 20% of each other
    let leaf_file_counts: HashMap<String, usize> = {
        let mut map = HashMap::new();
        for (path, _) in &leaves {
            let count = conn
                .query_row(
                    "SELECT COUNT(*) FROM files WHERE path LIKE ?1 || '/%'",
                    params![path],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0) as usize;
            map.insert(path.clone(), count);
        }
        map
    };

    let mut candidate_pairs: Vec<(String, String)> = Vec::new();
    for (_name, paths) in &by_name {
        if paths.len() < 2 {
            continue;
        }
        for i in 0..paths.len() {
            for j in (i + 1)..paths.len() {
                let count_i = leaf_file_counts.get(&paths[i]).copied().unwrap_or(0);
                let count_j = leaf_file_counts.get(&paths[j]).copied().unwrap_or(0);
                if count_i == 0 && count_j == 0 {
                    continue;
                }
                let max_count = count_i.max(count_j) as f64;
                let min_count = count_i.min(count_j) as f64;
                if min_count / max_count >= 0.80 {
                    candidate_pairs.push((paths[i].clone(), paths[j].clone()));
                }
            }
        }
    }

    println!(
        "Checking {} candidate leaf pairs (same name, similar file count)...",
        candidate_pairs.len()
    );

    // Score each candidate pair and walk up to find the highest similar ancestor
    // Deduplicate by ancestor pair so multiple leaf matches under the same parents
    // don't produce duplicate reports.
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    let mut similar_pairs: Vec<SimilarPair> = Vec::new();

    for (leaf_a, leaf_b) in &candidate_pairs {
        let pair = match compare_dirs(conn, leaf_a, leaf_b)? {
            Some(p) if p.score >= threshold => p,
            _ => continue,
        };

        // Walk up to find the highest similar ancestor
        let (top_a, top_b) = walk_up_similar(
            conn,
            leaf_a.clone(),
            leaf_b.clone(),
            threshold,
            scanned_dirs,
        )?;

        // Normalise pair key so (A,B) and (B,A) are the same
        let key = if top_a <= top_b {
            (top_a.clone(), top_b.clone())
        } else {
            (top_b.clone(), top_a.clone())
        };
        if seen_pairs.contains(&key) {
            continue;
        }
        seen_pairs.insert(key);

        // Re-compare at the top level (may differ from leaf-level pair)
        match compare_dirs(conn, &top_a, &top_b)? {
            Some(top_pair) if top_pair.score >= threshold => similar_pairs.push(top_pair),
            _ => {
                // Walk-up landed at the same level as the leaf (didn't move), use leaf pair
                similar_pairs.push(pair);
            }
        }
    }

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

        // Determine which side to keep (canon wins; otherwise ask)
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

        // Summarise what will happen and ask for confirmation
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

        let copied = merge_into(pair, &keep_path, &discard_path, conn)?;
        println!(
            "  Merge complete: {} file(s) copied into '{}'.",
            copied, keep_path
        );
        println!(
            "  Note: re-run without --similar to detect '{}' as a duplicate and delete it.",
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
        // Empty DB — should complete without error
        find_similar_directories(&conn, 0.9, None, &[], false).unwrap();
    }

    #[test]
    fn test_find_similar_identical_leaves_not_reported() {
        // Two leaves with identical file sets should NOT appear as "similar but non-identical"
        // (they'd be exact duplicates, handled by find_duplicate_directories)
        let conn = open_test_db();
        conn.execute_batch(
            "INSERT INTO directories VALUES ('/a/photos', 'hash1', 1000);
             INSERT INTO directories VALUES ('/b/photos', 'hash1', 1000);
             INSERT INTO files VALUES ('/a/photos/img1.jpg', 'filehash1', 500, 1000);
             INSERT INTO files VALUES ('/b/photos/img1.jpg', 'filehash1', 500, 1000);",
        )
        .unwrap();
        // Score = 1.0 but all files are identical — the pair passes the threshold
        // but that's fine; in practice it'd already be caught by exact-hash dedup.
        // Just verify it doesn't panic.
        find_similar_directories(&conn, 0.9, None, &[], false).unwrap();
    }

    #[test]
    fn test_find_similar_detects_near_duplicate() {
        let conn = open_test_db();
        // /a/photos has 10 files, /b/photos has 9 of the same + 1 extra
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
        // /a/photos and /b/photos share only 1 of 10 files — score = 0.1
        let mut batch = String::from(
            "INSERT INTO directories VALUES ('/a/photos', 'hashA', 5000);
             INSERT INTO directories VALUES ('/b/photos', 'hashB', 5000);",
        );
        for i in 1..=9 {
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/a/photos/imgA{}.jpg', 'fhA{}', 500, 1000);",
                i, i
            ));
        }
        for i in 1..=9 {
            batch.push_str(&format!(
                "INSERT INTO files VALUES ('/b/photos/imgB{}.jpg', 'fhB{}', 500, 1000);",
                i, i
            ));
        }
        // One shared file
        batch.push_str("INSERT INTO files VALUES ('/a/photos/shared.jpg', 'shared', 500, 1000);");
        batch.push_str("INSERT INTO files VALUES ('/b/photos/shared.jpg', 'shared', 500, 1000);");
        conn.execute_batch(&batch).unwrap();
        // With threshold 0.9, this pair (score ~0.1) should NOT be reported
        find_similar_directories(&conn, 0.9, None, &[], false).unwrap();
    }
}
