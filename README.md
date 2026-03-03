# Deduplifier

A Rust command-line application that scans directories, computes hashes for files and directories, stores them in a SQLite database, and finds duplicates.

## Features

- **File Hashing**: Computes SHA-256 hashes for all files in specified directories
- **Directory Hashing**: Computes hashes for directories based on their immediate children (files and subdirectories), enabling whole-tree duplicate detection
- **Incremental Updates**: Avoids recalculation by checking file modification times — only rehashes files that have changed
- **SQLite Storage**: Stores all hash data with full paths and modification times in a SQLite database
- **Stale Entry Cleanup**: Detects files in the database that no longer exist on disk and offers to remove them
- **Duplicate Detection**:
  - Finds duplicate directories (default, sorted by size largest-first)
  - Finds duplicate files (with `--files`)
- **Interactive Deletion**: With `--delete`, walks through each duplicate group and asks which copy to keep; requires typing the full path to confirm deletion
- **Canonical Directory**: With `--canon`, automatically keeps whichever copy lives under the specified path, making bulk cleanup scriptable

## Installation

### Build from source

```bash
cargo build --release
```

The binary will be available at `target/release/deduplifier`.

## Usage

```bash
deduplifier [OPTIONS] <DIRECTORIES>...
```

### Arguments

- `<DIRECTORIES>...`: One or more directories to scan (required)

### Options

- `-d, --database <DATABASE>`: Database file path (default: `deduplifier.db`)
- `--files`: Also report duplicate files (in addition to duplicate directories)
- `--delete`: Interactively delete duplicate directories
- `--canon <PATH>`: Canonical directory — when a duplicate exists under this path, auto-select it as the copy to keep
- `-h, --help`: Print help information

### Examples

Scan a single directory:
```bash
deduplifier /path/to/directory
```

Scan multiple directories:
```bash
deduplifier /path/to/dir1 /path/to/dir2
```

Also report duplicate files:
```bash
deduplifier --files /path/to/directory
```

Interactively delete duplicates, keeping whichever copy is under `/my/canon`:
```bash
deduplifier --delete --canon /my/canon /path/to/other
```

Use a custom database file:
```bash
deduplifier -d my_hashes.db /path/to/directory
```

## How It Works

1. **File Scanning**: Walks all specified directories, computing SHA-256 hashes for each file. Progress is shown as a single overwriting line.
2. **Change Detection**: Before hashing, checks the modification time against the database to skip files that haven't changed.
3. **Stale Cleanup**: After scanning, detects any paths in the database that were not seen on disk, and offers to remove them.
4. **Directory Hashing**: For each directory, computes a hash based on the names and hashes of its immediate children (files and subdirectories), sorted alphabetically for repeatability. This is done bottom-up so parent hashes incorporate subtree changes.
5. **Duplicate Detection**: Groups files or directories by hash; reports groups with more than one member, sorted by size.
6. **Interactive Deletion**: With `--delete`, presents each duplicate group and prompts for which copy to keep. Requires typing the full directory path to confirm — no accidental deletions. Removes deleted paths from the database immediately.

## Code Structure

The codebase is split into modules primarily to keep each piece independently testable. Functions that interact with the database, filesystem, and user all have different testing needs, so separating them means tests can be focused and avoid side effects.

- **`main.rs`**: CLI argument parsing (`clap`) and top-level orchestration only. Also contains `build_scan_list`, which determines scan order (canon directory always first).
- **`db.rs`**: Database setup (`setup_schema`, `init_database`) and the `should_update_file` query. Isolated here so tests can use an in-memory SQLite connection without touching the filesystem.
- **`hashing.rs`**: Hashing operations — `path_to_str`, `count_files`, `compute_file_hash`, and `compute_directory_hash`. Named for its primary responsibility rather than the filesystem, since `compute_directory_hash` also reads from the `directories` table. These are straightforward to test with temporary directories.
- **`scan.rs`**: `scan_directory` ties `db` and `hashing` together — it walks the directory tree, hashes new/changed files, tracks visited paths for stale detection, and triggers the bottom-up directory hash pass. Tested with temp directories and in-memory databases.
- **`duplicates.rs`**: `find_duplicate_files` and `find_duplicate_directories` query the database for hash collisions and handle interactive deletion. Tested by seeding an in-memory database directly, so no filesystem scanning is needed.

## Database Schema

### `files` table
- `path` (TEXT, PRIMARY KEY): Full path to the file
- `hash` (TEXT): SHA-256 hash of the file content
- `size` (INTEGER): File size in bytes
- `modified` (INTEGER): Unix timestamp of last modification

### `directories` table
- `path` (TEXT, PRIMARY KEY): Full path to the directory
- `hash` (TEXT): Computed hash based on immediate children (relative names + content hashes)
- `size` (INTEGER): Total size of all immediate children

## License

MIT