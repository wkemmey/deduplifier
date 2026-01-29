# Deduplifier

A Rust command-line application that scans directories, computes hashes for files and directories, stores them in a SQLite database, and finds duplicates.

## Features

- **File Hashing**: Computes SHA-256 hashes for all files in specified directories
- **Directory Hashing**: Computes non-recursive hashes for directories based on their immediate children (files and subdirectories)
- **Incremental Updates**: Avoids recalculation by checking file modification times - only rehashes files that have changed
- **SQLite Storage**: Stores all hash data with full paths and modification times in a SQLite database
- **Duplicate Detection**: 
  - Finds duplicate files (sorted by total size)
  - Finds duplicate directories (sorted by average size)

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
- `-h, --help`: Print help information

### Examples

Scan a single directory:
```bash
deduplifier /path/to/directory
```

Scan multiple directories:
```bash
deduplifier /path/to/dir1 /path/to/dir2 /path/to/dir3
```

Use a custom database file:
```bash
deduplifier -d my_hashes.db /path/to/directory
```

## How It Works

1. **File Scanning**: The tool walks through all specified directories and computes SHA-256 hashes for each file
2. **Change Detection**: Before hashing, it checks the modification time against the database to avoid unnecessary rehashing
3. **Directory Hashing**: For each directory, it computes a hash based on:
   - Hashes of immediate child files
   - Hashes of immediate child directories
   - Names are sorted alphabetically for repeatability
4. **Database Storage**: All hashes, paths, sizes, and modification times are stored in SQLite
5. **Duplicate Detection**: 
   - Files with identical hashes are reported as duplicates
   - Directories with identical hashes are reported as duplicates
   - Results are sorted by size (largest first)

## Database Schema

The tool creates two tables:

### `files` table
- `path` (TEXT, PRIMARY KEY): Full path to the file
- `hash` (TEXT): SHA-256 hash of the file content
- `size` (INTEGER): File size in bytes
- `modified` (INTEGER): Unix timestamp of last modification

### `directories` table
- `path` (TEXT, PRIMARY KEY): Full path to the directory
- `hash` (TEXT): Computed hash based on immediate children
- `size` (INTEGER): Total size of all files in the directory tree

## Example Output

```
Scanning directory: "/home/user/documents"

=== Finding Duplicate Files ===

Duplicate files (hash: 54c91bb9a50f98a6, count: 2, total size: 104 bytes):
  - /home/user/documents/folder1/file.txt (52 bytes)
  - /home/user/documents/folder2/file_copy.txt (52 bytes)

=== Finding Duplicate Directories ===
No duplicate directories found.
```

## License

MIT