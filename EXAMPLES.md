# Example Usage

This file demonstrates some example usage scenarios for deduplifier.

## Basic Usage

Scan a single directory:
```bash
cargo run -- /path/to/directory
```

## Multiple Directories

Scan multiple directories at once:
```bash
cargo run -- /home/user/documents /home/user/downloads /mnt/backup
```

## Custom Database

Use a custom database file location:
```bash
cargo run -- --database /path/to/my_hashes.db /path/to/directory
```

## Example Output

When you run deduplifier on a directory with duplicates:

```
$ cargo run -- /tmp/test_dirs
Scanning directory: "/tmp/test_dirs"

=== Finding Duplicate Files ===

Duplicate files (hash: c70f8f950cbcbddf, count: 3, total size: 21 bytes):
  - /tmp/test_dirs/folder1/file.txt (7 bytes)
  - /tmp/test_dirs/folder2/copy.txt (7 bytes)
  - /tmp/test_dirs/backup/file.txt (7 bytes)

=== Finding Duplicate Directories ===

Duplicate directories (hash: 98eb7d7eb5758bad, count: 2, size: 42 bytes):
  - /tmp/test_dirs/backup_2024 (42 bytes)
  - /tmp/test_dirs/backup_2023 (42 bytes)
```

## Incremental Updates

The tool tracks file modification times. Running it again on the same directory without changes is very fast:

First run:
```bash
$ time cargo run -- /large/directory
... (processes all files)
real    0m15.234s
```

Second run (no changes):
```bash
$ time cargo run -- /large/directory
... (skips unchanged files)
real    0m0.342s
```

## Finding Large Duplicates

Results are sorted by size, so the largest duplicates appear first. This helps identify the biggest space-saving opportunities.
