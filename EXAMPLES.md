# Example Usage

This file demonstrates common usage scenarios for deduplifier.

## Basic Usage

Scan a single directory and report duplicate directories:
```bash
cargo run -- /path/to/directory
```

Or with the release build:
```bash
./target/release/deduplifier /path/to/directory
```

## Multiple Directories

Scan multiple directories at once — duplicates across them will be found:
```bash
cargo run -- /home/user/documents /home/user/downloads /mnt/backup
```

## Also Report Duplicate Files

By default only duplicate directories are shown. Pass `--files` to also report duplicate files:
```bash
cargo run -- --files /path/to/directory
```

## Custom Database

Use a custom database file location:
```bash
cargo run -- --database /path/to/my_hashes.db /path/to/directory
```

## Interactive Deletion

Pass `--delete` to interactively choose which copy of each duplicate directory to keep.
You will be shown a numbered list and asked to type the full path of each directory
you want to delete to confirm — there are no accidental deletions:

```
$ cargo run -- --delete /home/user/photos /mnt/backup/photos

Duplicate directories (hash: 98eb7d7eb5758bad…, count: 2, size: 524288000 bytes each):
  [1] /home/user/photos/vacation_2023 (524288000 bytes)
  [2] /mnt/backup/photos/vacation_2023 (524288000 bytes)
  Keep which? (1-2, or 's' to skip): 1
  Keeping:  /home/user/photos/vacation_2023
  Will permanently delete:
    - /mnt/backup/photos/vacation_2023
  Type the directory name to confirm deletion of
  '/mnt/backup/photos/vacation_2023'
  > /mnt/backup/photos/vacation_2023
  Deleted '/mnt/backup/photos/vacation_2023'.
  Removed '/mnt/backup/photos/vacation_2023' and its contents from the database.
```

## Canonical Directory

If you have a known "source of truth" directory (e.g. a primary drive), use `--canon`
to automatically keep whichever copy lives there, without being prompted for each group.
The canon directory is always scanned first so its hashes are in the database before
the others are compared against it:

```bash
cargo run -- --delete --canon /home/user/photos /mnt/backup/photos
```

This will auto-select any duplicate found under `/home/user/photos` as the keeper and
delete the copy under `/mnt/backup/photos` after type-to-confirm.

## Stale Entry Cleanup

If files have been deleted from disk since the last scan, deduplifier will detect them
and offer to remove the stale entries from the database:

```
3 file(s) in the database no longer exist on disk under "/home/user/photos".
Delete them from the database? [y/N]
```

## Incremental Updates

The tool tracks file modification times. Running it again on the same directory
without changes is very fast because unchanged files are loaded from the database
rather than rehashed:

First run (all files hashed):
```bash
$ time cargo run -- /large/directory
Found 48301 files to process
...
real    2m14s
```

Second run (no changes — hashes loaded from DB):
```bash
$ time cargo run -- /large/directory
Found 48301 files to process
...
real    0m3.1s
```

## Finding Large Duplicates

Results are sorted by size largest-first, so the biggest space-saving opportunities
appear at the top.
