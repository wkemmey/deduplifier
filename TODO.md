update according to style guide

do i want to allow interactive deletion of duplicate files in the same manner as directories?

compile for windows

cargo run -- --delete --similarity 0.85 --canon /home/whit/Dropbox/photos/pictures2016 /home/whit/Dropbox/photos/20100617_backup/

ls -a **/{ZbThumbnail.info,Picasa.ini,Thumbs.db,desktop.ini,.DS_Store}

rm -rf **/{ZbThumbnail.info,Picasa.ini,Thumbs.db,desktop.ini,.DS_Store}

ls -a **/{hpothb*.tif,hpothb*.dat}

rm -rf **/{hpothb*.tif,hpothb*.dat}



Options:
      --dup-dirs                  find duplicate directories and optionally delete them (see --delete)
      --dup-files                 find duplicate files
      --similarity [<THRESHOLD>]  find and interactively merge similar (but non-identical) directories; optionally specify a similarity threshold (0.0–1.0, default 0.85)
      --merge                     merge two directory trees together (not yet implemented)
      --sort-photos               sort photos into a date-based folder hierarchy (not yet implemented)
      --database <DATABASE>       database file path [default: deduplifier.db]
      --canon <CANON>             canonical directory: when a duplicate exists under this path, auto-select it as the one to keep
      --delete                    interactively delete duplicate directories (used with --dup-dirs)
      --no-confirmation           skip per-deletion confirmation prompts when --canon has auto-selected the keeper
  -h, --help                      Print help



  implement --merge

  add --prune, which deletes all files in a directory tree that have duplicates elsewhere -- just thinking about this, not sure yet

refactor so all file operations are in one place

refactor so test helpers can help tests be more dry by setting up data for test

refactor so ui is separate from logic--e.g., similar.rs shouldn't have both computation and prints


remember to put updated direnv.nix in nixos config project

do i want to rename should_update_file to something that makes more sense and is more readable?

run tests on windows

should count_files be moved to main, or wherever the gui code is?


For next time, I'd recommend scan.rs. Here's the reasoning:

It's the most self-contained refactor. Every inline SQL call in scan.rs has a direct 1-to-1 replacement in db.rs — init_visited_files, mark_visited, should_update_file, upsert_file, stale_file_count, delete_stale_files. No tricky logic changes, just swapping call sites.

It's the foundation for the others. scan.rs populates the DB that everything else reads. Getting it clean first means you have a reliable, well-abstracted pipeline from disk → DB before touching the consumers.

merge.rs and photos.rs are more involved. They have local db_* helper functions with inline SQL and error handling that swallows errors with eprintln! instead of propagating — there's more to untangle there, so they're better tackled with fresh energy after the easier wins.

duplicates.rs is in the middle — a reasonable second step after scan.rs.

So the order: scan.rs → duplicates.rs → merge.rs + photos.rs (together, since they're nearly identical).




consitent ordering of use commands--global to local





One question before we proceed: should file_system::move_file be used for fs::rename? Rename is atomic on the same filesystem but fails across filesystems; file_system::move_file may handle the cross-filesystem case (copy + delete). In merge.rs this matters since canon and source could be on different volumes. What does file_system.rs currently expose?


similar.rs currently has no DB awareness in its merge — it copies files but doesn't update the DB. This is noted in the printed message ("re-run without --similarity to detect exact duplicates"), so it's a known limitation, not a bug. But it's worth calling out as a gap.