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
      --merge                     merge two directory trees together
      --sort-photos               sort photos into a date-based folder hierarchy
      --database <DATABASE>       database file path [default: deduplifier.db]
      --canon <CANON>             canonical directory: when a duplicate exists under this path, auto-select it as the one to keep
      --delete                    interactively delete duplicate directories (used with --dup-dirs)
      --no-confirmation           skip per-deletion confirmation prompts when --canon has auto-selected the keeper
  -h, --help                      Print help




  add --prune, which deletes all files in a directory tree that have duplicates elsewhere -- just thinking about this, not sure yet




remember to put updated direnv.nix in nixos config project

do i want to rename should_update_file to something that makes more sense and is more readable?

run tests on windows

should count_files be moved to main, or wherever the gui code is?


## Interactive TUI (future idea)

Two options for adding an interactive shell UI (directory picker, operation selector, progress display, conflict prompts):

1. **`inquire` (wizard/prompt style):** Easiest. Provides fuzzy multi-select, confirms, and prompts out of the box. Directory picking would be a fuzzy-filtered multi-select over a walked directory tree. ~1 day of work. No full-screen UI, just guided prompts in sequence.

2. **`ratatui` + `crossterm` (full TUI):** Full-screen terminal UI with layout, widgets, and keyboard navigation. Directory picker would need to be hand-rolled (no built-in widget). Logic callbacks (`on_progress`, `on_event`, etc.) map cleanly to TUI updates. Long-running operations would need a background thread + `mpsc` channel to keep the UI responsive. ~3–5 days of work.





similar.rs currently has no DB awareness in its merge — it copies files but doesn't update the DB. This is noted in the printed message ("re-run without --similarity to detect exact duplicates"), so it's a known limitation, not a bug. But it's worth calling out as a gap.




UI mixed with logic in similar.rs and duplicates.rs — you have this in your TODO already. find_similar_directories interleaves computation with println!, stdin.lock(), and prompting. This is the most substantive remaining architectural issue.


photos.rs has similar UI/logic mixing — sort_photos both does DB operations and prints progress.