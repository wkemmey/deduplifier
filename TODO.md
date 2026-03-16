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
  