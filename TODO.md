add tests

do i want to check directories before files?

why are we using struct and can we use db instead?

do i want to be able to --scan --files --directories or not?

make a source of truth, and delete directories not in source of truth--ask first

is directory hash made up of files only, or subdirectories too?  should be subdirectories too

can i list (and possibly delete) the largest duplicate directories first?  shouldn't matter whether i order by space or depth

do i have any file names that have invalid characters?

don't count duplicate directories that are inside another duplicate directory
--but that could cause problems when multiple directories are not exact duplicates of each other

list largest duplicates first!!!