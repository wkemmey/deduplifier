update according to style guide

do i want to allow interactive deletion of duplicate files in the same manner as directories?

compile for windows

cargo run -- --delete --similarity 0.85 --canon /home/whit/Dropbox/photos/pictures2016 /home/whit/Dropbox/photos/20100617_backup/

ls -a **/{ZbThumbnail.info,Picasa.ini,Thumbs.db,desktop.ini,.DS_Store}

rm -rf **/{ZbThumbnail.info,Picasa.ini,Thumbs.db,desktop.ini,.DS_Store}

ls -a **/{hpothb*.tif,hpothb*.dat}

rm -rf **/{hpothb*.tif,hpothb*.dat}




when merging, asking A or B doesn't really make much sense, because gonna keep both and make them identical.  would it be better to ask keep old or new?  or always assume new and just ask y/N?

will it find more than 2 similar directories?

i think for photos i often want to keep the older--unedited, unrotated, etc