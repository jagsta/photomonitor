photomonitor
============

linux inotify based photo archiver

This perl script was written by me to watch a directory on a central NAS which acts as a dump for images from all devices in our household. When images/folders are copied or moved into the watched directory the script archives them and converts the RAW/original files to a lower quality full sized JPG which is saved in a "working" directory tree for use with applications like Picasa at an acceptable speed.

The script uses inotify to provide realtime processing of images (i'm impatient)

In order to handle large volumes of images being dumped into the watched folder the script runs as multiple threads:

WATCHER - responsible for setting up the inotify watches and monitoring them
SPOOLER - responsible for passing image names to processing threads and receiving results
PROCESSOR(s) - responsible for the image conversion and archiving 

You can run as many processors as you like, the default is two and it's hard coded.

The images are stored by Exif date in a YYYY/YYYY-MM-DD directory structure.

raw image conversion is performed using the excellend dcraw http://www.cybercom.net/~dcoffin/dcraw/
