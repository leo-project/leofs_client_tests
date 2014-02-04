s3cmd mb s3://bucket1/
#/usr/local/bin/fusermount -u /mnt/s3fs-fuse/
#/usr/local/bin/fusermount -u /mnt/s3fs/
sudo -u jenkins /usr/local/s3fs-fuse/bin/s3fs  bucket1 /mnt/s3fs-fuse -o allow_other,uid=498,gid=498,umask=0022     -o url='http://localhost:8080' 
sudo -u jenkins /usr/local/s3fs/bin/s3fs   bucket1 /mnt/s3fs/         -o allow_other,uid=498,gid=498,umask=0022    -o url='http://localhost:8080'




