#!/bin/bash

cp -p README /mnt/s3fs-fuse && \
ls -l /mnt/s3fs-fuse && \
cp  -p /mnt/s3fs-fuse/README README.copy && \
diff README README.copy && rm README.copy && \
cp README /mnt/s3fs-fuse/README.copy && \
mv /mnt/s3fs-fuse/README.copy /mnt/s3fs-fuse/README.org && \
rm /mnt/s3fs-fuse/README /mnt/s3fs-fuse/README.org && \
ls /mnt/s3fs-fuse
/usr/bin/fusermount -u /mnt/s3fs-fuse

