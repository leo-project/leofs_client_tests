#!/bin/bash

cp README /mnt/s3fs && \
ls /mnt/s3fs && \
cp /mnt/s3fs/README README.copy && \
diff README README.copy && rm README.copy && \
cp README /mnt/s3fs/README.copy && \
mv /mnt/s3fs/README.copy /mnt/s3fs/README.org && \
rm /mnt/s3fs/README /mnt/s3fs/README.org && \
ls /mnt/s3fs

