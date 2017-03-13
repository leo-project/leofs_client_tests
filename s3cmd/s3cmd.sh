#!/bin/bash

function gen_test_sync_dir {
    mkdir -p test_sync
    for i in $(seq 1 3)
    do
        mkdir -p test_sync/$i/
        for j in $(seq 1 2)
        do
            dd if=/dev/urandom of=test_sync/$i/rand_file$j bs=4K count=1
        done
    done
    s3cmd sync test_sync s3://s3cmd-bucket/test_sync/ && \
        check_count 6 && \
        rm -rf test_sync/1 && \
        s3cmd --delete-removed sync test_sync s3://s3cmd-bucket/test_sync/ && \
        check_count 4
}

function check_count() {
    EXPECT=$1
    COUNT=`s3cmd ls --recursive s3://s3cmd-bucket/test_sync/ | wc -l`
    if [ $COUNT -eq $EXPECT ]; then
        return 0
    else
        echo "[Failed] ls check_count, expected:$EXPECT, value:$COUNT"
        return 1
    fi
}

which s3cmd && \
    s3cmd mb s3://s3cmd-bucket && \
    s3cmd put README s3://s3cmd-bucket/README && \
    s3cmd put s3cmd-bucket s3://s3cmd-bucket/s3cmd-bucket && \
    s3cmd ls s3://s3cmd-bucket && \
    s3cmd get s3://s3cmd-bucket/README README.copy && \
    diff README README.copy && rm README.copy && \
    s3cmd cp s3://s3cmd-bucket/README s3://s3cmd-bucket/README.copy && \
    s3cmd mv s3://s3cmd-bucket/README.copy s3://s3cmd-bucket/README.org && \
    s3cmd del s3://s3cmd-bucket/README s3://s3cmd-bucket/README.org && \
    s3cmd ls s3://s3cmd-bucket && \
    s3cmd put README s3://s3cmd-bucket/a/b/c/README && \
    s3cmd put s3cmd.sh s3://s3cmd-bucket/a/b/c/s3cmd.sh && \
    s3cmd ls s3://s3cmd-bucket && \
    s3cmd del s3://s3cmd-bucket/a && \
    s3cmd ls s3://s3cmd-bucket && \
    gen_test_sync_dir && \
    s3cmd rb s3://s3cmd-bucket

