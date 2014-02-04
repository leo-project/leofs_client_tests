#!/bin/bash


which s3cmd && \
    s3cmd mb s3://s3cmd-bucket && \
    s3cmd put README s3://s3cmd-bucket/README && \
    s3cmd ls s3://s3cmd-bucket && \
    s3cmd get s3://s3cmd-bucket/README README.copy && \
    diff README README.copy && rm README.copy && \
    s3cmd cp s3://s3cmd-bucket/README s3://s3cmd-bucket/README.copy && \
    s3cmd mv s3://s3cmd-bucket/README.copy s3://s3cmd-bucket/README.org && \
    s3cmd del s3://s3cmd-bucket/README s3://s3cmd-bucket/README.org && \
    s3cmd ls s3://s3cmd-bucket && \
    s3cmd rb s3://s3cmd-bucket

