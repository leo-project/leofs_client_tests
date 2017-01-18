#!/bin/bash

TEMPDIR="/dev/shm/test"

mkdir -p $TEMPDIR

dd if=/dev/urandom of=$TEMPDIR/32kb bs=32K count=1
dd if=/dev/urandom of=$TEMPDIR/1mb  bs=1M count=1
dd if=/dev/urandom of=$TEMPDIR/5mb  bs=5M count=1
dd if=/dev/urandom of=$TEMPDIR/32mb bs=32M count=1
dd if=/dev/urandom of=$TEMPDIR/256mb    bs=32M count=8
