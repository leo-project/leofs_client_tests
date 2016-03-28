#!/bin/bash

dd if=/dev/urandom of=testFile.large bs=5M count=20
dd if=/dev/urandom of=testFile.medium bs=5M count=2
