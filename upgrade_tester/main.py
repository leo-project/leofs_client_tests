#!/usr/bin/env python

import os
import time
from boto3client import *

import sys

sys.path.insert(0,  "../boto3/")

#Host    = "localhost"
Host    = "192.168.100.35"
Port    = 8080

AccessKeyId     = "05236"
SecretAccessKey = "802562235"
SignVer         = "v2"

TempDir = "/dev/shm/test/"

test32k     = os.path.join(TempDir, "32kb")
test1m      = os.path.join(TempDir, "1mb")
test5m      = os.path.join(TempDir, "5mb")
test32m     = os.path.join(TempDir, "32mb")
test256m    = os.path.join(TempDir, "256mb")

test_set = [
        {"path" : test32k,    "name" : "test32k",   "count" : 100},
        {"path" : test1m,     "name" : "test1m",    "count" : 100},
        {"path" : test5m,     "name" : "test5m",    "count" : 100},
        {"path" : test32m,    "name" : "test32m",   "count" : 10},
        {"path" : test256m,   "name" : "test256m",  "count" : 10}
        ]

def wait_for_upgrade():
    print "Wating for Upgrade"

    ## TODO: Automatically check if ugprade is completed

    raw_input("Press Enter to continue...")

def doesFileExist(bucketName, key):
    try:
        s3.head_object(
                Bucket = bucketName,
                Key = key)
        return True
    except:
        return False

def doesFileMatch(io1, io2):
    while True:
        b1 = io1.read(4096)
        b2 = io2.read(4096)
        if b1 == "":
            return b2 == ""
        elif b2 == "":
            return b1 == ""
        elif b1 != b2:
            return False

s3 = Boto3Client(SignVer, Host, Port, AccessKeyId, SecretAccessKey)

s3.do_create_bucket("test_pre")
s3.do_create_bucket("test_after")
s3.set_bucket("test_pre")

old_count = 0
for test_ele in test_set:
    print "[Old] [Put] %s\t(%d)" % (test_ele["name"], test_ele["count"])
    old_count = old_count + test_ele["count"]
    for i in range(0, test_ele["count"]):
        s3.do_put_object(test_ele["name"]+"_"+str(i), test_ele["path"])

wait_for_upgrade()

s3.set_bucket("test_after")
for test_ele in test_set:
    print "[New] [Put] %s\t(%d)" % (test_ele["name"], test_ele["count"])
    for i in range(0, test_ele["count"]):
        s3.do_put_object(test_ele["name"]+"_"+str(i), test_ele["path"])

s3.set_bucket("test_pre")
old_list = s3.do_list_object("")['objects']

if len(old_list) != old_count:
    raise ValueError("Number of Entry NOT Match!")

for test_ele in test_set:
    print "[Old] [Get] %s\t(%d)" % (test_ele["name"], test_ele["count"])
    base_file = open(test_ele["path"])
    for i in range(0, test_ele["count"]):
        ret = s3.do_get_object(test_ele["name"]+"_"+str(i))
        if not doesFileMatch(base_file, ret["content"]):
            raise ValueError("Content NOT Match!")
        base_file.seek(0)

