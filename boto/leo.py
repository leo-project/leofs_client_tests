#!/usr/bin/python
# coding: utf8

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.bucket import Bucket
from boto.s3.key import Key

AWS_ACCESS_KEY = "05236"
AWS_SECRET_ACCESS_KEY = "802562235"

conn = S3Connection(AWS_ACCESS_KEY,
                    AWS_SECRET_ACCESS_KEY,
                    host = "localhost",
                    port = 8080,
                    calling_format = OrdinaryCallingFormat(),
                    is_secure = False)

# show buckets
for bucket in conn.get_all_buckets():
    print bucket

    # show S3Objects
    for obj in bucket.get_all_keys():
        print obj

    print

# get bucket
bucket = conn.get_bucket("leofs")
print bucket

# create object
s3_object = bucket.new_key("kanetayusaku01.txt")

# write
s3_object.set_contents_from_string("This is a test.")

# get S3Object
s3_object = bucket.get_key("kanetayusaku01.txt")
print s3_object

# read
print s3_object.read()
