#!/usr/bin/python
# coding: utf8

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.bucket import Bucket
from boto.s3.key import Key

AWS_ACCESS_KEY = "05236"
AWS_SECRET_ACCESS_KEY = "802562235"
BUCKET_NAME = "photo02"

conn = S3Connection(AWS_ACCESS_KEY,
                    AWS_SECRET_ACCESS_KEY,
                    host = "localhost",
                    port = 8080,
                    calling_format = OrdinaryCallingFormat(),
                    is_secure = False
       )

try:
  # create bucket
  bucket = conn.create_bucket(BUCKET_NAME)
  
  # create object
  s3_object = bucket.new_key("image")
  
  # write
  s3_object.set_contents_from_string("This is a text.")
  
  # show buckets
  for bucket in conn.get_all_buckets():
    print bucket
  
    # show S3Objects
    for obj in bucket.get_all_keys():
      print obj
  
    print
  
  # get bucket
  bucket = conn.get_bucket(BUCKET_NAME)
  print bucket
  
  # get S3Object
  s3_object = bucket.get_key("image")
  print s3_object
  
  # read
  print s3_object.read()
  
  # write from file
  s3_object.set_contents_from_filename("test.txt")
  
  # delete S3Object
  s3_object.delete()

  # get deleted key
  s3_object = bucket.get_key("image")
  # should print None
  print s3_object
finally:
  bucket = conn.get_bucket(BUCKET_NAME)
  bucket.delete()
