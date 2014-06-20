#!/usr/bin/python
# coding: utf8

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.bucket import Bucket
from boto.s3.key import Key
import boto
import hashlib
import os
import mimetypes
import random
import traceback
import magic

AWS_ACCESS_KEY = "05236"
AWS_SECRET_ACCESS_KEY = "802562235"
BUCKET_NAME = "test" + str(random.randint(1,99999))  ## Dynamic BucketName
FILE_NAME = "testFile"
CHUNK_SIZE = 5 * 1024 * 1024

conn = S3Connection(AWS_ACCESS_KEY,
    AWS_SECRET_ACCESS_KEY,
    host = "localhost",
    port = 8080,
    calling_format = OrdinaryCallingFormat(),
    is_secure = False
)

try:
    # Create bucket
    print "Bucket Creation Test [Start]\n"
    buckets = conn.create_bucket(BUCKET_NAME)
    print "Bucket Created Successfully"

    # Show buckets
    print "--------Bucket List------"
    for bucket in conn.get_all_buckets():
        print bucket
    print "Bucket Creation Test [End]\n"

    # Get Bucket
    bucket = conn.get_bucket(BUCKET_NAME,validate=False)
    print "Get Bucket Successfully\n"

    # Put Object
    print "File Upload Test [Start]\n"
    file_path = "../temp_data/" + FILE_NAME
    file_object = open(file_path, "r")
    file_digest = hashlib.md5(file_object.read()).hexdigest()
    file_size = os.path.getsize(file_path)
    file_type = magic.from_file(file_path, mime=True)

    # Files in Amazon S3 are called "objects" and are stored in buckets. A specific object is 
    # referred to by its key (i.e., name) and holds data. Here, we create a new object with 
    # the key name, HEAD request is Metadata of that object. e.g. Size, etag, Content_type etc.
    # For more information http://boto.readthedocs.org/en/latest/s3_tut.html#storing-data

    # PUT Object using single-part method
    print "File is being upload:"
    bucket.new_key(FILE_NAME).set_contents_from_filename(file_path)

    # HEAD Object
    obj = bucket.get_key(FILE_NAME)
    if not(obj.exists()):
        raise "Object doesn't exists"
    if not(file_size == obj.size and file_digest == obj.etag[1:-1]):
        raise "File Metadata could not match"
    else:
        print "File MetaData : Content_type:", obj.content_type, "\b, Content_encoding:", obj.content_encoding
        print "\b, etag:", obj.etag, "\b, Size:", obj.size, "\b, Name:", obj.name, "\n"

    # GET object
    if not file_size == obj.size:
        raise "Upload File content is not equal\n"
    if "text/plain" in file_type:
        print "Uploaded object data : \t", obj.read()
    else:
        print "File Content type is :", obj.content_type

    # Show Objects
    print"--------------------------------List Objects-----------------------------------"
    for key in bucket.list():
        if not(file_size == key.size):
            raise "Content length is changed for :", key.size
        print key.name, "\t\t", key.size, "\t\t", key.last_modified
    print "File Uploaded Successfully\n"
    print "File Upload Test [End]\n"

    # File copy
    print "File Copy Test [Start]\n"
    obj = bucket.copy_key(FILE_NAME + ".copy", BUCKET_NAME, FILE_NAME)
    if not(obj.exists()):
       raise "File could not Copy Successfully\n"

    # Show Objects
    print"--------------------------------List Objects-----------------------------------"
    for key in bucket.list():
        if not(file_size == key.size):
            raise "Content length is changed for :", key.size
        print key.name, "\t\t", key.size, "\t\t", key.last_modified
    print "File copied successfully\n"
    print "File Copy Test [End]\n"

    # File Download
    print "File Download Test [Start]\n"
    this_file_path = FILE_NAME + ".copy"
    obj.get_contents_to_filename(this_file_path)
    this_file_object = open(this_file_path, "r")
    this_file_digest = hashlib.md5(this_file_object.read()).hexdigest()
    this_file_size = os.path.getsize(this_file_path)
    if not(this_file_size == obj.size and this_file_digest == file_digest):
        raise "Downloaded File Metadata could not match\n"
    print "File Downloaded Successfully\n"
    print "File Download Test [End]\n"

    # Delete objects one by one and check if exist
    print "File Delete Test [Start]\n"
    print "--------------------------------Delete Objects---------------------------------"
    for key in bucket.list():
        print key.name, "\tDeleted Successfully", key.delete()
        if key.exists():
            raise "\tObject is not Deleted Successfully\n"
        try:
            print "Deleted files:", key.read()
        except Exception, not_found:
            print not_found
            continue
        raise "Deleted Failed\n"
    print "\nFile Delete Test [End]\n"

    # Get-Put ACL
    print "Object ACL Test [Start]"
    print "\n#####Default ACL#####"
    acp = bucket.get_acl()
    print acp
    print "Owner ID :" + acp.owner.id
    print "Owner Display name : " + acp.owner.display_name
    permissions = []
    for grant in acp.acl.grants:
        print "Bucket ACL is :", grant.permission,"\nBucket Grantee URI is :", grant.uri
        permissions.append(grant.permission)
    if not all(x in permissions for x in ["FULL_CONTROL"]):
        raise "Permission is Not full_control"
    else:
        print "Bucket ACL permission is 'private'\n"

    print "########:public_read ACL########"
    bucket.set_acl("public-read")
    acp = bucket.get_acl()
    print "Owner ID :", acp.owner.id
    print "Owner Display name :", acp.owner.display_name
    permissions = []
    for grant in acp.acl.grants:
        print "Bucket ACL is :", grant.permission,"\nBucket Grantee URI is :", grant.uri
        permissions.append(grant.permission)
    if not all(x in permissions for x in ["READ","READ_ACP"]):
        raise "Permission is Not public_read"
    else:
        print "Bucket ACL Successfully changed to 'public-read'\n"

    print "#####:public_read_write ACL#####"
    bucket.set_acl("public-read-write")
    acp = bucket.get_acl()
    print "Owner ID :", acp.owner.id
    print "Owner Display name :", acp.owner.display_name
    permissions = []
    for grant in acp.acl.grants:
        print "Bucket ACL is :", grant.permission,"\nBucket Grantee URI is :", grant.uri
        permissions.append(grant.permission)
    if not all(x in permissions for x in ["READ","READ_ACP","WRITE", "WRITE_ACP"]):
        raise "Permission is Not public_read_write"
    else:
        print "Bucket ACL Successfully changed to 'public-read-write'\n"

    print "##########:private ACL##########"
    bucket.set_acl("private")
    acp = bucket.get_acl()
    print "Owner ID :", acp.owner.id
    print "Owner Display name :", acp.owner.display_name
    permissions = []
    for grant in acp.acl.grants:
        print "Bucket ACL is :", grant.permission,"\nBucket Grantee URI is :", grant.uri
        permissions.append(grant.permission)
    if not all(x in permissions for x in ["FULL_CONTROL"]):
        raise "Permission is Not full_control"
    else:
        print "Bucket ACL Successfully changed to 'private'\n"
    print "Bucket ACL Test [End]\n"
except Exception,e :
    print traceback.format_exc()
finally:
    # Delete Bucket
    print "Bucket Delete Test [Start]\n"
    bucket = conn.get_bucket(BUCKET_NAME,validate=False)
    bucket.delete()
    print "Bucket Deleted Successfully\n"
    print "Bucket Delete Test [End]\n"
