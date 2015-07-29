#!/usr/bin/python
# coding: utf8

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.bucket import Bucket
from boto.s3.key import Key
from filechunkio import FileChunkIO
import boto
import traceback
import os
import hashlib
import sys
from functools import partial

Host    = "localhost"
Port    = 8080

AccessKeyId     = "05236"
SecretAccessKey = "802562235"
SignVer         = "v4"

Bucket      = "testb"
TempData    = "../temp_data/"

SmallTestF  = TempData + "testFile"
LargeTestF  = TempData + "testFile.large"

s3 = None

def main():
    global SignVer
    if len(sys.argv) > 1:
        SignVer = sys.argv[1]
    try:
        init(SignVer)
        createBucket(Bucket)

        # Put Object Test
        putObject(Bucket, "test.simple",    SmallTestF)
        putObject(Bucket, "test.large",     LargeTestF)
    
#        # Multipart Upload Object Test
#        mpObject(Bucket, "test.simple.mp",  SmallTestF)
#        mpObject(Bucket, "test.large.mp",   LargeTestF)
    
        # Head Object Test
        headObject(Bucket, "test.simple",   SmallTestF)
        headObject(Bucket, "test.large",    LargeTestF)
#        headObject(Bucket, "test.simple.mp",SmallTestF)
    
        # Get Object Test
        getObject(Bucket, "test.simple",    SmallTestF)
        getObject(Bucket, "test.large",     LargeTestF)
#        getObject(Bucket, "test.simple.mp", SmallTestF)
#        getObject(Bucket, "test.large.mp",  LargeTestF)
    
        # Get Not Exist Object Test
        getNotExist(Bucket, "test.noexist")
    
        # Range Get Object Test
        rangeObject(Bucket, "test.simple",      SmallTestF, 1, 4)
        rangeObject(Bucket, "test.large",       LargeTestF, 1048576, 10485760)
#        rangeObject(Bucket, "test.simple.mp",   SmallTestF, 1, 4)
#        rangeObject(Bucket, "test.large.mp",    LargeTestF, 1048576, 10485760)
    
        # Copy Object Test
        copyObject(Bucket, "test.simple", "test.simple.copy")
        getObject(Bucket, "test.simple.copy", SmallTestF)
    
        # List Object Test
        listObject(Bucket, "", -1)
    
        # Delete All Object Test
        deleteAllObjects(Bucket)
        listObject(Bucket, "", 0)
    
        # Multiple Page List Object Test
        putDummyObjects(Bucket, "list/", 35, SmallTestF)
        pageListBucket(Bucket, "list/", 35, 10)
    
        # Multiple Delete
        multiDelete(Bucket, "list/", 10)
    
        # GET-PUT ACL
        setBucketAcl(Bucket, "private")
        setBucketAcl(Bucket, "public-read")
        setBucketAcl(Bucket, "public-read-write")

    except Exception, e:
        print traceback.format_exc()

def init(signVer):
    global s3
    if signVer == "v4":
        boto.config.add_section('s3')
        boto.config.set('s3', 'use-sigv4', 'True')
    else:
        boto.config.add_section('s3')
        boto.config.set('s3', 'use-sigv4', 'False')
    s3 = S3Connection(
            AccessKeyId,
            SecretAccessKey,
            host = 's3.amazonaws.com',
            proxy = Host,
            proxy_port = Port,
            calling_format = OrdinaryCallingFormat(),
            is_secure = False
            )

def createBucket(bucketName):
    print "===== Create Bucket [%s] Start =====" % bucketName
    s3.create_bucket(bucketName)
    print "===== Create Bucket End ====="
    print 

def putObject(bucketName, key, path):
    print "===== Put Object [%s/%s] Start =====" % (bucketName, key)
    bucket = s3.get_bucket(bucketName, validate=False)
    bucket.new_key(key).set_contents_from_filename(path)
    if not doesFileExist(bucketName, key):
        raise ValueError("Put Object [%s/%s] Failed!" % (bucketName, key))
    print "===== Put Object End ====="
    print

def mpObject(bucketName, key, path):
    print "===== Multipart Upload Object [%s/%s] Start =====" % (bucketName, key)
    bucket = s3.get_bucket(bucketName, validate=False)
    mp = bucket.initiate_multipart_upload(key)
    fileSize = os.path.getsize(path)
    offset = 0
    count = 0

    while (fileSize > offset):
        partSize = min(5 * 1024 * 1024, fileSize - offset)
        count = count + 1
        with FileChunkIO(path, 'rb', offset = offset, bytes=partSize) as fp:
            mp.upload_part_from_file(fp, part_num = count)
        offset = offset + partSize
    mp.complete_upload()

    if not doesFileExist(bucketName, key):
        raise ValueError("Multipary Upload Object [%s/%s] Failed!" % (bucketName, key))
    print "===== Multipart Upload Object End ====="
    print

def headObject(bucketName, key, path):
    print "===== Head Object [%s/%s] Start =====" % (bucketName, key)
    bucket = s3.get_bucket(bucketName, validate=False)
    obj = bucket.get_key(key)
    size = os.path.getsize(path)
    context = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(partial(f.read, 4096), ''):
            context.update(chunk)
    md5sum = context.hexdigest()
    etag = obj.etag[1:-1]

    print "ETag: %s, Size: %d" % (etag, obj.size)
    if etag != md5sum or obj.size != size:
        raise ValueError("Metadata [%s/%s] NOT Match, Size: %d, MD5: %s" % (bucketName, key, size, md5sum))
    print "===== Head Object End ====="
    print

def getObject(bucketName, key, path):
    print "===== Get Object [%s/%s] Start =====" % (bucketName, key)
    bucket = s3.get_bucket(bucketName, validate=False)
    obj = bucket.get_key(key)
    file = open(path)
    if not doesFileMatch(file, obj):
        raise ValueError("Content NOT Match!")
    print "===== Get Object End ====="
    print

def getNotExist(bucketName, key):
    print "===== Get Not Exist Object [%s/%s] Start =====" % (bucketName, key)
    try:
        obj.read(1)
        raise ValueError("Should NOT Exist!")
    except Exception, not_found:
        pass
    print "===== Get Not Exist Object End ====="
    print

def rangeObject(bucketName, key, path, start, end):
    print "===== Range Get Object [%s/%s] (%d-%d) Start =====" % (bucketName, key, start, end)
    bucket = s3.get_bucket(bucketName, validate=False)
    obj = bucket.get_key(key)
    obj.open_read(headers = {'Range': 'bytes=%d-%d' % (start, end)})
    file = FileChunkIO(path, 'rb', offset=start, bytes=end - start + 1)
    if not doesFileMatch(file, obj):
        raise ValueError("Content NOT Match!")
    print "===== Range Get Object End ====="
    print

def copyObject(bucketName, src, dst):
    print "===== Copy Object [%s/%s] -> [%s/%s] Start =====" % (bucketName, src, bucketName, dst)
    bucket = s3.get_bucket(bucketName, validate=False)
    bucket.copy_key(dst, bucketName, src)
    print "===== Copy Object End ====="
    print

def listObject(bucketName, prefix, expected):
    print "===== List Objects [%s/%s*] Start =====" % (bucketName, prefix)
    bucket = s3.get_bucket(bucketName, validate=False)
    count = 0
    for obj in bucket.list():
        if doesFileExist(bucketName, obj.key):
            print "%s \t Size: %d\n" % (obj.key, obj.size)
            count = count + 1
    if expected >= 0 and count != expected:
        raise ValueError("Number of Objects NOT Match!")
    print "===== List Objects End ====="
    print

def deleteAllObjects(bucketName):
    print "===== Delete All Objects [%s] Start =====" % bucketName
    bucket = s3.get_bucket(bucketName, validate=False)
    for obj in bucket.list():
        obj.delete()
    print "===== Delete All Objects End ====="
    print

def putDummyObjects(bucketName, prefix, total, holder):
    bucket = s3.get_bucket(bucketName, validate=False)
    for i in range(0, total):
        bucket.new_key(prefix+str(i)).set_contents_from_filename(holder)

def pageListBucket(bucketName, prefix, total, pageSize):
    print "===== Multiple Page List Objects [%s/%s*] %d Objs @%d Start =====" % (bucketName, prefix, total, pageSize)
    bucket = s3.get_bucket(bucketName, validate=False)
    marker = ""
    count = 0
    while True:
        print "===== Page ====="
        res = bucket.get_all_keys(
                prefix = prefix,
                max_keys = pageSize,
                marker = marker
                )
        for obj in res:
            count = count + 1
            print "%s \t Size: %d \t Count: %d" % (obj.key, obj.size, count)
        if not res.is_truncated:
            break
        else:
            marker = res.next_marker
    print "===== End ====="
    if count != total:
        raise ValueError("Number of Objects NOT Match!")
    print "===== Multiple Page List Objects End ====="
    print

def multiDelete(bucketName, prefix, total):
    print "===== Multiple Delete Objects [%s/%s] Start =====" % (bucketName, prefix)
    bucket = s3.get_bucket(bucketName, validate=False)
    delKeyList = []
    for i in range(0, total):
        delKeyList.append(prefix+str(i))
    res = bucket.delete_keys(delKeyList)
    for obj in res.deleted:
        print "Deleted %s/%s" % (bucketName, obj.key)
    if len(res.deleted) != total:
        raise ValueError("Number of Objects NOT Match!")
    print "===== Multiple Delete Objects End ====="
    print

def setBucketAcl(bucketName, permission):
    print "===== Set Bucket ACL [%s] (%s) Start =====" % (bucketName , permission)
    if permission == "private":
        checkList = ["FULL_CONTROL"]
    elif permission == "public-read":
        checkList = ["READ", "READ_ACP"]
    elif permission == "public-read-write":
        checkList = ["READ", "READ_ACP", "WRITE", "WRITE_ACP"]
    else:
        raise ValueError("Invalid Permission!")
    bucket = s3.get_bucket(bucketName, validate=False)
    bucket.set_acl(permission)
    res = bucket.get_acl()
    print "Owner ID: S3Owner [name=%s,id=%s]" % (res.owner.display_name, res.owner.id)
    list = []
    for grant in res.acl.grants:
        print "Grantee: %s \t Permissions: %s" % (grant.uri, grant.permission)
        list.append(grant.permission)
    if not all(x in list for x in checkList):
        raise ValueError("ACL NOT Match!")
    print "===== Set Bucket ACL End ====="
    print

def doesFileExist(bucketName, key):
    bucket = s3.get_bucket(bucketName, validate=False)
    obj = bucket.get_key(key)
    if obj:
        return obj.exists()
    else:
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

main()
