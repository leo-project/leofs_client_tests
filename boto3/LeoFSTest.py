#!/usr/bin/python
# coding: utf8

from filechunkio import FileChunkIO
import boto3
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
import traceback
import os
import hashlib
import sys
import time
from functools import partial

Host    = "localhost"
Port    = 8080

AccessKeyId     = "05236"
SecretAccessKey = "802562235"
SignVer         = "v4"

Bucket      = "testb"
TempData    = "../temp_data/"

SmallTestF  = TempData + "testFile"
MediumTestF = TempData + "testFile.medium"
LargeTestF  = TempData + "testFile.large"

MetadataKey = "cmeta_key"
MetadataVal = "cmeta_val"
MetadataMap = {MetadataKey : MetadataVal}

s3 = None

def main():
    global SignVer
    global Host
    global Port
    global Bucket

    if len(sys.argv) > 1:
        SignVer = sys.argv[1]
    if len(sys.argv) > 2:
        Host = sys.argv[2]
        Port = int(sys.argv[3])
        Bucket = sys.argv[4]
    try:
        init(SignVer, Host, Port)
        createBucket(Bucket)

        # Put Object Test
        putObject(Bucket, "test.simple",    SmallTestF)
        putObject(Bucket, "test.medium",    MediumTestF)
        putObject(Bucket, "test.large",     LargeTestF)
    
        # Put Object with Metadata Test
        putObjectWithMeta(Bucket, "test.simple.meta", SmallTestF, MetadataMap)
        putObjectWithMeta(Bucket, "test.large.meta", LargeTestF, MetadataMap)

#        # Multipart Upload Object Test
#        mpObject(Bucket, "test.simple.mp",  SmallTestF)
#        mpObject(Bucket, "test.large.mp",   LargeTestF)
  
        # Head Object Test
        headObject(Bucket, "test.simple",   SmallTestF)
        headObject(Bucket, "test.large",    LargeTestF)
#        headObject(Bucket, "test.simple.mp",SmallTestF)
    
        # Get Object Test
        getObject(Bucket, "test.simple",    SmallTestF)
        getObject(Bucket, "test.medium",    MediumTestF)
        getObject(Bucket, "test.large",     LargeTestF)
#        getObject(Bucket, "test.simple.mp", SmallTestF)
#        getObject(Bucket, "test.large.mp",  LargeTestF)
    
        # Get Object Again (Cache) Test
        getObject(Bucket, "test.simple",    SmallTestF)
        getObject(Bucket, "test.medium",    MediumTestF)
        getObject(Bucket, "test.large",     LargeTestF)

        # Get Object with Metadata Test
        getObjectWithMetadata(Bucket, "test.simple.meta", SmallTestF, MetadataMap)
        getObjectWithMetadata(Bucket, "test.large.meta", LargeTestF, MetadataMap)

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
        time.sleep(3)
        listObject(Bucket, "", 0)
    
        # Multiple Page List Object Test
        putDummyObjects(Bucket, "list/", 35, SmallTestF)
        time.sleep(3)
        pageListBucket(Bucket, "list/", 35, 10)
    
        # Multiple Delete
        multiDelete(Bucket, "list/", 10)

        # GET-PUT ACL
        setBucketAcl(Bucket, "private")
        setBucketAcl(Bucket, "public-read")
        setBucketAcl(Bucket, "public-read-write")

    except Exception, e:
        print traceback.format_exc()
        sys.exit(-1)

def init(signVer, Host, Port):
    global s3
    if signVer == "v4":
        econfig = Config(signature_version='s3v4')
    else:
        econfig = Config(signature_version='s3')
    s3 = boto3.client(
            's3',
            region_name = 'us-east-1',
            use_ssl = False,
            endpoint_url = "http://%s:%d/" % (Host, Port),
            aws_access_key_id = AccessKeyId,
            aws_secret_access_key = SecretAccessKey,
            config = econfig
            )

def createBucket(bucketName):
    print "===== Create Bucket [%s] Start =====" % bucketName
    s3.create_bucket(Bucket = bucketName)
    print "===== Create Bucket End ====="
    print 

def putObject(bucketName, key, path):
    print "===== Put Object [%s/%s] Start =====" % (bucketName, key)

    GB = 1024 ** 3
    # Ensure that multipart uploads only happen if the size of a transfer
    # is larger than S3's size limit for nonmultipart uploads, which is 5 GB.
    config = TransferConfig(multipart_threshold=5 * GB)

    s3.upload_file(path, bucketName, key,
            Config=config)
    if not doesFileExist(bucketName, key):
        raise ValueError("Put Object [%s/%s] Failed!" % (bucketName, key))
    print "===== Put Object End ====="
    print

def putObjectWithMeta(bucketName, key, path, meta_map):
    print "===== Put Object [%s/%s] with Metadata Start =====" % (bucketName, key)

    GB = 1024 ** 3
    # Ensure that multipart uploads only happen if the size of a transfer
    # is larger than S3's size limit for nonmultipart uploads, which is 5 GB.
    config = TransferConfig(multipart_threshold=5 * GB)

    s3.upload_file(path, bucketName, key,
            ExtraArgs={'Metadata' : meta_map},
            Config=config)
    if not doesFileExist(bucketName, key):
        raise ValueError("Put Object [%s/%s] with Metadata Failed!" % (bucketName, key))
    print "===== Put Object with Metadata End ====="
    print

def headObject(bucketName, key, path):
    print "===== Head Object [%s/%s] Start =====" % (bucketName, key)
    obj = s3.head_object(
            Bucket = bucketName,
            Key = key)
    size = os.path.getsize(path)
    context = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(partial(f.read, 4096), ''):
            context.update(chunk)
    md5sum = context.hexdigest()
    etag = obj['ETag'][1:-1]

    print "ETag: %s, Size: %d" % (etag, obj['ContentLength'])
    if etag != md5sum or obj['ContentLength'] != size:
        raise ValueError("Metadata [%s/%s] NOT Match, Size: %d, MD5: %s" % (bucketName, key, size, md5sum))
    print "===== Head Object End ====="
    print

def getObject(bucketName, key, path):
    print "===== Get Object [%s/%s] Start =====" % (bucketName, key)
    obj = s3.get_object(
            Bucket = bucketName,
            Key = key)
    file = open(path)
    if not doesFileMatch(file, obj['Body']):
        raise ValueError("Content NOT Match!")
    print "===== Get Object End ====="
    print

def getObjectWithMetadata(bucketName, key, path, meta_map):
    print "===== Get Object [%s/%s] with Metadata Start =====" % (bucketName, key)
    obj = s3.get_object(
            Bucket = bucketName,
            Key = key)
    file = open(path)
    
    if meta_map != obj['Metadata']:
        raise ValueError("Metadata NOT Match!")
    if not doesFileMatch(file, obj['Body']):
        raise ValueError("Content NOT Match!")
    print "===== Get Object with Metadata End ====="
    print

def getNotExist(bucketName, key):
    print "===== Get Not Exist Object [%s/%s] Start =====" % (bucketName, key)
    try:
        obj = s3.get_object(
                Bucket = bucketName,
                Key = key)
        raise ValueError("Should NOT Exist!")
    except Exception, not_found:
        pass
    print "===== Get Not Exist Object End ====="
    print

def rangeObject(bucketName, key, path, start, end):
    print "===== Range Get Object [%s/%s] (%d-%d) Start =====" % (bucketName, key, start, end)
    obj = s3.get_object(
            Bucket = bucketName,
            Key = key,
            Range = 'bytes=%d-%d' % (start, end))
    file = FileChunkIO(path, 'rb', offset=start, bytes=end - start + 1)
    if not doesFileMatch(file, obj['Body']):
        raise ValueError("Content NOT Match!")
    print "===== Range Get Object End ====="
    print

def copyObject(bucketName, src, dst):
    print "===== Copy Object [%s/%s] -> [%s/%s] Start =====" % (bucketName, src, bucketName, dst)
    s3.copy({'Bucket' : bucketName, 'Key' : src}, bucketName, dst)
    print "===== Copy Object End ====="
    print

def listObject(bucketName, prefix, expected):
    print "===== List Objects [%s/%s*] Start =====" % (bucketName, prefix)
    obj = s3.list_objects(Bucket = bucketName, Prefix = prefix)
    count = 0
    if 'Contents' in obj:
        obj_list = obj['Contents']
        for obj in obj_list:
            if doesFileExist(bucketName, obj['Key']):
                print "%s \t Size: %d" % (obj['Key'], obj['Size'])
                count = count + 1
    if expected >= 0 and count != expected:
        raise ValueError("Number of Objects NOT Match!")
    print "===== List Objects End ====="
    print

def deleteAllObjects(bucketName):
    print "===== Delete All Objects [%s] Start =====" % bucketName
    obj = s3.list_objects(Bucket = bucketName)
    obj_list = obj['Contents']
    for obj in obj_list:
        s3.delete_object(Bucket = bucketName, Key = obj['Key'][1:])
    print "===== Delete All Objects End ====="
    print

def putDummyObjects(bucketName, prefix, total, holder):
    for i in range(0, total):
        s3.upload_file(holder, bucketName, prefix+str(i))

def pageListBucket(bucketName, prefix, total, pageSize):
    print "===== Multiple Page List Objects [%s/%s*] %d Objs @%d Start =====" % (bucketName, prefix, total, pageSize)
    marker = ""
    count = 0
    while True:
        print "===== Page ====="
        res = s3.list_objects(
                Bucket = bucketName,
                Prefix = prefix,
                MaxKeys = pageSize,
                Marker = marker)
        for obj in res['Contents']:
            count = count + 1
            print "%s \t Size: %d \t Count: %d" % (obj['Key'], obj['Size'], count)
        if not res['IsTruncated']:
            break
        else:
            marker = res['NextMarker']
    print "===== End ====="
    if count != total:
        raise ValueError("Number of Objects NOT Match!")
    print "===== Multiple Page List Objects End ====="
    print

def multiDelete(bucketName, prefix, total):
    print "===== Multiple Delete Objects [%s/%s] Start =====" % (bucketName, prefix)
    delKeyList = []
    for i in range(0, total):
        delKeyList.append({'Key' : prefix+str(i)})
    res = s3.delete_objects(
            Bucket = bucketName,
            Delete = {'Objects' : delKeyList})
    for obj in res['Deleted']:
        print "Deleted %s/%s" % (bucketName, obj['Key'])
    if len(res['Deleted']) != total:
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
    s3.put_bucket_acl(
            Bucket = bucketName,
            ACL = permission)
    res = s3.get_bucket_acl(
            Bucket = bucketName)
    print "Owner ID: S3Owner [name=%s,id=%s]" % (res['Owner']['DisplayName'], res['Owner']['ID'])
    list = []
    for grant in res['Grants']:
        print "Grantee: %s \t Permissions: %s" % (grant['Grantee']['URI'], grant['Permission'])
        list.append(grant['Permission'])
    if not all(x in list for x in checkList):
        raise ValueError("ACL NOT Match!")
    print "===== Set Bucket ACL End ====="
    print

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

main()
