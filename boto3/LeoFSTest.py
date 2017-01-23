#!/usr/bin/python
# coding: utf8

import traceback
import sys
import time

sys.path.insert(0, "../python_common/")

from utils import *
from leofs_tester import *
from boto3client import *

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

tester = None

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
        init(SignVer, Host, Port, AccessKeyId, SecretAccessKey)
        tester.createBucket(Bucket)

        # Put Object Test
        tester.putObject(Bucket, "test.simple",    SmallTestF)
        tester.putObject(Bucket, "test.medium",    MediumTestF)
        tester.putObject(Bucket, "test.large",     LargeTestF)
    
        # Put Object with Metadata Test
        tester.putObjectWithMeta(Bucket, "test.simple.meta", SmallTestF, MetadataMap)
        tester.putObjectWithMeta(Bucket, "test.large.meta", LargeTestF, MetadataMap)

#        # Multipart Upload Object Test
#        mpObject(Bucket, "test.simple.mp",  SmallTestF)
#        mpObject(Bucket, "test.large.mp",   LargeTestF)
  
        # Head Object Test
        tester.headObject(Bucket, "test.simple",   SmallTestF)
        tester.headObject(Bucket, "test.large",    LargeTestF)
#        headObject(Bucket, "test.simple.mp",SmallTestF)
    
        # Get Object Test
        tester.getObject(Bucket, "test.simple",    SmallTestF)
        tester.getObject(Bucket, "test.medium",    MediumTestF)
        tester.getObject(Bucket, "test.large",     LargeTestF)
#        tester.getObject(Bucket, "test.simple.mp", SmallTestF)
#        tester.getObject(Bucket, "test.large.mp",  LargeTestF)
    
        # Get Object Again (Cache) Test
        tester.getObject(Bucket, "test.simple",    SmallTestF)
        tester.getObject(Bucket, "test.medium",    MediumTestF)
        tester.getObject(Bucket, "test.large",     LargeTestF)

        # Get Object with Metadata Test
        tester.getObjectWithMetadata(Bucket, "test.simple.meta", SmallTestF, MetadataMap)
        tester.getObjectWithMetadata(Bucket, "test.large.meta", LargeTestF, MetadataMap)

        # Get Not Exist Object Test
        tester.getNotExist(Bucket, "test.noexist")
    
        # Range Get Object Test
        tester.rangeObject(Bucket, "test.simple",      SmallTestF, 1, 4)
        tester.rangeObject(Bucket, "test.large",       LargeTestF, 1048576, 10485760)
#        tester.rangeObject(Bucket, "test.simple.mp",   SmallTestF, 1, 4)
#        tester.rangeObject(Bucket, "test.large.mp",    LargeTestF, 1048576, 10485760)
    
        # Copy Object Test
        tester.copyObject(Bucket, "test.simple", "test.simple.copy")
        tester.getObject(Bucket, "test.simple.copy", SmallTestF)
    
        # List Object Test
        tester.listObject(Bucket, "", -1)
    
        # Delete All Object Test
        tester.deleteAllObjects(Bucket)
        time.sleep(3)
        tester.listObject(Bucket, "", 0)
    
        # Multiple Page List Object Test
        tester.putDummyObjects(Bucket, "list/", 35, SmallTestF)
        time.sleep(3)
        tester.pageListBucket(Bucket, "list/", 35, 10)
    
        # Multiple Delete
        tester.multiDelete(Bucket, "list/", 10)

        # GET-PUT ACL
        tester.setBucketAcl(Bucket, "private")
        tester.setBucketAcl(Bucket, "public-read")
        tester.setBucketAcl(Bucket, "public-read-write")

    except Exception, e:
        print traceback.format_exc()
        sys.exit(-1)

def init(signVer, Host, Port, AccessKeyId, SecretAccessKey):
    global tester
    boto3cli = Boto3Client(signVer, Host, Port, AccessKeyId, SecretAccessKey)
    tester = LeoFSTester(boto3cli)

main()
