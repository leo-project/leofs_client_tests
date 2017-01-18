import os
import hashlib
from functools import partial
from filechunkio import FileChunkIO

from utils import *

class LeoFSTester:

    _client = None

    def __init__(self, client):
        self._client = client

    def createBucket(self, bucketName):
        print "===== Create Bucket [%s] Start =====" % bucketName
        self._client.do_create_bucket(bucketName)
        print "===== Create Bucket End ====="
        print 

    def putObject(self, bucketName, key, path):
        print "===== Put Object [%s/%s] Start =====" % (bucketName, key)

        self._client.do_put_object(key, path, None, bucketName)
        if not self._client.check_object_exist(key, bucketName):
            raise ValueError("Put Object [%s/%s] Failed!" % (bucketName, key))
        print "===== Put Object End ====="
        print

    def putObjectWithMeta(self, bucketName, key, path, meta_map):
        print "===== Put Object [%s/%s] with Metadata Start =====" % (bucketName, key)

        self._client.do_put_object(key, path, meta_map, bucketName)
        if not self._client.check_object_exist(key, bucketName):
            raise ValueError("Put Object [%s/%s] with Metadata Failed!" % (bucketName, key))

        print "===== Put Object with Metadata End ====="
        print

    def headObject(self, bucketName, key, path):
        print "===== Head Object [%s/%s] Start =====" % (bucketName, key)

        obj = self._client.do_head_object(key, bucketName)

        size = os.path.getsize(path)
        context = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(partial(f.read, 4096), ''):
                context.update(chunk)
        md5sum = context.hexdigest()

        etag = obj['etag']
        length = obj['size']

        print "ETag: %s, Size: %d" % (etag, length)
        if etag != md5sum or length != size:
            raise ValueError("Metadata [%s/%s] NOT Match, Size: %d, MD5: %s" % (bucketName, key, size, md5sum))
        print "===== Head Object End ====="
        print

    def getObject(self, bucketName, key, path):
        print "===== Get Object [%s/%s] Start =====" % (bucketName, key)
        obj = self._client.do_get_object(key, bucketName)

        file = open(path)
        if not doesFileMatch(file, obj['content']):
            raise ValueError("Content NOT Match!")
        print "===== Get Object End ====="
        print

    def getObjectWithMetadata(self, bucketName, key, path, meta_map):
        print "===== Get Object [%s/%s] with Metadata Start =====" % (bucketName, key)
        obj = self._client.do_get_object(key, bucketName)

        file = open(path)
        
        if meta_map != obj['meta']:
            raise ValueError("Metadata NOT Match!")
        if not doesFileMatch(file, obj['content']):
            raise ValueError("Content NOT Match!")
        print "===== Get Object with Metadata End ====="
        print

    def getNotExist(self, bucketName, key):
        print "===== Get Not Exist Object [%s/%s] Start =====" % (bucketName, key)
        try:
            obj = self._client.do_get_object(key, bucketName)
            raise ValueError("Should NOT Exist!")
        except Exception, not_found:
            pass
        print "===== Get Not Exist Object End ====="
        print

    def rangeObject(self, bucketName, key, path, start, end):
        print "===== Range Get Object [%s/%s] (%d-%d) Start =====" % (bucketName, key, start, end)
        obj = self._client.do_range_object(key, start, end, bucketName)
        file = FileChunkIO(path, 'rb', offset=start, bytes=end - start + 1)
        if not doesFileMatch(file, obj['content']):
            raise ValueError("Content NOT Match!")
        print "===== Range Get Object End ====="
        print

    def copyObject(self, bucketName, src, dst):
        print "===== Copy Object [%s/%s] -> [%s/%s] Start =====" % (bucketName, src, bucketName, dst)
        self._client.do_copy_object(src, dst, bucketName)
        print "===== Copy Object End ====="
        print

    def listObject(self, bucketName, prefix, expected):
        print "===== List Objects [%s/%s*] Start =====" % (bucketName, prefix)
        obj = self._client.do_list_object(prefix, bucketName)
        obj_list = obj['objects']
        count = 0
        for obj in obj_list:
            if self._client.check_object_exist(obj['key'], bucketName):
                print "%s \t Size: %d" % (obj['key'], obj['size'])
                count = count + 1
        if expected >= 0 and count != expected:
            raise ValueError("Number of Objects NOT Match!")
        print "===== List Objects End ====="
        print

    def deleteAllObjects(self, bucketName):
        print "===== Delete All Objects [%s] Start =====" % bucketName
        obj = self._client.do_list_object("", bucketName)
        obj_list = obj['objects']
        count = 0
        for obj in obj_list:
            self._client.do_delete_object(obj['key'], bucketName)
        print "===== Delete All Objects End ====="
        print

    def putDummyObjects(self, bucketName, prefix, total, holder):
        for i in range(0, total):
            self._client.do_put_object(prefix+str(i), holder, None, bucketName)

    def pageListBucket(self, bucketName, prefix, total, pageSize):
        print "===== Multiple Page List Objects [%s/%s*] %d Objs @%d Start =====" % (bucketName, prefix, total, pageSize)
        marker = ""
        count = 0
        while True:
            print "===== Page ====="
            res = self._client.do_list_object(prefix, bucketName, pageSize, marker)
            for obj in res['objects']:
                count = count + 1
                print "%s \t Size: %d \t Count: %d" % (obj['key'], obj['size'], count)
            if res['marker'] == "":
                break
            else:
                marker = res['marker']

        print "===== End ====="
        if count != total:
            raise ValueError("Number of Objects NOT Match!")
        print "===== Multiple Page List Objects End ====="
        print

    def multiDelete(self, bucketName, prefix, total):
        print "===== Multiple Delete Objects [%s/%s] Start =====" % (bucketName, prefix)
        delKeyList = []
        for i in range(0, total):
            delKeyList.append(prefix+str(i))
        
        res = self._client.do_multidelete_object(delKeyList, bucketName)

        for obj in res['deleted']:
            print "Deleted %s/%s" % (bucketName, obj['key'])
        if len(res['deleted']) != total:
            raise ValueError("Number of Objects NOT Match!")
        print "===== Multiple Delete Objects End ====="
        print

    def setBucketAcl(self, bucketName, permission):
        print "===== Set Bucket ACL [%s] (%s) Start =====" % (bucketName , permission)
        if permission == "private":
            checkList = ["FULL_CONTROL"]
        elif permission == "public-read":
            checkList = ["READ", "READ_ACP"]
        elif permission == "public-read-write":
            checkList = ["READ", "READ_ACP", "WRITE", "WRITE_ACP"]
        else:
            raise ValueError("Invalid Permission!")
        
        self._client.do_put_bucket_acl(permission, bucketName)

        res = self._client.do_get_bucket_acl(bucketName)

        print "Owner ID: S3Owner [name=%s,id=%s]" % (res['owner']['name'], res['owner']['id'])
        list = []
        for grant in res['grants']:
            print "Grantee: %s \t Permissions: %s" % (grant['uri'], grant['permission'])
            list.append(grant['permission'])
        if not all(x in list for x in checkList):
            raise ValueError("ACL NOT Match!")
        print "===== Set Bucket ACL End ====="
        print

