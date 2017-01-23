#!/usr/bin/python
# coding: utf8

import boto
from boto.s3.connection import S3Connection, OrdinaryCallingFormat

class BotoClient:

    s3 = None
    bucket_name = ""

    def __init__(self, sign_ver, host, port, access_key, secret_key):
        if sign_ver == "v4":
            boto.config.add_section('s3')
            boto.config.set('s3', 'use-sigv4', 'True')
        self.s3 = S3Connection(
                access_key,
                secret_key,
                host = 's3.amazonaws.com',
                proxy = host,
                proxy_port = port,
                calling_format = OrdinaryCallingFormat(),
                is_secure = False)

    def set_bucket(self, bucket_name):
        self.bucket_name = bucket_name

    def target_bucket(self, bucket_name=None):
        if bucket_name == None:
            return self.bucket_name
        else:
            return bucket_name

    def do_create_bucket(self, bucket_name):
        self.s3.create_bucket(bucket_name)

    def do_put_object(self, key, path, meta_map=None, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        obj = bucket.new_key(key)
        
        if meta_map != None:
            for mkey, val in meta_map.items():
                obj.set_metadata(mkey, val)
        obj.set_contents_from_filename(path)

    def do_head_object(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        obj = bucket.get_key(key)

        return {'etag' : obj.etag[1:-1], 'size' : obj.size}

    def do_get_object(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        obj = bucket.get_key(key)

        return {'content' : obj, 'meta' : obj.metadata}

    def do_range_object(self, key, start, end, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        obj = bucket.get_key(key)
        obj.open_read(headers = {'Range': 'bytes=%d-%d' % (start, end)})

        return {'content' : obj, 'meta' : obj.metadata}

    def do_copy_object(self, src, dst, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        bucket.copy_key(dst, bucket_name, src)

    def do_list_object(self, prefix, bucket_name=None, max_keys=None, marker=""):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)

        ret_list = []
        next_marker = ""

        if max_keys == None:
            for obj in bucket.list():
                ret_list.append({'key' : obj.key, 'size' : obj.size})
        else:
            res = bucket.get_all_keys(
                    prefix = prefix,
                    max_keys = max_keys,
                    marker = marker)
            for obj in res:
                ret_list.append({'key' : obj.key, 'size' : obj.size})
            next_marker = res.next_marker

        return {'objects' : ret_list, 'marker' : next_marker}
       
    def do_delete_object(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        bucket.delete_key(key)

    def do_multidelete_object(self, keys, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)
        del_list = []

        for key in keys:
            del_list.append(key)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        res = bucket.delete_keys(del_list)
        
        deleted_list = []

        for obj in res.deleted:
            deleted_list.append({'key' : obj.key})

        return {'deleted' : deleted_list}

    def do_put_bucket_acl(self, acl, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        bucket.set_acl(acl)

    def do_get_bucket_acl(self, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        bucket = self.s3.get_bucket(bucket_name, validate=False)
        res = bucket.get_acl()

        owner = {'name' : res.owner.display_name, 'id' : res.owner.id}

        grant_list = []
        for grant in res.acl.grants:
            grant_list.append({'uri' : grant.uri, 'permission' : grant.permission})

        return {'owner' : owner, 'grants' : grant_list}

    def check_object_exist(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)
        try:
            self.do_head_object(key, bucket_name)
            return True
        except:
            return False
        
