import boto3
from botocore.client import Config
from boto3.s3.transfer import TransferConfig

class Boto3Client:

    s3 = None
    bucket_name = ""

    def __init__(self, sign_ver, host, port, access_key, secret_key):
        if sign_ver == "v4":
            econfig = Config(signature_version='s3v4')
        else:
            econfig = Config(signature_version='s3')
        self.s3 = boto3.client(
                's3',
                region_name = 'us-east-1',
                use_ssl = False,
                endpoint_url = "http://%s:%d/" % (host, port),
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                config = econfig
                )

    def set_bucket(self, bucket_name):
        self.bucket_name = bucket_name

    def target_bucket(self, bucket_name=None):
        if bucket_name == None:
            return self.bucket_name
        else:
            return bucket_name

    def do_create_bucket(self, bucket_name):
        self.s3.create_bucket(Bucket=bucket_name)

    def do_put_object(self, key, path, meta_map=None, bucket_name=None):
        GB = 1024 ** 3
        # Ensure that multipart uploads only happen if the size of a transfer
        # is larger than S3's size limit for nonmultipart uploads, which is 5 GB.
        config = TransferConfig(multipart_threshold=5 * GB)

        bucket_name = self.target_bucket(bucket_name)

        extra_args = None
        if meta_map != None:
            extra_args = {'Metadata' : meta_map}

        self.s3.upload_file(path, bucket_name, key,
                ExtraArgs = extra_args,
                Config=config)

    def do_head_object(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        obj = self.s3.head_object(
                Bucket = bucket_name,
                Key = key)

        return {'etag' : obj['ETag'][1:-1], 'size' : obj['ContentLength']}

    def do_get_object(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        obj = self.s3.get_object(
                Bucket = bucket_name,
                Key = key)
        
        return {'content' : obj.get('Body', None), 'meta' : obj.get('Metadata', None)}

    def do_range_object(self, key, start, end, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        obj = self.s3.get_object(
                Bucket = bucket_name,
                Key = key,
                Range = 'bytes=%d-%d' % (start, end))

        return {'content' : obj.get('Body', None), 'meta' : obj.get('Metadata', None)}

    def do_copy_object(self, src, dst, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)
        self.s3.copy({'Bucket' : bucket_name, 'Key' : src}, bucket_name, dst)

    def do_list_object(self, prefix, bucket_name=None, max_keys=None, marker=""):
        bucket_name = self.target_bucket(bucket_name)

        if max_keys == None:
            obj = self.s3.list_objects(
                    Bucket = bucket_name,
                    Prefix = prefix)
        else:
            obj = self.s3.list_objects(
                    Bucket = bucket_name,
                    Prefix = prefix,
                    Marker = marker,
                    MaxKeys = max_keys)

        ret_list = []

        obj_list = obj.get('Contents', [])
        next_marker = obj.get('NextMarker', "")

        for obj in obj_list:
            ret_list.append({'key' : obj['Key'], 'size' : obj['Size']})

        return {'objects' : ret_list, 'marker' : next_marker}
       
    def do_delete_object(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        self.s3.delete_object(
                Bucket = bucket_name,
                Key = key)

    def do_multidelete_object(self, keys, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)
        del_list = []

        for key in keys:
            del_list.append({'Key' : key})

        res = self.s3.delete_objects(
                Bucket = bucket_name,
                Delete = {'Objects' : del_list})

        deleted_list = []

        for obj in res['Deleted']:
            deleted_list.append({'key' : obj['Key']})

        return {'deleted' : deleted_list}

    def do_put_bucket_acl(self, acl, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        self.s3.put_bucket_acl(
                Bucket = bucket_name,
                ACL = acl)

    def do_get_bucket_acl(self, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)

        res = self.s3.get_bucket_acl(
                Bucket = bucket_name)

        owner = {'name' : res['Owner']['DisplayName'], 'id' : res['Owner']['ID']}

        grant_list = []
        for grant in res['Grants']:
            grant_list.append({'uri' : grant['Grantee']['URI'], 'permission' : grant['Permission']})

        return {'owner' : owner, 'grants' : grant_list}


    def check_object_exist(self, key, bucket_name=None):
        bucket_name = self.target_bucket(bucket_name)
        try:
            self.do_head_object(key, bucket_name)
            return True
        except:
            return False
        

