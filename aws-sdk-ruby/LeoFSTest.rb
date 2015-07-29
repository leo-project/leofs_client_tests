require "aws-sdk-core"
require "aws-sdk-resources"
require "content_type"
require 'logger'

Host    = "localhost"
Port    = 8080

AccessKeyId     = "05236"
SecretAccessKey = "802562235"
SignVer         = "v4"

Bucket      = "testr"
TempData    = "../temp_data/"

SmallTestF  = TempData + "testFile"
LargeTestF  = TempData + "testFile.large"

def main()
    signVer = SignVer
    if ARGV.length > 0
        signVer = ARGV[0]
    end
    begin
        init(signVer)
        createBucket(Bucket)

        # Put Object Test
        putObject(Bucket, "test.simple",    SmallTestF)
        putObject(Bucket, "test.large",     LargeTestF)

        # Multipart Upload Object Test
        mpObject(Bucket, "test.simple.mp",  SmallTestF)
        mpObject(Bucket, "test.large.mp",   LargeTestF)

        # Head Object Test
        headObject(Bucket, "test.simple",   SmallTestF)
        headObject(Bucket, "test.simple.mp",SmallTestF)
        headObject(Bucket, "test.large",    LargeTestF)

        # Get Object Test
        getObject(Bucket, "test.simple",    SmallTestF)
        getObject(Bucket, "test.simple.mp", SmallTestF)
        getObject(Bucket, "test.large",     LargeTestF)
        getObject(Bucket, "test.large.mp",  LargeTestF)

        # Get Not Exist Object Test
        getNotExist(Bucket, "test.noexist")

        # Range Get Object Test
        rangeObject(Bucket, "test.simple",      SmallTestF, 1, 4)
        rangeObject(Bucket, "test.simple.mp",   SmallTestF, 1, 4)
        rangeObject(Bucket, "test.large",       LargeTestF, 1048576, 10485760)
        rangeObject(Bucket, "test.large.mp",    LargeTestF, 1048576, 10485760)

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
    rescue
        p $!
        exit(-1)
    end
end

def init(signVer) 

    options = {
        endpoint: "http://"+Host+":"+Port.to_s,
        region: "us-west-2",
        access_key_id: AccessKeyId,
        secret_access_key: SecretAccessKey,
        http_proxy: "http://"+Host+":"+Port.to_s,
        compute_checksums: false,
        force_path_style: true,
    }

    if signVer.eql? "v4"
        options[:signature_version] = "v4"
    else
        options[:signature_version] = "s3"
    end
    $s3 = Aws::S3::Client.new(options)
end

def createBucket(bucketName)
    printf("===== Create Bucket [%s] Start =====\n", bucketName)
    $s3.create_bucket(bucket: bucketName)
    printf("===== Create Bucket End =====\n")
    printf("\n")
end

def putObject(bucketName, key, path)
    printf("===== Put Object [%s/%s] Start =====\n", bucketName, key)
    file = open(path)
    $s3.put_object(
        bucket: bucketName,
        key:    key,
        body:   file
    )
    if !doesFileExist(bucketName, key)
        raise sprintf("Put Object [%s/%s] Failed!\n", bucketName, key)
    end
    printf("===== Put Object End =====\n")
    printf("\n")
end

def mpObject(bucketName, key, path)
    printf("===== Multipart Upload Object [%s/%s] Start =====\n", bucketName, key)
    s3r = Aws::S3::Resource.new(client: $s3)
    obj = s3r.bucket(bucketName).object(key)
    obj.upload_file(path)
    if !doesFileExist(bucketName, key)
        raise sprintf("Multipart Upload Object [%s/%s] Failed!\n", bucketName, key)
    end
    printf("===== Multipart Upload Object End =====\n")
    printf("\n")
end

def headObject(bucketName, key, path)
    printf("===== Head Object [%s/%s] Start =====\n", bucketName, key)
    res = $s3.head_object(
        bucket: bucketName,
        key:    key
    )
    hash = Digest::MD5.file path
    md5sum = hash.hexdigest
    size = File.size(path)
    etag = res.etag.gsub("\"","")
    printf("ETag: %s, Size: %d\n", etag, res.content_length)
    if (etag != md5sum || res.content_length != size)
        raise sprintf("Metadata [%s/%s] NOT Match, Size: %d, MD5: %s\n", bucketName, key, size, md5sum)
    end
    printf("===== Head Object End =====\n")
    printf("\n")
end

def getObject(bucketName, key, path)
    printf("===== Get Object [%s/%s] Start =====\n", bucketName, key)
    res = $s3.get_object(
        bucket: Bucket,
        key:    key
    )
    file = open(path)
    if !doesFileMatch(file, res.body)
        raise "Content NOT Match!\n";
    end

    printf("===== Get Object End =====\n")
    printf("\n")
end

def getNotExist(bucketName, key)
    printf("===== Get Not Exist Object [%s/%s] Start =====\n", bucketName, key);
    begin
        $s3.get_object(
            bucket: bucketName,
            key:    key
        )
        raise "Should NOT Exist!\n"
    rescue  Aws::S3::Errors::NoSuchKey
    end
    printf("===== Get Not Exist Object End =====\n")
    printf("\n")
end

def rangeObject(bucketName, key, path, start, end_)
    printf("===== Range Get Object [%s/%s] (%d-%d) Start =====\n", bucketName, key, start, end_)
    res = $s3.get_object(
        bucket: Bucket,
        key:    key,
        range:  "bytes="+start.to_s+"-"+end_.to_s
    )
    file = open(path)
    file.seek(start)
    if !doesFileMatch(file, res.body, end_ - start + 1)
        raise "Content NOT Match!\n";
    end
    printf("===== Range Get Object End =====\n")
    printf("\n")
end

def copyObject(bucketName, src, dst)
    printf("===== Copy Object [%s/%s] -> [%s/%s] Start =====\n",
           bucketName, src, bucketName, dst)
    $s3.copy_object(
        copy_source:    bucketName+"/"+src,
        bucket:         bucketName,
        key:            dst
    )
    printf("===== Copt Object End =====\n")
    printf("\n")
end

def listObject(bucketName, prefix, expected)
    printf("===== List Objects [%s/%s*] Start =====\n", bucketName, prefix)
    res = $s3.list_objects(
        bucket: bucketName,
        prefix: prefix
    )
    count = 0
    res.contents.each do |obj|
        if doesFileExist(bucketName, obj.key)
            printf("%s \t Size: %d\n", obj.key, obj.size)
            count = count + 1
        end
    end
    if expected >= 0 && count != expected
        raise "Number of Objects NOT Match!\n"
    end
    printf("===== List Objects End =====\n")
    printf("\n")
end

def deleteAllObjects(bucketName)
    printf("===== Delete All Objects [%s] Start =====\n", bucketName)
    res = $s3.list_objects(
        bucket: bucketName,
        prefix: ""
    )
    res.contents.each do |obj|
        $s3.delete_object(
            bucket: bucketName,
            key:    obj.key
        )
    end
    printf("===== Delete All Objects End =====\n")
    printf("\n")
end

def putDummyObjects(bucketName, prefix, total, holder)
    for i in 0..total-1  
        file = open(holder)
        $s3.put_object(
            bucket: bucketName,
            key:    prefix+i.to_s,
            body:   file
        )
    end
end

def pageListBucket(bucketName, prefix, total, pageSize) 
    printf("===== Multiple Page List Objects [%s/%s*] %d Objs @%d Start =====\n", bucketName, prefix, total, pageSize)
    marker = ""
    count = 0
    while true
        res = $s3.list_objects(
            bucket:     bucketName,
            prefix:     prefix,
            max_keys:   pageSize,
            marker:     marker
        )
        printf("===== Page =====\n")
        res.contents.each do |obj|
            count = count + 1
            printf("%s \t Size: %d \t Count: %d\n", obj.key, obj.size, count)
        end
        if !res.is_truncated
            break
        else
            marker = res.next_marker
        end
    end
    printf("===== End =====\n")
    if count != total
        raise "Number of Objects NOT Match!\n"
    end
    printf("===== Multiple Page List Objects End =====\n")
    printf("\n")
end

def multiDelete(bucketName, prefix, total)
    printf("===== Multiple Delete Objects [%s/%s] Start =====\n", bucketName, prefix)
    delKeyList = []
    for i in 0..total-1
        delKeyList << {key: prefix+i.to_s}
    end
    res = $s3.delete_objects(
        bucket: bucketName,
        delete: {
            objects: delKeyList
        }
    )
    res.deleted.each do |obj|
        printf("Deleted %s/%s\n", bucketName, obj.key)
    end
    if res.deleted.length != total
        raise "Number of Objects NOT Match!\n"
    end

    printf("===== Multiple Delete Objects End =====\n")
    printf("\n")
end

def setBucketAcl(bucketName, permission)
    printf("===== Set Bucket ACL [%s] (%s) Start =====\n", bucketName , permission)
    checkList = []
    if permission == "private"
        checkList << "FULL_CONTROL"
    elsif permission == "public-read"
        checkList << "READ" << "READ_ACP"
    elsif permission == "public-read-write"
        checkList << "READ" << "READ_ACP" << "WRITE" << "WRITE_ACP"
    else
        raise "Invalid Permission!\n"
    end
    $s3.put_bucket_acl(
        bucket: bucketName,
        acl:    permission
    )
    res = $s3.get_bucket_acl(
        bucket: bucketName
    )
    printf("Owner ID: S3Owner [name=%s,id=%s]\n", res.owner.display_name, res.owner.id)
    list = []
    res.grants.each do |grant|
        printf("Grantee: %s \t Permissions: %s\n", grant.grantee.uri, grant.permission)
        list << grant.permission
    end
    if list != checkList
        raise "ACL NOT Match!\n"
    end
    printf("===== Set Bucket ACL End =====\n")
    printf("\n")
end

def doesFileMatch(io1, io2, size = -1)
    readSize = 4096
    remain = size
    while true
        if size > 0
            if (remain < readSize)
                readSize = remain 
            end
            remain = remain - readSize
        end
            
        b1 = io1.read(readSize)
        b2 = io2.read(readSize)

        if b1 == nil
            return b2 == nil
        elsif b2 == nil
            return b1 == nil
        elsif b1 != b2
            return false
        elsif remain == 0
            return true
        end
    end
end

def doesFileExist(bucketName, key)
    begin
        res = $s3.head_object(
            bucket: bucketName,
            key:    key
        )
        return true
    rescue
        return false
    end
end

main()
