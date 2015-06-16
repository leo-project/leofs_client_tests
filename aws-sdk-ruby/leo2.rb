## This code supports "aws-sdk v2.0.27"

require "aws-sdk-core"
require "aws-sdk-resources"
require "content_type"

# set your s3 key and variable
Endpoint = "http://localhost:8080"
AccessKeyId = "05236"
SecretAccessKey = "802562235"
Filename = "testFile s"
ChunkSize = 5 * 1024 * 1024 ## 5 MB chunk size
Bucket = "test" + rand(99999).to_s ## Dynamic BucketName
LargeObjSize = 52428800
LargeFilePath = "../temp_data/testFile.large"

Aws.config = {
    access_key_id: AccessKeyId,
    secret_access_key: SecretAccessKey,
    endpoint: Endpoint,
    region: 'ap-northeast-1',
    http_proxy: Endpoint,
    signature_version: 's3',
    force_path_style: true
}

s3 = Aws::S3::Client.new

begin
    puts "Bucket name: #{Bucket}"
    # Create bucket
    s3.create_bucket(bucket: Bucket)
    puts "Bucket Created Successfully\n\n"

    # Get bucket
    bucket = s3.get_bucket_location(bucket: Bucket)
    puts "Get Bucket Successfully\n\n"

    # PUT Object
    file_path = "../temp_data/" + Filename
    fileObject = open(file_path)

    # PUT an object using single-part method and the obj-name is "bucket-name"
    s3.put_object(bucket: Bucket, body: fileObject, key: Filename + ".single", content_type: fileObject.content_type)

    # PUT an object using multi-part method
    fileObject.rewind
    puts "File is being upload:"
    counter = 0
    parts = []
    resp1 = s3.create_multipart_upload(bucket: Bucket, key: Filename, content_type: fileObject.content_type)
    puts "upload_id: #{resp1.upload_id}"
    while !fileObject.eof?
        counter += 1
        puts "Chunk number: #{counter}"
        body = fileObject.read ChunkSize
        resp2 = s3.upload_part(body: body, bucket: Bucket, key: Filename, part_number: counter, upload_id: resp1.upload_id)
        parts.push({etag: resp2.etag.gsub("\"", ""), part_number: counter.to_i})
    end
    status = s3.complete_multipart_upload(bucket: Bucket, key: Filename, upload_id: resp1.upload_id,
                                          multipart_upload: {parts: parts})
    puts "File Uploaded Successfully\n\n"

    if !File.exist?(LargeFilePath)
        File.open(LargeFilePath, "wb") do |f|
            f.write(Random.new.bytes(LargeObjSize))
        end
    end

    puts "Uploading Single Part Large Object"
    # Put Single-Part Large Object
    largeFileObject = open(LargeFilePath)
    s3.put_object(bucket: Bucket, body: largeFileObject, key: Filename + ".large.one", content_type: largeFileObject.content_type)

    # Put Multi-Part Large Object
    puts "Uploading Multi Part Large Object"
    largeFileObject.rewind
    resp1 = s3.create_multipart_upload(bucket: Bucket, key: Filename + ".large.part", content_type: largeFileObject.content_type)
    counter = 0
    parts = []
    while !largeFileObject.eof?
        counter += 1
        puts "Chunk number: #{counter}"
        body = largeFileObject.read ChunkSize
        resp2 = s3.upload_part(body: body, bucket: Bucket, key: Filename + ".large.part", part_number: counter, upload_id: resp1.upload_id)
        parts.push({etag: resp2.etag.gsub("\"", ""), part_number: counter.to_i})
    end
    status = s3.complete_multipart_upload(bucket: Bucket, key: Filename + ".large.part", upload_id: resp1.upload_id,
                                          multipart_upload: {parts: parts})

    # List objects in the bucket
    puts "----------List Files---------"
    resp = s3.list_objects(bucket: Bucket)
    resp.contents.each do |obj|
        puts "key: #{obj.key}\tsize: #{obj.size}\t#{obj.etag}"
        if !fileObject.size.eql? obj.size 
            if !largeFileObject.size.eql? obj.size
                raise " Content length is changed for : #{obj.key}"
            end
        end
    end
    puts "\n"

    # HEAD object
    puts "----------Head---------"
    fileObject.rewind
    fileDigest = Digest::MD5.hexdigest(fileObject.read)
    resp = s3.head_object(bucket: Bucket, key: Filename + ".single")
    if !((fileObject.size.eql? resp.content_length) && (fileDigest.eql? resp.etag.gsub("\"", ""))) ## for future use  && (fileObject.content_type.eql? resp.content_type))
        raise "Single Part File Metadata could not match"
    else
        puts "Single Part File etag: #{resp.etag} size: #{resp.content_length}"
    end

    resp = s3.head_object(bucket: Bucket, key: Filename)
    if !((fileObject.size.eql? resp.content_length)) ## for future use && (fileDigest.eql? resp.etag.gsub("\"", ""))) && (fileObject.content_type.eql? resp.content_type)
        raise "Multipart File Metadata could not match"
    else
        puts "Multipart Part File etag: #{resp.etag} size: #{resp.content_length}"
    end
    puts "\n"

    # GET object(To be handled at the below rescue block)
    puts "----------Get---------"
    resp = s3.get_object(bucket: Bucket, key: Filename + ".single")
    if !fileObject.size.eql?  resp.content_length
        raise "Signle part Upload File content is not equal\n"
    end
    puts "Single Part Upload object data :\t" # + bucket.objects[FileName + ".single"].read
    resp = s3.get_object(bucket: Bucket, key: Filename)
    if !fileObject.size.eql? resp.content_length
        raise "Multi Part Upload File content is not equal\n"
    end
    puts fileObject.content_type
    if fileObject.content_type.eql? "text/plain"
        puts "Multi Part Upload object data :\t" # + bucket.objects[FileName].read + "\n"
    else
        puts "File Content type is :" + resp.content_type + "\n\n"
    end

    # GET non-existing object
    puts "----------Get non-existing---------"
    begin
        resp = s3.get_object(bucket: Bucket, key: Filename + ".nonexists")
        raise "The file must NOT be exist\n"
    rescue  Aws::S3::Errors::NoSuchKey
        puts "Get non-existing object Successfully..\n"
    end
    puts "\n"

    # Range GET object
    puts "----------Range Get---------"
    resp = s3.get_object(bucket: Bucket, key: Filename, range: "bytes=1-4")
    if resp.body.read != "his "
        raise "Range Get Result does NOT match"
    else
        puts "Range Get Succeeded"
    end
    puts "\n"

    baseArr = []
    open LargeFilePath, 'r' do |f|
        f.seek 1048576
        baseArr = f.read (10485760 - 1048576 + 1)
    end

    puts "---Range Get Single-Part---"
    resp = s3.get_object(bucket: Bucket, key: Filename+".large.one", range: "bytes=1048576-10485760")
    getBin = resp.body.read
    if getBin != baseArr
        puts getBin.size
        raise "Range Get Result does NOT match"
    else
        puts "Range Get Succeeded"
    end
    puts "\n"

    puts "---Range Get Multi-Part---"
    resp = s3.get_object(bucket: Bucket, key: Filename+".large.part", range: "bytes=1048576-10485760")
    getBin = resp.body.read
    if getBin != baseArr
        puts getBin.size
        raise "Range Get Result does NOT match"
    else
        puts "Range Get Succeeded"
    end
    puts "\n"

    # Copy object
    # Copy object
    puts "----------Copy---------"
    resp = s3.copy_object(bucket: Bucket, key: Filename + ".copy", copy_source: Bucket + "/" + Filename)
    begin
        resp = s3.get_object(bucket: Bucket, key: Filename + ".copy")
        puts "File copied successfully\n"
    rescue Aws::S3::Errors::NoSuchKey
        puts "File could not Copy Successfully\n"
    end
    puts "\n"

    # List objects in the bucket
    puts "----------List Files---------"
    resp = s3.list_objects(bucket: Bucket)
    resp.contents.each do |obj|
        if !fileObject.size.eql? obj.size
            raise " Content length is changed for : #{obj.key}"
        end
        puts "key: #{obj.key}\tsize: #{obj.size}\t#{obj.etag}"
    end
    puts "\n"

    # Move object
    # Not exists API

    # Rename object
    # Not exists API

    # Download File
    puts "----------Download---------"
    resp = s3.get_object(response_target: file_path + ".copy", bucket: Bucket, key: Filename)
    md5s = []
    binary = ""
    fp = open(file_path + ".copy")
    while !fp.eof?
        part = fp.read(ChunkSize)
        binary += part
        md5s << Digest::MD5.digest(part)
    end
    fp.close
    fileDigest = Digest::MD5.hexdigest(md5s.join('')) # + '-' + md5s.size.to_s
    resp = s3.head_object(bucket: Bucket, key: Filename)
    puts "org_size: #{resp.content_length} org_etag: #{resp.etag.gsub("\"", "")}"
    puts "dst_size: #{binary.size} dst_etag: #{fileDigest}"
    if !((binary.size.eql? resp.content_length) && (fileDigest.eql? resp.etag.gsub("\"", "")))
        raise "Downloaded File Metadata could not match"
    else
        puts "File Downloaded Successfully\n"
    end
    puts "\n"

    # Delete objects one by one and check if exist
    puts "----------Delete---------"
    resp = s3.list_objects(bucket: Bucket)
    resp.contents.each do |obj|
        s3.delete_object(bucket: Bucket, key: obj.key)
        # to be not found
        begin
            s3.get_object(bucket: Bucket, key: obj.key)
        rescue Aws::S3::Errors::NoSuchKey
            puts "#{obj.key} \t File Deleted Successfully..\n"
            next
        end
        raise "Object is not Deleted Successfully\n"
    end
    puts "\n"

    # List multi layered directories
    # PUT an object using single-part method and the obj-name is "bucket-name"
    puts "----------List multi layered directories---------"
    BaseDir = "a/b/c/"
    s3.put_object(bucket: Bucket, key: BaseDir + "test1", body: fileObject, content_type: fileObject.content_type)
    s3.put_object(bucket: Bucket, key: BaseDir + "test2", body: fileObject, content_type: fileObject.content_type)
    resp = s3.list_objects(bucket: Bucket, prefix: BaseDir)
    resp.contents.each do |obj|
        puts "key: #{obj.key}"
    end
    puts "\n"

    # Delete multi layered directories
    puts "----------Delete multi layered directories---------"
    BaseDir2 = "a"
    s3.delete_object(bucket: Bucket, key: BaseDir2)
    resp = s3.list_objects(bucket: Bucket, prefix: BaseDir2)
    if resp.contents.size > 0
        raise "Multi layered directories are not Deleted Successfully\n"
    else
        puts "Multi layered directories Deleted Successfully\n"
    end
    puts "\n"

    # Get-Put ACL
    puts "#####Default ACL#####"
    resp = s3.get_bucket_acl(bucket: Bucket)
    puts "Owner ID : #{resp.owner[:id]}"
    puts "Owner Display name : #{resp.owner[:display_name]}"
    permissions = []
    resp.grants.each do |grants|
        puts "Bucket ACL is : #{grants.permission}"
        puts "Bucket Grantee URI is : #{grants.grantee[:uri]}"
        permissions << grants.permission
    end
    if !(permissions == ["FULL_CONTROL"])
        raise "Permission is Not full_control"
    else
        puts "Bucket ACL permission is 'private'\n\n"
    end

    puts "#####:public_read ACL#####"
    s3.put_bucket_acl(bucket: Bucket, acl: "public-read")
    resp = s3.get_bucket_acl(bucket: Bucket)
    puts "Owner ID : #{resp.owner[:id]}"
    puts "Owner Display name : #{resp.owner[:display_name]}"
    permissions = []
    resp.grants.each do |grants|
        puts "Bucket ACL is : #{grants.permission}"
        puts "Bucket Grantee URI is : #{grants.grantee[:uri]}"
        permissions << grants.permission
    end
    if !(permissions == ["READ", "READ_ACP"] )
        raise "Permission is Not public_read"
    else
        puts "Bucket ACL Successfully changed to 'public-read'\n\n"
    end

    puts "#####:public_read_write ACL#####"
    s3.put_bucket_acl(bucket: Bucket, acl: "public-read-write")
    resp = s3.get_bucket_acl(bucket: Bucket)
    puts "Owner ID : #{resp.owner[:id]}"
    puts "Owner Display name : #{resp.owner[:display_name]}"
    permissions = []
    resp.grants.each do |grants|
        puts "Bucket ACL is : #{grants.permission}"
        puts "Bucket Grantee URI is : #{grants.grantee[:uri]}"
        permissions << grants.permission
    end
    if !(permissions == ["READ", "READ_ACP", "WRITE", "WRITE_ACP"] )
        raise "Permission is Not public_read_write"
    else
        puts "Bucket ACL Successfully changed to 'public-read-write'\n\n"
    end

    puts "#####:private ACL#####"
    s3.put_bucket_acl(bucket: Bucket, acl: "private")
    resp = s3.get_bucket_acl(bucket: Bucket)
    puts "Owner ID : #{resp.owner[:id]}"
    puts "Owner Display name : #{resp.owner[:display_name]}"
    permissions = []
    resp.grants.each do |grants|
        puts "Bucket ACL is : #{grants.permission}"
        puts "Bucket Grantee URI is : #{grants.grantee[:uri]}"
        permissions << grants.permission
    end
    if !(permissions == ["FULL_CONTROL"])
        raise "Permission is Not full_control"
    else
        puts "Bucket ACL Successfully changed to 'private'\n\n"
    end
    puts "\n"
rescue
    # Unexpected error occurred
    p $!
    exit(-1)
ensure
    puts "----------Delete test bucket---------"
    # Bucket Delete
    s3.delete_bucket(bucket: Bucket)
    puts "Bucket deleted Successfully\n"
end
