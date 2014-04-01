## This code supports "aws-sdk v1.9.5"
require "aws-sdk"
require "content_type"

# set your s3 key and variable
Endpoint = "localhost"
Port = 8080
AccessKeyId = "05236"
SecretAccessKey = "802562235"
FileName = "testFile"
ChunkSize = 5 * 1024 * 1024  ## 5 MB chunk size
Bucket = "test" + rand(99999).to_s  ## Dynamic BucketName

class LeoFSHandler < AWS::Core::Http::NetHttpHandler
  def handle(request, response)
    request.port = ::Port
    super
  end
end

SP = AWS::Core::CredentialProviders::StaticProvider.new(
{
  :access_key_id     => AccessKeyId,
  :secret_access_key => SecretAccessKey
})

AWS.config(
  access_key_id: AccessKeyId,
  secret_access_key: SecretAccessKey,
  s3_endpoint: Endpoint,
  http_handler: LeoFSHandler.new,
  credential_provider: SP,
  s3_force_path_style: true,
  use_ssl: false
)

s3 = AWS::S3.new
begin
  # Create bucket
  s3.buckets.create(Bucket)
  puts "Bucket Created Successfully\n"

  # Get bucket
  bucket = s3.buckets[Bucket]
  puts "Get Bucket Successfully\n\n"

  # PUT Object
  file_path = "../temp_data/" + FileName
  fileObject =  open(file_path)

  # PUT object using single-part method
  obj = bucket.objects[FileName + ".single"].write(file: file_path, content_type: fileObject.content_type)

  # PUT object using multi-part method
  puts "File is being upload:\n"
  counter = fileObject.size / ChunkSize
  uploading_object = bucket.objects[File.basename(fileObject.path)]
  uploading_object.multipart_upload(:content_type => fileObject.content_type.to_s) do |upload|
    while !fileObject.eof?
      puts " #{upload.id} \t\t #{counter} "
      counter -= 1
      upload.add_part(fileObject.read ChunkSize) ## 20MB Default size is 5242880 Byte
      p("Aborted") if upload.aborted?
    end
  end
  puts "File Uploaded Successfully\n\n"

  # List objects in the bucket
  puts "----------List Files---------\n"
  bucket.objects.with_prefix("").each do |obj|
    if !fileObject.size.eql? obj.content_length
       raise " Content length is changed for : #{obj.key}"
    end
    puts "#{obj.key} \t #{obj.content_length}"
  end

  # HEAD object
  fileObject.seek(0)
  fileDigest = Digest::MD5.hexdigest(fileObject.read)
  metadata = bucket.objects[FileName + ".single"].head
  if !((fileObject.size.eql? metadata.content_length) && (fileDigest.eql? metadata.etag.gsub('"', ''))) ## for future use  && (fileObject.content_type.eql? metadata.content_type))
    raise "Single Part File Metadata could not match"
  else
    puts "Single Part File MetaData :"
    p metadata
  end
  metadata = bucket.objects[FileName].head
  if !(fileObject.size.eql? metadata.content_length)  ## for future use && (fileObject.content_type.eql? metadata.content_type)
    raise "Multipart File Metadata could not match"
  else
    puts "Multipart Part File MetaData :"
    p metadata
  end

  # GET object(To be handled at the below rescue block)
  if !fileObject.size.eql?  bucket.objects[FileName + ".single"].head.content_length
    raise "\nSignle part Upload File content is not equal\n"
  end
  puts "\nSingle Part Upload object data :\t" + bucket.objects[FileName + ".single"].read
  if !fileObject.size.eql? bucket.objects[FileName].head.content_length
    raise "Multi Part Upload File content is not equal\n"
  end
  if fileObject.content_type.eql? "text/plain"
    puts "Multi Part Upload object data :\t" +  bucket.objects[FileName].read + "\n"
  else
    puts "File Content type is :" + bucket.objects[FileName].content_type + "\n\n"
  end

  # Copy object
  bucket.objects[FileName + ".copy"].copy_from(FileName)
  if !bucket.objects[FileName + ".copy"].exists?
    raise "File could not Copy Successfully\n"
  end
  puts "File copied successfully\n"

  # List objects in the bucket
  puts "----------List Files---------\n"
  bucket.objects.with_prefix("").each do |obj|
    if !fileObject.size.eql? obj.content_length
       raise " Content length is changed for : #{obj.key}"
    end
    puts "#{obj.key} \t #{obj.content_length}"
  end

  # Move object
  obj = bucket.objects[FileName + ".copy"].move_to(FileName + ".org")
  if !obj.exists?
    raise "File could not Moved Successfully\n"
  end
  puts "\nFile move Successfully\n"

  # List objects in the bucket
  puts "----------List Files---------\n"
  bucket.objects.with_prefix("").each do |obj|
    if !fileObject.size.eql? obj.content_length
      raise " Content length is changed for : #{obj.key}"
    end
    puts "#{obj.key} \t #{obj.content_length}"
  end

  # Rename object
  obj = bucket.objects[FileName + ".org"].rename_to(FileName + ".copy")
  if !obj.exists?
    raise "File could not Rename Successfully\n"
  end
  puts "\nFile rename Successfully\n"

  # List objects in the bucket
  puts "----------List Files---------\n"
  bucket.objects.with_prefix("").each do |obj|
    if !fileObject.size.eql? obj.content_length
      raise " Content length is changed for : #{obj.key}"
    end
    puts "#{obj.key} \t #{obj.content_length}"
  end

  # Download File
  File.open(FileName + ".copy", "w+") do |thisfileObject|
    bucket.objects[FileName].read do |chunk|
      thisfileObject.write(chunk)
    end
    thisfileObject.seek(0)
    thisfileDigest = Digest::MD5.hexdigest(thisfileObject.read)
    if !((thisfileObject.size.eql? metadata.content_length) && (fileDigest.eql? thisfileDigest))
      raise "Downloaded File Metadata could not match"
    else
      puts "\nFile Downloaded Successfully\n"
    end
  end

  # Delete objects one by one and check if exist
  puts "--------------------Delete Files--------------------\n"
  bucket.objects.with_prefix("").each do |obj|
    obj.delete
    puts "#{obj.key} \t File Deleted Successfully..\n"
    if obj.exists?
      raise "Object is not Deleted Successfully\n"
    end
  end

  # Get-Put ACL
  puts "\n#####Default ACL#####"
  puts "Owner ID : #{bucket.acl.owner.id}"
  puts "Owner Display name : #{bucket.acl.owner.display_name}"
  permissions = []
  bucket.acl.grants.each do |grant|
    puts "Bucket ACL is : #{grant.permission.name}"
    puts "Bucket Grantee URI is : #{grant.grantee.uri}"
    permissions << grant.permission.name
  end
  if !(permissions == [:full_control])
    raise "Permission is Not full_control"
  else
    puts "Bucket ACL permission is 'private'\n\n"
  end

  puts "#####:public_read ACL#####"
  bucket.acl = :public_read
  puts "Owner ID : #{bucket.acl.owner.id}"
  puts "Owner Display name : #{bucket.acl.owner.display_name}"
  permissions = []
  bucket.acl.grants.each do |grant|
    puts "Bucket ACL is : #{grant.permission.name}"
    puts "Bucket Grantee URI is : #{grant.grantee.uri}"
    permissions << grant.permission.name
  end
  if !(permissions == [:read, :read_acp] )
    raise "Permission is Not public_read"
  else
    puts "Bucket ACL Successfully changed to 'public-read'\n\n"
  end

  puts "#####:public_read_write ACL#####"
  bucket.acl = :public_read_write
  puts "Owner ID : #{bucket.acl.owner.id} "
  puts "Owner Display name : #{bucket.acl.owner.display_name}"
  permissions = []
  bucket.acl.grants.each do |grant|
    puts "Bucket ACL is : #{grant.permission.name}"
    puts "Bucket Grantee URI is : #{grant.grantee.uri}"
    permissions << grant.permission.name
  end
  if !(permissions == [:read, :read_acp, :write, :write_acp])
    raise "Permission is Not public_read_write"
  else
    puts "Bucket ACL Successfully changed to 'public-read-write'\n\n"
  end

  puts "#####:private ACL#####"
  bucket.acl = :private
  puts "Owner ID : #{bucket.acl.owner.id} "
  puts "Owner Display name : #{bucket.acl.owner.display_name}"
  permissions = []
  bucket.acl.grants.each do |grant|
    puts "Bucket ACL is : #{grant.permission.name}"
    puts "Bucket Grantee URI is : #{grant.grantee.uri}"
    permissions << grant.permission.name
  end
  if !(permissions == [:full_control])
    raise "Permission is Not full_control"
  else
    puts "Bucket ACL Successfully changed to 'private'\n\n"
  end
rescue
  # Unexpected error occurred
  p $!
  exit(-1)
ensure
  # Bucket Delete
  bucket = s3.buckets[Bucket]
  bucket.clear!  #clear the versions only
  bucket.delete
  puts "Bucket deleted Successfully\n"
end
