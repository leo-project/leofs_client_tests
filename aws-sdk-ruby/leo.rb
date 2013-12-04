# AWS::S3::Errors::NoSuchKey# This code supports "aws-sdk v1.9.5"
require "aws-sdk"

Endpoint = "localhost"
Port = 8080
# set your s3 key
AccessKeyId = "05236"
SecretAccessKey = "802562235"

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
    # PUT
    # create bucket
    s3.buckets.create("photo")
    
    # get bucket
    bucket = s3.buckets["photo"]
    
    # create a new object
    object = bucket.objects.create("image", "value")
    
    # show objects in the bucket
    bucket.objects.with_prefix("").each do |obj|
      p obj
    end
    
    # retrieve an object
    object = bucket.objects["image"]
    
    # insert an object
    object.write(
      file: "test.txt",
      content_type: "text/plain"
    )
    
    # GET
    image = object.read
    p image
    
    # HEAD
    metadata = object.head
    p metadata
    
    # DELETE
    object.delete

    # Multi part
    file_path_for_multipart_upload = '32M.dat'
    open(file_path_for_multipart_upload) do |file|
      uploading_object = bucket.objects[File.basename(file.path)]
      uploading_object.multipart_upload do |upload|
        while !file.eof?
          upload.add_part(file.read 5242880) ## 5MB ##
          p('Aborted') if upload.aborted?
        end
      end
    end
    large_object = bucket.objects["32M.dat"]
    image = large_object.read
    
    # GET(To be handled at the below rescure block)
    image = object.read
    p image
rescue AWS::S3::Errors::NoSuchKey
    exit
rescue
    # unexpected error occured
    p $!
    exit(-1)
ensure
    bucket = s3.buckets["photo"]
    bucket.delete 
end
