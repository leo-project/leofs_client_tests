#include <iostream>
#include <string>
#include <fstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListMultipartUploadsRequest.h>
#include <aws/s3/model/MultipartUpload.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "definitions.h"

using ClientPtrType = std::shared_ptr<Aws::S3::S3Client>;
using String = Aws::String;
using Map = Aws::Map<String, String>;

ClientPtrType init(String host, String port);
bool doesBucketExists(ClientPtrType client, String bucketName);
void createBucket(ClientPtrType client, String bucketName);
void deleteBucket(ClientPtrType client, String bucketName);
bool doesObjectExists(ClientPtrType client, String bucketName, String key);
void putObject(ClientPtrType client, String bucketName, String key, String path, Map metadata = Map());
void putObjectMp(ClientPtrType client, String bucketName, String key, String path, Map metadata = Map());

int main(int argc, char** argv)
{
    std::cout << "=== AWS API Init: Start===\n";
    Aws::SDKOptions options;
    // set the options
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    // end of options
    Aws::InitAPI(options);
    // setup
    String signVer = SIGN_VER, host = HOST, portStr = PORT,
                bucketName = BUCKET;
    if (argc == 5)
    {
        signVer = argv[1];
        host = argv[2];
        portStr = argv[3];
        bucketName = argv[4];
    }

    auto client = init(host, portStr);
    std::cout << "=== AWS API Init: End ===\n";
    // call tests here
    createBucket(client, bucketName);
    std::cout << '\n';

    // put object
    putObject(client, bucketName, "test.simple", SMALL_TEST_FILE);
    putObject(client, bucketName, "test.medium", MED_TEST_FILE);
    putObject(client, bucketName, "test.large", LARGE_TEST_FILE);

    // put object with metadata
    Map metadata;
    metadata[METADATA_KEY] = METADATA_VAL;
    putObject(client, bucketName, "test.simple.meta", SMALL_TEST_FILE, metadata);
    putObject(client, bucketName, "test.medium.meta", MED_TEST_FILE, metadata);
    putObject(client, bucketName, "test.large.meta", LARGE_TEST_FILE, metadata);

    // put object in parts

    deleteBucket(client, bucketName);
    // end of tests
    std::cout << "=== AWS API Shutdown: Start===\n";
    Aws::ShutdownAPI(options);
    std::cout << "=== AWS API Shutdown: End ===\n";
    return 0;
}

ClientPtrType init(String host, String port)
{
    Aws::Client::ClientConfiguration config;
    config.region = "us-west-2";
    config.endpointOverride = host + ":" + port;
    config.scheme = Aws::Http::Scheme::HTTP;
    Aws::Auth::AWSCredentials cred(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    return Aws::MakeShared<Aws::S3::S3Client>("S3Client", cred, config);
}

bool doesBucketExists(ClientPtrType client, String bucketName)
{
    auto objectReq = Aws::S3::Model::HeadBucketRequest();
    auto objectRes = client->HeadBucket(objectReq.WithBucket(bucketName));
    if (objectRes.IsSuccess())
    {
        return true;
    }
    return false;
}

void createBucket(ClientPtrType client, String bucketName)
{
    String base = "=== Create Bucket [" + bucketName;
    std::cout << base << "]: Start ===\n";
    auto bucketReq = Aws::S3::Model::CreateBucketRequest();
    auto bucketRes = client->CreateBucket(bucketReq.WithBucket(bucketName));
    if (!bucketRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << bucketRes.GetError().GetExceptionName() << "\t" <<
                     bucketRes.GetError().GetMessage() << "\n";
    }
    if (!doesBucketExists(client, bucketName))
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n";
}

void deleteBucket(ClientPtrType client, String bucketName)
{
    String base = "=== Delete Bucket [" + bucketName;
    std::cout << base << "]: Start ===\n";
    auto bucketReq = Aws::S3::Model::DeleteBucketRequest();
    auto bucketRes = client->DeleteBucket(bucketReq.WithBucket(bucketName));
    if (!bucketRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << bucketRes.GetError().GetExceptionName() << "\t" <<
                     bucketRes.GetError().GetMessage() << "\n";
    }
    if (doesBucketExists(client, bucketName))
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n";
}

bool doesObjectExists(ClientPtrType client, String bucketName, String key)
{
    auto objectReq = Aws::S3::Model::HeadObjectRequest();
    objectReq.WithBucket(bucketName).WithKey(key);
    auto objectRes = client->HeadObject(objectReq);
    if (objectRes.IsSuccess())
    {
        return true;
    }
    return false;
}

void putObject(ClientPtrType client, String bucketName, String key, String path, Map metadata)
{
    String base = "=== Put Object [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    std::cout << "Reading from " << path << "\n";
    auto inpData = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
            path.c_str(), std::ios_base::in | std::ios_base::binary);
    auto objReq = Aws::S3::Model::PutObjectRequest();
    objReq.WithBucket(bucketName).WithKey(key).SetBody(inpData);;
    if (!metadata.empty())
    {
        std::cout << "Key\t:\t\tValue\n";
        for(auto& it: metadata)
        {
            auto key = it.first;
            auto value = it.second;
            std::cout << key << "\t:\t\t" << value << "\n";
        }
        objReq.SetMetadata(metadata);
    }
    auto objRes = client->PutObject(objReq);
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << objRes.GetError().GetExceptionName() << "\t" <<
                     objRes.GetError().GetMessage() << "\n";
    }
    if (!doesObjectExists(client, bucketName, key))
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n";
}

void putObjectMp(ClientPtrType client, String bucketName, String key, String path, Map metadata)
{
    String base = "=== Put Object MultiPart [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    std::cout << "Reading from " << path << "\n";
    auto inpData = Aws::MakeShared<Aws::FStream>("PutObjectMpInputStream",
            path.c_str(), std::ios_base::in | std::ios_base::binary);
    auto objReq = Aws::S3::Model::CreateMultipartUploadRequest();
    objReq.WithBucket(bucketName).WithKey(key);
    if (!metadata.empty())
    {
        std::cout << "Map Key\t:\t\tData Value\n";
        for(auto& it: metadata)
        {
            auto key = it.first;
            auto value = it.second;
            std::cout << key << "\t:\t\t" << value << "\n";
        }
        objReq.SetMetadata(metadata);
    }
    auto objRes = client->CreateMultipartUpload(objReq);
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << "Couldn't create multi-part upload\n";
        std::cout << objRes.GetError().GetExceptionName() << "\t" <<
                     objRes.GetError().GetMessage() << "\n";
        goto ReturnPt;
    }
    {
    // check if multipart upload exists
    auto listReq = Aws::S3::Model::ListMultipartUploadsRequest();
    listReq.WithBucket(bucketName).WithPrefix(key);
    auto listRes = client->ListMultipartUploads(listReq);
    if (!listRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << "Couldn't list multi-part upload\n";
        std::cout << listRes.GetError().GetExceptionName() << "\t" <<
                     listRes.GetError().GetMessage() << "\n";
        // abort upload?
        goto ReturnPt;
    }
    auto id = objRes.GetResult().GetUploadId();
    auto uploads = listRes.GetResult().GetUploads();
    if (uploads.size() != 1 || uploads[0].GetUploadId() != id)
    {
        std::cout << base << "]: Upload check failure ===\n";
        std::cout << "Couldn't find in multi-part upload list\n";
        std::cout << listRes.GetError().GetExceptionName() << "\t" <<
                     listRes.GetError().GetMessage() << "\n";
        // abort upload?
        goto ReturnPt;
    }
    }
    // upload part
    {
    auto id = objRes.GetResult().GetUploadId();
    auto uploadReq = Aws::S3::Model::UploadPartRequest();
    uploadReq.WithBucket(bucketName).WithKey(key).WithUploadId(id);;
    uploadReq.WithPartNumber(1).SetBody(inpData);
    auto uploadRes = client->UploadPart(uploadReq);
    if (!uploadRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << "Couldn't list multi-part upload\n";
        std::cout << uploadRes.GetError().GetExceptionName() << "\t" <<
                     uploadRes.GetError().GetMessage() << "\n";
        // abort upload?
        goto ReturnPt;
    }
    }
    // check if multipart exists
    // finish upload
    {
    auto id = objRes.GetResult().GetUploadId();
    auto compReq = Aws::S3::Model::CompleteMultipartUploadRequest();
    compReq.WithBucket(bucketName).WithKey(key).WithUploadId(id);
    auto compRes = client->CompleteMultipartUpload(compReq);
    if (!compRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << "Couldn't complete multi-part upload\n";
        std::cout << compRes.GetError().GetExceptionName() << "\t" <<
                     compRes.GetError().GetMessage() << "\n";
        goto ReturnPt;
    }
    }
ReturnPt:
    if (!doesObjectExists(client, bucketName, key))
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n";
}
