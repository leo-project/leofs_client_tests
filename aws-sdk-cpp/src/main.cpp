#include <iostream>
#include <string>
#include <fstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "definitions.h"

using ClientPtrType = std::shared_ptr<Aws::S3::S3Client>;
using String = Aws::String;

ClientPtrType init(String host, String port)
{
    Aws::Client::ClientConfiguration config;
    config.region = "us-west-2";
    config.endpointOverride = "http://" + host + ":" + port;
    config.scheme = Aws::Http::Scheme::HTTP;
    Aws::Auth::AWSCredentials cred(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    return Aws::MakeShared<Aws::S3::S3Client>("S3Client", cred, config);
}

void createBucket(ClientPtrType client, String bucketName)
{
    String base = "=== Create Bucket [" + bucketName;
    std::cout << base << "]: Start ===\n";
    auto bucketReq = Aws::S3::Model::CreateBucketRequest();
    auto bucketRes = client->CreateBucket(bucketReq.WithBucket(bucketName));
    if (bucketRes.IsSuccess())
    {
        std::cout << base << "]: Success ===\n";
    }
    std::cout << base << "]: End ===\n";
}

void deleteBucket(ClientPtrType client, String bucketName)
{
    String base = "=== Delete Bucket [" + bucketName;
    std::cout << base << "]: Start ===\n";
    auto bucketReq = Aws::S3::Model::DeleteBucketRequest();
    auto bucketRes = client->DeleteBucket(bucketReq.WithBucket(bucketName));
    if (bucketRes.IsSuccess())
    {
        std::cout << base << "]: Success ===\n";
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

void putObject(ClientPtrType client, String bucketName, String key, String path)
{
    String base = "=== Put Object [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    auto inpData = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
            path.c_str(), std::ios_base::in | std::ios_base::binary);
    auto objReq = Aws::S3::Model::PutObjectRequest();
    objReq.WithBucket(bucketName).WithKey(key).SetBody(inpData);
    auto objRes = client->PutObject(objReq);
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side success ===\n";
    }
    if (!doesObjectExists(client, bucketName, key))
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n";
}

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
    if (argc)
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

    // put object
    putObject(client, bucketName, "test.simple", SMALL_TEST_FILE);
    putObject(client, bucketName, "test.medium", MED_TEST_FILE);
    putObject(client, bucketName, "test.large", LARGE_TEST_FILE);

    deleteBucket(client, bucketName);
    // end of tests
    std::cout << "=== AWS API Shutdown: Start===\n";
    Aws::ShutdownAPI(options);
    std::cout << "=== AWS API Shutdown: End ===\n";
    return 0;
}
