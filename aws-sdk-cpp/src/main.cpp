#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListMultipartUploadsRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListPartsRequest.h>
#include <aws/s3/model/MultipartUpload.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "definitions.h"

using ClientPtrType = std::shared_ptr<Aws::S3::S3Client>;
using String = Aws::String;
using Map = Aws::Map<String, String>;
using Object = Aws::S3::Model::Object;

ClientPtrType init(String host, String port);
bool doesBucketExists(ClientPtrType client, String bucketName);
void createBucket(ClientPtrType client, String bucketName);
void deleteBucket(ClientPtrType client, String bucketName);
std::tuple<bool, Map> doesObjectExists(ClientPtrType client, String bucketName, String key);
void putObject(ClientPtrType client, String bucketName, String key, String path, Map metadata = Map(), size_t loop = 0);
void putObjectMp(ClientPtrType client, String bucketName, String key, String path, Map metadata = Map());
bool doFilesMatch(Aws::FStream* a, Aws::IOStream& b, size_t min = 0, size_t max = 0);
void getObject(ClientPtrType client, String bucketName, String key, String path, Map metadata = Map());
void getFakeObject(ClientPtrType client, String bucketName, String key);
void rangeObject(ClientPtrType client, String bucketName, String key, String path, size_t min, size_t max);
void copyObject(ClientPtrType client, String bucketName, String src, String dst);
Aws::Vector<Object> listObjects(ClientPtrType client, String bucketName, String prefix, size_t expected, int maxKeys = 0, bool debugCheck = false);
void deleteObject(ClientPtrType client, String bucketName, String key);
void deleteObjects(ClientPtrType client, String bucketName, String prefix, size_t num);
void deleteAllObjects(ClientPtrType client, String bucketName);

int main(int argc, char** argv)
{
    String base = "=== [AWS API Init";
    std::cout << base << "]: Start===\n";
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
    std::cout << base << "]: End ===\n\n";
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
    putObjectMp(client, bucketName, "test.simple.mp", SMALL_TEST_FILE);
    putObjectMp(client, bucketName, "test.medium.mp", MED_TEST_FILE);
    putObjectMp(client, bucketName, "test.large.mp", LARGE_TEST_FILE);

    // put object in parts with metadata
    putObjectMp(client, bucketName, "test.simple.meta.mp", SMALL_TEST_FILE, metadata);
    putObjectMp(client, bucketName, "test.medium.meta.mp", MED_TEST_FILE, metadata);
    putObjectMp(client, bucketName, "test.large.meta.mp", LARGE_TEST_FILE, metadata);

    // head is already tested
    // get object
    getObject(client, bucketName, "test.simple", SMALL_TEST_FILE);
    getObject(client, bucketName, "test.medium", MED_TEST_FILE);
    getObject(client, bucketName, "test.large", LARGE_TEST_FILE);
    getObject(client, bucketName, "test.simple.mp", SMALL_TEST_FILE);
    getObject(client, bucketName, "test.medium.mp", MED_TEST_FILE);
    getObject(client, bucketName, "test.large.mp", LARGE_TEST_FILE);
    getObject(client, bucketName, "test.simple.meta", SMALL_TEST_FILE, metadata);
    getObject(client, bucketName, "test.medium.meta", MED_TEST_FILE, metadata);
    getObject(client, bucketName, "test.large.meta", LARGE_TEST_FILE, metadata);
    getObject(client, bucketName, "test.simple.meta.mp", SMALL_TEST_FILE, metadata);
    getObject(client, bucketName, "test.medium.meta.mp", MED_TEST_FILE, metadata);
    getObject(client, bucketName, "test.large.meta.mp", LARGE_TEST_FILE, metadata);

    // get fake object
    getFakeObject(client, bucketName, "test.noexist");

    // range get object
    rangeObject(client, bucketName, "test.simple", SMALL_TEST_FILE, 1, 4);
    // rangeObject(client, bucketName, "test.simple.mp", SMALL_TEST_FILE, 1, 4);
    rangeObject(client, bucketName, "test.large", LARGE_TEST_FILE, 1048576, 40485760);
    // rangeObject(client, bucketName, "test.large.mp", LARGE_TEST_FILE, 1048576, 10485760);

    // copy object
    copyObject(client, bucketName, "test.simple", "test.simple.copy");
    getObject(client, bucketName, "test.simple.copy", SMALL_TEST_FILE);

    // list object
    listObjects(client, bucketName, "", -1);

    // delete all objects
    deleteAllObjects(client, bucketName);
    listObjects(client, bucketName, "", 0);

    // put dummy objects
    putObject(client, bucketName, "list/test.small.", SMALL_TEST_FILE, Map(), 35);
    // multi-page list obj
    listObjects(client, bucketName, "list/", 35, 10);

    // multi-delete
    deleteObjects(client, bucketName, "list/test.small.", 10);
    listObjects(client, bucketName, "list/", 25);

    // get-put acl

    // delete bucket
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
    std::cout << base << "]: End ===\n\n";
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
    std::cout << base << "]: End ===\n\n";
}

std::tuple<bool, Map> doesObjectExists(ClientPtrType client, String bucketName, String key)
{
    auto objectReq = Aws::S3::Model::HeadObjectRequest();
    objectReq.WithBucket(bucketName).WithKey(key);
    auto objectRes = client->HeadObject(objectReq);
    if (objectRes.IsSuccess())
    {
        return std::make_tuple(true, objectRes.GetResult().GetMetadata());
    }
    return std::make_tuple(false, Map());
}

void putObject(ClientPtrType client, String bucketName, String key, String path, Map metadata, size_t loop)
{
    String base = "=== Put Object [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    std::cout << "Reading from " << path << "\n";
    auto inpData = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
            path.c_str(), std::ios_base::in | std::ios_base::binary);
    auto objReq = Aws::S3::Model::PutObjectRequest();
    objReq.WithBucket(bucketName).SetBody(inpData);;
    if (!metadata.empty())
    {
        std::cout << "Map Key\t:\t\tValue\n";
        for(auto& it: metadata)
        {
            auto key = it.first;
            auto value = it.second;
            std::cout << key << "\t:\t\t" << value << "\n";
        }
        objReq.SetMetadata(metadata);
    }
    if (loop == 0)
    {
        auto objRes = client->PutObject(objReq.WithKey(key));
        if (!objRes.IsSuccess())
        {
            std::cout << base << "]: Client Side failure ===\n";
            std::cout << objRes.GetError().GetExceptionName() << "\t" <<
                        objRes.GetError().GetMessage() << "\n";
        }
        if (!std::get<0>(doesObjectExists(client, bucketName, key)))
        {
            std::cout << base << "]: Failed ===\n";
        }
    }
    for (size_t i = 0; i < loop; ++i)
    {
        auto inpData = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
                path.c_str(), std::ios_base::in | std::ios_base::binary);
        objReq.SetBody(inpData);;
        String key_suffix = std::to_string(i).c_str();
        auto objRes = client->PutObject(objReq.WithKey(key + key_suffix));
        if (!objRes.IsSuccess())
        {
            std::cout << base << key_suffix << "]: Client Side failure ===\n";
            std::cout << objRes.GetError().GetExceptionName() << "\t" <<
                        objRes.GetError().GetMessage() << "\n";
        }
        if (!std::get<0>(doesObjectExists(client, bucketName, key + key_suffix)))
        {
            std::cout << base << key_suffix << "]: Failed ===\n";
        }
    }
    std::cout << base << "]: End ===\n\n";
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
        goto PutReturnPt;
    }
    /** LeoFS Doesn't support ListMultipartUploadsRequest
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
        goto PutReturnPt;
    }
    auto id = objRes.GetResult().GetUploadId();
    auto uploads = listRes.GetResult().GetUploads();
    if (uploads.size() != 1 || uploads[0].GetUploadId() != id)
    {
        std::cout << base << "]: Upload check failure ===\n";
        std::cout << "Couldn't find in multi-part upload list\n";
        std::cout << "Found " << uploads.size() << " parts\n" <<
                     "Id was: " << uploads[0].GetUploadId() << "\n";
        // abort upload?
        goto PutReturnPt;
    }
    }
    **/
    // upload part
    /** LeoFS doesn't recognise the key provided earlier
    int Number = 1;
    String partETag;
    {
    auto id = objRes.GetResult().GetUploadId();
    auto uploadReq = Aws::S3::Model::UploadPartRequest();
    uploadReq.WithBucket(bucketName).WithKey(key).WithUploadId(id);;
    uploadReq.WithPartNumber(Number).SetBody(inpData);
    auto uploadRes = client->UploadPart(uploadReq);
    if (!uploadRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << "Couldn't list multi-part upload\n";
        std::cout << uploadRes.GetError().GetExceptionName() << "\t" <<
                     uploadRes.GetError().GetMessage() << "\n";
        // abort upload?
        goto PutReturnPt;
    }
    partETag = uploadRes.GetResult().GetETag();
    }
    // check if multipart exists
    {
    auto id = objRes.GetResult().GetUploadId();
    auto partReq = Aws::S3::Model::ListPartsRequest();
    partReq.WithBucket(bucketName).WithKey(key).WithUploadId(id);;
    auto partRes = client->ListParts(partReq);
    if (!partRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << "Couldn't list multi-part upload\n";
        std::cout << partRes.GetError().GetExceptionName() << "\t" <<
                     partRes.GetError().GetMessage() << "\n";
        // abort upload?
        goto PutReturnPt;
    }
    auto uploads = partRes.GetResult().GetParts();
    if (uploads.size() != 1 || uploads[0].GetPartNumber() != Number ||
        uploads[0].GetETag() != partETag)
    {
        std::cout << base << "]: Upload check failure ===\n";
        std::cout << "Couldn't find part in uploaded list\n";
        // abort upload?
        goto PutReturnPt;
    }
    }
    // finish upload
    {
    auto id = objRes.GetResult().GetUploadId();
    auto part = Aws::S3::Model::CompletedPart();
    auto parts = Aws::S3::Model::CompletedMultipartUpload();
    parts.AddParts(part.WithETag(partETag).WithPartNumber(Number));
    auto compReq = Aws::S3::Model::CompleteMultipartUploadRequest();
    compReq.WithBucket(bucketName).WithKey(key).WithUploadId(id);
    compReq.SetMultipartUpload(parts);
    auto compRes = client->CompleteMultipartUpload(compReq);
    if (!compRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << "Couldn't complete multi-part upload\n";
        std::cout << compRes.GetError().GetExceptionName() << "\t" <<
                     compRes.GetError().GetMessage() << "\n";
        goto PutReturnPt;
    }
    }
    **/
PutReturnPt:
    if (!std::get<0>(doesObjectExists(client, bucketName, key)))
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n\n";
}

bool doFilesMatch(Aws::FStream* a, Aws::IOStream& b, size_t min, size_t max)
{
    std::ifstream::pos_type size1, size2;
    if (min == max)
    {
        size1 = a->seekg(0, std::ifstream::end).tellg();
        a->seekg(0, std::ifstream::beg);
        size2 = b.seekg(0, std::ifstream::end).tellg();
        b.seekg(0, std::ifstream::beg);
    }
    else
    {
        // ensures that the file size is taken care of
        // off by 1 added coz aws starts counting from 1
        size1 = a->seekg(max + 1).tellg() - a->seekg(min).tellg();
        a->seekg(min);
        size2 = b.seekg(0, std::ifstream::end).tellg();
        b.seekg(0, std::ifstream::beg);
    }
    if (size1 != size2)
    {
        std::cout << "Buffer size is different (" << size1 << " vs " <<
                     size2 << ")\n";
        return false;
    }
    const size_t BLOCKSIZE = 4096;
    size_t remaining = size1;
    while (remaining)
    {
        char buffer1[BLOCKSIZE], buffer2[BLOCKSIZE];
        size_t size = std::min(BLOCKSIZE, remaining);

        a->read(buffer1, size);
        b.read(buffer2, size);

        if (memcmp(buffer1, buffer2, size))
        {
            std::cout << "Offset: " << (size_t(size1) - remaining) << "\n";
            for (size_t i = 0; i < size; ++i)
            {
                std::cout << uint16_t(buffer1[i]) << ":" <<
                             uint16_t(buffer2[i]) << "\t";
            }
            std::cout << "Buffer content is different\n";
            return false;
        }
        remaining -= size;
    }
    return true;
}

void print(Map map)
{
    std::cout << "MAP\n";
    for (auto it: map)
    {
        std::cout << it.first << '\t' << it.second << '\n';
    }
}

void getObject(ClientPtrType client, String bucketName, String key, String path, Map metadata)
{
    String base = "=== Get Object [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    std::cout << "Reading from " << path << "\n";
    auto inpData = Aws::MakeShared<Aws::FStream>("GetObjectInputStream",
            path.c_str(), std::ios_base::in | std::ios_base::binary);
    auto objReq = Aws::S3::Model::GetObjectRequest();
    objReq.WithBucket(bucketName).WithKey(key);
    auto objRes = client->GetObject(objReq);
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << objRes.GetError().GetExceptionName() << "\t" <<
                     objRes.GetError().GetMessage() << "\n";
        std::cout << base << "]: Failed ===\n";
    }
    else
    {
        if (!metadata.empty())
        {
            if (metadata != objRes.GetResult().GetMetadata())
            {
                std::cout << objRes.GetResult().GetMissingMeta() << "\n";
                print(metadata);
                print(objRes.GetResult().GetMetadata());
                std::cout << base << "]: Metadata not equal ===\n";
            }
        }
        Aws::IOStream& file = objRes.GetResult().GetBody();
        if (!doFilesMatch(inpData.get(), file))
        {
            std::cout << base << "]: Content not equal ===\n";
        }
    }
    std::cout << base << "]: End ===\n\n";
}

void getFakeObject(ClientPtrType client, String bucketName, String key)
{
    String base = "=== Get Fake Object [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    std::cout << "Reading from " << key << "\n";
    auto inpData = Aws::MakeShared<Aws::FStream>("GetObjectInputStream",
            key.c_str(), std::ios_base::in | std::ios_base::binary);
    auto objReq = Aws::S3::Model::GetObjectRequest();
    objReq.WithBucket(bucketName).WithKey(key);
    auto objRes = client->GetObject(objReq);
    if (objRes.IsSuccess())
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n\n";
}

void rangeObject(ClientPtrType client, String bucketName, String key, String path, size_t min, size_t max)
{
    String base = "=== Range Object [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    std::cout << "Reading from " << path << "\n";
    String range(("byte=" + std::to_string(min) + "-" + std::to_string(max)).c_str());
    auto inpData = Aws::MakeShared<Aws::FStream>("GetObjectInputStream",
            path.c_str(), std::ios_base::in | std::ios_base::binary);
    auto objReq = Aws::S3::Model::GetObjectRequest();
    objReq.WithBucket(bucketName).WithKey(key).WithRange(range);
    auto objRes = client->GetObject(objReq);
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
        std::cout << objRes.GetError().GetExceptionName() << "\t" <<
                     objRes.GetError().GetMessage() << "\n";
        std::cout << base << "]: Failed ===\n";
    }
    else
    {
        Aws::IOStream& file = objRes.GetResult().GetBody();
        if (!doFilesMatch(inpData.get(), file, min, max))
        {
            std::cout << base << "]: Content not equal ===\n";
        }
    }
    std::cout << base << "]: End ===\n\n";
}

void copyObject(ClientPtrType client, String bucketName, String src, String dst)
{
    String base = "=== Copy Object [" + bucketName + "/" + src + "->" + dst;
    std::cout << base << "]: Start ===\n";
    auto objReq = Aws::S3::Model::CopyObjectRequest();
    objReq.WithBucket(bucketName).WithCopySource(bucketName + "/" + src);
    auto objRes = client->CopyObject(objReq.WithKey(dst));
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
    }
    if (!std::get<0>(doesObjectExists(client, bucketName, dst)))
    {
        std::cout << base << "]: Failed ===\n";
    }
    std::cout << base << "]: End ===\n\n";
}

Aws::Vector<Object> listObjects(ClientPtrType client, String bucketName, String prefix, size_t expected, int maxKeys, bool debugCheck)
{
    String base = "=== List Object [" + bucketName + "/" + prefix;
    std::cout << base << "]: Start ===\n";
    auto objReq = Aws::S3::Model::ListObjectsRequest();
    objReq.WithBucket(bucketName).WithPrefix(prefix);;
    if (maxKeys)
    {
        objReq.SetMaxKeys(maxKeys);
    }
    String marker = "";
    size_t count = 0;
    Aws::S3::Model::ListObjectsOutcome objRes;
    do
    {
        objRes = client->ListObjects(objReq.WithMarker(marker));
        if (!objRes.IsSuccess())
        {
            std::cout << base << "]: Client Side failure ===\n";
        }
        count += objRes.GetResult().GetContents().size();
        if (!debugCheck)
        {
            for (Object it: objRes.GetResult().GetContents())
            {
                std::cout << "Name: " << it.GetKey() <<
                            "\tSize: " << it.GetSize() << "\n";
            }
            std::cout << "===== Page End =====\n";
        }
        marker = objRes.GetResult().GetNextMarker();
    } while (objRes.GetResult().GetIsTruncated());
    if (!debugCheck)
    {
        if (expected != size_t(-1))
        {
            if (expected != count)
            {
                std::cout << base << "]: Failed ===\n";
                std::cout << "Expected " << expected << " objects, got " <<
                            count << "\n";
            }
        }
    }
    std::cout << base << "]: End ===\n\n";
    return objRes.GetResult().GetContents();
}

void deleteObject(ClientPtrType client, String bucketName, String key)
{
    String base = "=== Delete Object [" + bucketName + "/" + key;
    std::cout << base << "]: Start ===\n";
    auto objReq = Aws::S3::Model::DeleteObjectRequest();
    objReq.WithBucket(bucketName);
    auto objRes = client->DeleteObject(objReq.WithKey(key));
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
    }
    if (std::get<0>(doesObjectExists(client, bucketName, key)))
    {
        std::cout << base << "]: Deletion of " << key << " Failed ===\n";
    }
    std::cout << base << "]: End ===\n\n";
}

void deleteObjects(ClientPtrType client, String bucketName, String prefix, size_t num)
{
    String base = "=== Delete Objects [" + bucketName + "/" + prefix;
    std::cout << base << "]: Start ===\n";
    Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
    for (size_t i = 0; i < num; ++i)
    {
        objects.push_back(Aws::S3::Model::ObjectIdentifier().WithKey(prefix + std::to_string(i).c_str()));
    }

    auto objReq = Aws::S3::Model::DeleteObjectsRequest();
    objReq.WithBucket(bucketName).WithDelete(Aws::S3::Model::Delete().WithObjects(objects));
    auto objRes = client->DeleteObjects(objReq);
    if (!objRes.IsSuccess())
    {
        std::cout << base << "]: Client Side failure ===\n";
    }/*
    if (std::get<0>(doesObjectExists(client, bucketName, key)))
    {
        std::cout << base << "]: Deletion of " << key << " Failed ===\n";
    }*/
    std::cout << base << "]: End ===\n\n";
}

void deleteAllObjects(ClientPtrType client, String bucketName)
{
    String base = "=== Delete All Objects [" + bucketName;
    std::cout << base << "]: Start ===\n";
    auto objects = listObjects(client, bucketName, "", -1, 0, true);
    for (auto obj: objects)
    {
        deleteObject(client, bucketName, obj.GetKey());
    }
    std::cout << base << "]: End ===\n\n";
}
