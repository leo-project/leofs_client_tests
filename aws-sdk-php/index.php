<?php
// Include the SDK using the Composer autoloader
require "vendor/autoload.php";
use Aws\Common\Enum\Region;
use Aws\S3\S3Client;
use Aws\S3\Model\MultipartUpload\UploadBuilder;
use Aws\Common\Exception\MultipartUploadException;

ini_set("memory_set",-1);
$bucket_name="test";
$file_name="testFile";

/* key ==> replace your Access_ID, secret ==> replace your secret_key, base_
url ==> your leofs service address */

$client = S3Client::factory(array(
    "key" => "05236",
    "secret" => "802562235",
    "region" => Region::US_EAST_1,
    "scheme" => "http",
    "base_url" => "http://localhost:8080"
));

try {
    // Create bucket
    print "Bucket Creation Test [Start]\n";
    $result = $client->createBucket(array("Bucket" => $bucket_name));
    print "Bucket Created Successfully \n";

    // Show buckets
    print "--------------Bucket List---------\n";
    $result = $client->listBuckets();
    foreach($result["Buckets"] as $bucket) {
        print "{$bucket["Name"]} -\t {$bucket["CreationDate"]}\n";
    }
    print "Bucket Creation Test [End]\n\n";

    // PUT object
    print "Object Upload Test [Start]\n";
    $file_path = "../temp_data/".$file_name;
    $file_size = filesize($file_path);
    $file_type = mime_content_type($file_path);

    // PUT Single-Part upload Object
    print "Single-Part File is being upload:\n";
    $client->putObject(array("Bucket" => $bucket_name, "Key" => $file_name.".single", "Body" => fopen($file_path, "r")));
    if(!$client->doesObjectExist($bucket_name, $file_name.".single")) {
         throw new Exception("Single-part File could not Uploaded Successfully");
    }
    print "Single-Part File Uploaded Successfully\n";

    /* Multipart upload allows you to upload a single object as a set of parts.Each part is a contiguous
    portion of the object's data. You can upload these object parts independently and in any order. 
    If transmission of any part fails, you can retransmit that part without affecting other parts. 
    After all parts of your object are uploaded, LeoFS assembles these parts and creates the object. 
    In general, when your object size reaches 100 MB, you should consider using multipart uploads instead
    of uploading the object in a single operation. Advantages : Improved throughput, Quick recovery from 
    any network issues, Pause and resume object uploads Begin an upload before you know the final object size. */

    // PUT Multi-Part Upload Object
    print "Multi-Part file is being Upload:\n";
    $uploader = UploadBuilder::newInstance()
    ->setClient($client)
    ->setSource($file_path)
    ->setBucket($bucket_name)
    ->setKey($file_name)
    ->setOption("CacheControl", "max-age=3600")
    ->build();
    $uploader->upload();
    if(!$client->doesObjectExist($bucket_name, $file_name)) {
         throw new Exception("Multi-Part File could not Uploaded Successfully");
    }
    print "Multi-Part File Uploaded Successfully\n";

    // List Objects
    print "----------------------List Objects----------------------\n";
    $iterator = $client->getIterator("ListObjects", array("Bucket" => $bucket_name));
    foreach($iterator as $object) {
        if(!$file_size == $object["Size"]){
            throw new Exception("Content length is changed for :".$object["Key"]);
        }
        print $object["Key"]."\t".$object["Size"]."\t".$object["LastModified"]."\n";
    }
    print "Object Upload Test [End]\n\n";
   
    /* Files in Amazon S3 & LeoFS are called "objects" and are stored in buckets. A specific object is
    referred to by its key (i.e., name) and holds data. Here, we create a new object with
    the key name, HEAD request is Metadata of that object. e.g. Size, etag, Content_type etc.
    For more information http://boto.readthedocs.org/en/latest/s3_tut.html#storing-data */

    // HEAD Object
    print "HEAD Object Test [Start]\n";
    print "Single Part File MetaData :";
    $headers = $client->headObject(array("Bucket" => $bucket_name, "Key" => $file_name.".single"));
    if(!($file_size == $headers["ContentLength"])
        && (!strcmp(md5_file($file_path), trim($headers["ETag"],"\"")))) {
        throw new Exception("Sigle Part File Metadata could not match");
    }
    print_r($headers->toArray());
    print "Single Part File MetaData Test passed Successfully\n";
    print "Multi part File MetaData :";
    $headers = $client->headObject(array("Bucket" => $bucket_name, "Key" => $file_name));
    if(!($file_size == $headers["ContentLength"])
        && (!strcmp(md5_file($file_path), trim($headers["ETag"],"\"")))) {
        throw new Exception("Multi Part File Metadata could not match");
    }
    print_r($headers->toArray());
    print "Multi Part File MetaData Test passed Successfully\n";
    print "HEAD Object Test [End]\n\n";

    // GET Object
    print "GET Object Test [Start]\n";
    $object = $client->getObject(array("Bucket" => $bucket_name, "Key" => $file_name.".single"));
    if(!$file_size == $object["ContentLength"]) {
        throw new Exception("Single Part Upload File content is not equal\n");
    }
    $object = $client->getObject(array("Bucket" => $bucket_name, "Key" => $file_name));
    if(!$file_size == $object["ContentLength"]) {
        throw new Exception("Multi Part Upload File content is not equal\n");
    }
    if(!strcmp($file_type, "text/plain")) {
        print "Multi Part Upload Object Data:".$object->get("Body")."\n";
    } else {
        print "Multi Part Upload Object Content type is :".$file_type."\n";
    }
    print "Get Object Test passed Successfully\n";
    print "GET Object Test [End]\n\n";

    // Download Object
    print "Download Object Test [Start]\n";
    $object = $client->getObject(array("Bucket" => $bucket_name, "Key" => "testFile", "SaveAs" => $file_name.".copy"));
    if(!(filesize($file_path) == filesize($file_name.".copy"))
        && (!strcmp(md5_file($file_path),md5_file($file_name.".copy")))) {
        throw new Exception("Downloaded File MetaData Could not match\n");
    }
    print "File Successfully downloaded\n";
    print "Download Object Test [End]\n\n";

    // COPY Object
    print "Copy Object Test [Start]\n";
    $result = $client->copyObject(array("Bucket" => $bucket_name, "CopySource" => "/{$bucket_name}/".$file_name, "Key" => $file_name.".copy"));
    if(!$client->doesObjectExist($bucket_name, $file_name.".copy")){
        throw new Exception("File could not Copy Successfully\n");
    }
    print "File copied successfully\n";

    // List Objects
    print "--------------------List Objects----------------- \n";
    $iterator = $client->getIterator("ListObjects", array("Bucket" => $bucket_name));
    foreach($iterator as $object) {
        if(!$file_size == $object["Size"]) {
            throw new Exception("Content length is changed for :".$object["Key"]);
        }
        print $object["Key"]."\t".$object["Size"]."\t".$object["LastModified"]."\n";
    }
    print "COPY Object Test [End]\n\n";

    // DELETE Object
    print "DELETE Object Test [Start]\n";
    print "--------------------Deleted Objects----------------- \n";
    $iterator = $client->getIterator("ListObjects", array("Bucket" => $bucket_name));
    foreach($iterator as $object) {
        $client->deleteObject(array( "Bucket" => $bucket_name, "Key" => $object["Key"]));
        if($client->doesObjectExist($bucket_name, $object["Key"])){
            throw new Exception( $object["Key"]."\tFile could not Deleted Successfully");
        }
        print $object["Key"]."\t"."File Deleted Successfully\n";
    }
    print "DELETE Object Test [End]\n\n";

    // GET-PUT ACL
    print "Bucket ACL Test [Start]\n";
    print "#####Default ACL#####\n";
    $acp = $client->getBucketAcl(array("Bucket" => $bucket_name));
    print "Owner ID : ".$acp["Owner"]["ID"]."\n";
    print "Owner Display Name : ".$acp["Owner"]["DisplayName"]."\n";
    $permissions = array();
    foreach($acp["Grants"] as $grant) {
        print "Bucket ACL is : ".$grant["Permission"]."\n";
        print "Bucket Grantee URI is : ".$grant["Grantee"]["URI"]."\n";
        array_push($permissions,$grant["Permission"]);
    }
    if(!in_array("FULL_CONTROL", $permissions)) {
        throw new Exception("Permission is Not private");
    } else {
        print "Bucket ACL permission is 'private'\n\n";
    }

    print "#####:public_read ACL#####\n";
    $client->putBucketAcl(array("ACL" => "public-read", "Bucket" => $bucket_name));
    $acp = $client->getBucketAcl(array("Bucket" => $bucket_name));
    print "Owner ID : ".$acp["Owner"]["ID"]."\n";
    print "Owner Display Name : ".$acp["Owner"]["DisplayName"]."\n";
    $permissions = array();
    foreach($acp["Grants"] as $grant) {
        print "Bucket ACL is : ".$grant["Permission"]."\n";
        print "Bucket Grantee URI is : ".$grant["Grantee"]["URI"]."\n";
        array_push($permissions,$grant["Permission"]);
    }
    if(!(in_array("READ", $permissions) && in_array("READ_ACP", $permissions))) {
        throw new Exception("Permission is Not public_read");
    } else {
        print "Bucket ACL Successfully changed to 'public-read'\n\n";
    }

    print "#####:public_read_write ACL#####\n";
    $client->putBucketAcl(array("ACL" => "public-read-write", "Bucket" => $bucket_name));
    $acp = $client->getBucketAcl(array("Bucket" => $bucket_name));
    print "Owner ID : ".$acp["Owner"]["ID"]."\n";
    print "Owner Display Name : ".$acp["Owner"]["DisplayName"]."\n";
    $permissions = array();
    foreach($acp["Grants"] as $grant) {
        print "Bucket ACL is : ".$grant["Permission"]."\n";
        print "Bucket Grantee URI is : ".$grant["Grantee"]["URI"]."\n";
        array_push($permissions,$grant["Permission"]);
    }
    if(!(in_array("READ", $permissions) && in_array("WRITE", $permissions)
        && in_array("READ_ACP", $permissions) && in_array("WRITE_ACP", $permissions))) {
        throw new Exception("Permission is Not public_read_write");
    } else {
        print "Bucket ACL Syccessfully changed to 'public-read-write'\n\n";
    }

    print "#####:private ACL#####\n";
    $client->putBucketAcl(array("ACL" => "private", "Bucket" => $bucket_name));
    $acp = $client->getBucketAcl(array("Bucket" => $bucket_name));
    print "Owner ID : ".$acp["Owner"]["ID"]."\n";
    print "Owner Display Name : ".$acp["Owner"]["DisplayName"]."\n";
    $permissions = array();
    foreach($acp["Grants"] as $grant) {
        print "Bucket ACL is : ".$grant["Permission"]."\n";
        print "Bucket Grantee URI is : ".$grant["Grantee"]["URI"]."\n";
        array_push($permissions,$grant["Permission"]);
    }
    if(!in_array("FULL_CONTROL", $permissions)) {
        throw new Exception("Permission is Not private");
    } else {
        print "Bucket ACL Syccessfully changed to 'private'\n";
    }
    print "Bucket ACL Test [End]\n\n";

    // DELETE Bucket
    print "DELETE Bucket Test [Start]\n";
    $result = $client->deleteBucket(array("Bucket" => $bucket_name));
    print "Bucket Deleted Successfully\n";
    print "DELETE Bucket Test [End]\n\n";
} catch (\Aws\S3\Exception\S3Exception $e){
    // Exception messages
    print $e->getMessage();
} catch (MultipartUploadException $e) {
    $uploader->abort();
    print "Multi Part Upload failed\n";
} catch (Exception $e) {
    print $e->getMessage();
}
?>
