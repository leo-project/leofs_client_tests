<?php
require "vendor/autoload.php";

use Aws\Common\Enum\Region;
use Aws\S3\S3Client;

// your key==> access_key_id  secret ==> your secret_key_id 

$client = S3Client::factory(array(
  "key" => "05236",
  "secret" => "802562235",
  "region" => Region::US_EAST_1,
  "scheme" => "http",
));

try {

// list buckets
	$buckets = $client->listBuckets()->toArray();

	foreach($buckets as $bucket)
	{
	  print_r($bucket);
	}
	print("\n\n");

// create bucket  "Bucket" => "YOUR_BUCKET_NAME"
	$result = $client->createBucket(array(
	  "Bucket" => "bucket2"
	));

// PUT object
	$client->putObject(array(
	  "Bucket" => "test",
	  "Key" => "key-test",
	  "Body" => "Hello, world!"
	));

// GET object
	$object = $client->getObject(array(
	  "Bucket" => "test",
	  "Key" => "key-test"
	));
	print($object->get("Body"));
	print("\n\n");

// HEAD object
	$headers = $client->headObject(array(
	  "Bucket" => "test",
	  "Key" => "key-test"
	));
	print_r($headers->toArray());

// DELETE object
	$client->deleteObject(array(
	  "Bucket" => "test",
	  "Key" => "key-test"
	));
}
catch (\Aws\S3\Exception\S3Exception $e) 
{
// The bucket couldn't be created
	 echo $e->getMessage();
}

?>
