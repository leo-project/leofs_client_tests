<?php
require "vendor/autoload.php";
use Aws\Common\Enum\Region;
use Aws\S3\S3Client;

/* key ==> replace your Access_ID secret ==> replace your secret_key base_url ==> your leofs service address */

$client = S3Client::factory(array(
  "key" => "05236",
  "secret" => "802562235",
  "region" => Region::US_EAST_1,
  "scheme" => "http",
  'base_url' => 'http://localhost:8080'
));
try {
	echo "Bucket List\n";
	echo "------------\n\n";
// list buckets
	$buckets = $client->listBuckets()->toArray();
	foreach($buckets as $bucket){
	print_r($bucket);
	}
	print("\n\n");
	echo "Create New Bucket\n\n";
// create bucket
	$result = $client->createBucket(array(
	"Bucket" => "test"
	));

	echo "Put object into Bucket \n\n";
// PUT object
	$client->putObject(array(
		"Bucket" => "test",
		"Key" => "key-test",
		"Body" => "Hello, world!"
		));

	echo "Get object from Bucket";
// GET object
	$object = $client->getObject(array(
		"Bucket" => "test",
		"Key" => "key-test"
		));
	print($object->get("Body"));
	print("\n\n");

	echo "Head object\n\n " ;
// HEAD object
	$headers = $client->headObject(array(
		"Bucket" => "test",
		"Key" => "key-test"
		));
	print_r($headers->toArray());

	echo "delete object \n\n";
// DELETE object
	$client->deleteObject(array(
		"Bucket" => "test",
		"Key" => "key-test"
		));
// PUT file
    $client->putObject(array(
		"Bucket" => "test",
		"Key" => "README",
		"Body" => fopen('README', 'r')
		));
// HEAD object file
    $headers = $client->headObject(array(
       "Bucket" => "test",          "Key" => "README"        ));
    print_r($headers->toArray());
    print("\n\n");// GET object file
    $object = $client->getObject(array(
        "Bucket" => "test",
        "Key" => "README"
        "SaveAs" => "README.copy"
    ));
    print("\n\n")
// DELETE object file
	$client->deleteObject(array(
		"Bucket" => "test",
		"Key" => "README"
	));
	
	echo "delete bucket \n\n";
// delete bucket
	$result = $client->deleteBucket(array(
		"Bucket" => "test"
	));
	echo " object deleted \n\n ";
}
catch (\Aws\S3\Exception\S3Exception $e)
{
// Exeception messages
    echo $e->getMessage();
}
?>

