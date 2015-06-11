import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

import org.apache.commons.io.IOUtils;
public class LeoFSSample {
    public static void main(String[] args) throws IOException {
        /* ---------------------------------------------------------
         * You need to set 'Proxy host', 'Proxy port' and 'Protocol'
         * --------------------------------------------------------- */
        ClientConfiguration config = new ClientConfiguration();
        config.setProxyHost("localhost"); // LeoFS Gateway's Host
        config.setProxyPort(8080);        // LeoFS Gateway's Port
        config.withProtocol(Protocol.HTTP);
        final String accessKeyId = "05236";
        final String secretAccessKey = "802562235";
        AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        AmazonS3 s3 = new AmazonS3Client(credentials, config);
        final String bucketName = "test";
        final String key = "test-key";
        final String fileName = "testFile";

        final Integer largeObjSize = 52428800;
        try {
            // Create a bucket
            System.out.println("Bucket Creation Test [Start]");
            s3.createBucket(bucketName);
            if ( s3.doesBucketExist(bucketName) ) {
                System.out.println("Bucket Created Successfully");
            } else {
                throw new IOException("Bucket Creation Failed");
            }

            // Retrieve list of buckets
            System.out.println("-----List Buckets----");
            for ( Bucket bucket : s3.listBuckets() ) {
                System.out.println("Bucket:" + bucket.getName() + "\t" + bucket.getCreationDate());
            }
            System.out.println("Bucket Creation Test [End]\n");

            System.out.println("Object Upload Test [Start]");

            // PUT an object into the LeoFS 
            s3.putObject(new PutObjectRequest(bucketName, key, createFile()));
            if ( !doesFileExist(s3, bucketName, key) ) {
                throw new IOException("Text File could not Created Successfully");
            } else {
                System.out.println("Successfully created text file");
            }

            // File Upload to LeoFS using single part upload method
            String filePath= "../temp_data/" + fileName;
            System.out.println("Uploading a new object to S3 from a file\n");
            File file = new File(filePath);
            s3.putObject(new PutObjectRequest(bucketName, file.getName()+".single", file));
            if ( !doesFileExist(s3, bucketName, fileName+".single") ) {
                throw new IOException("Single Part File could not Uploaded Successfully");
            } else {
                System.out.println("Single Part File Uploaded Successfully");
            }

            byte[] largeObj = new byte[largeObjSize];
            new Random().nextBytes(largeObj);
            InputStream largeObjStream = new ByteArrayInputStream(largeObj);
            ObjectMetadata largeObjMeta = new ObjectMetadata();
            largeObjMeta.setContentLength(largeObjSize);

            System.out.println("Uploading Single Part Large Object to S3");
            s3.putObject(new PutObjectRequest(bucketName, fileName + ".large.one", largeObjStream, largeObjMeta)); 
            if (!doesFileExist(s3, bucketName, fileName + ".large.one")) {
                throw new IOException("Single Part Large Object Upload Failed");
            } else {
                System.out.println("Single Part Large Object Upload Suceeded");
            }

            /* Multipart upload allows you to upload a single object as a set of parts.Each part is a contiguous
            portion of the object's data. You can upload these object parts independently and in any order. 
            If transmission of any part fails, you can retransmit that part without affecting other parts. 
            After all parts of your object are uploaded, LeoFS assembles these parts and creates the object. 
            In general, when your object size reaches 100 MB, you should consider using multipart uploads instead
            of uploading the object in a single operation. Advantages : Improved throughput, Quick recovery from 
            any network issues, Pause and resume object uploads Begin an upload before you know the final object size. */

            // File Upload to LeoFS using multipart upload method
            TransferManager tx = new TransferManager(s3);
            Upload upload = tx.upload(bucketName, file.getName(), file); 
            upload.waitForCompletion();
            tx.shutdownNow(Boolean.FALSE.booleanValue());
            if ( !doesFileExist(s3, bucketName, fileName) ) {
                throw new IOException("Multi-part File could not Uploaded Successfully");
            } else {
                System.out.println("File Uploaded Successfully");
            }

            System.out.println("Uploading Multi-part Large Object to S3");
            TransferManager txLarge = new TransferManager(s3);
            largeObjStream.reset();

            Upload uploadLarge = txLarge.upload(bucketName, fileName + ".large.part", largeObjStream, largeObjMeta);
            System.out.println("Transfer: " + uploadLarge.getDescription() + "\t" + "State" + uploadLarge.getState());

            // You can poll your transfer's status to check its progress
            while ( uploadLarge.isDone() == false ) {
                System.out.println(" - Progress: " + uploadLarge.getProgress().getBytesTransferred() 
                                      + "Byte \t" + uploadLarge.getProgress().getPercentTransferred() + "%" );
                Thread.sleep(100);
            } 
            uploadLarge.waitForCompletion();
            txLarge.shutdownNow(Boolean.FALSE.booleanValue());
            if ( !doesFileExist(s3, bucketName, fileName + ".large.part") ) {
                throw new IOException("Multi-part Large File Upload Failed");
            } else {
                System.out.println("Multi-part Large File Upload Suceeded");
            }

            System.out.println("Object Upload Test [End] \n");

            /* Files in Amazon S3 & LeoFS are called "objects" and are stored in buckets. A specific object is
            referred to by its key (i.e., name) and holds data. Here, we create a new object with
            the key name, HEAD request is Metadata of that object. e.g. Size, etag, Content_type etc.
            For more information http://boto.readthedocs.org/en/latest/s3_tut.html#storing-data */ 

            // Head Object
            System.out.println("HEAD Object Test [Start]");
            ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, fileName+".single");
            System.out.println("Single Part Metadata => Etag :" + objectMetadata.getETag() + " \tContentLength :" + objectMetadata.getContentLength() );
            if ( file.length() == objectMetadata.getContentLength() && objectMetadata.getETag() == MD5(filePath) ) {
                throw new IOException("Sigle Part File Metadata could not match");
            } else {
                System.out.println("Single Part File MetaData Test passed Successfully");
            }

            objectMetadata = s3.getObjectMetadata(bucketName, fileName);
            System.out.println("Multi Part Metadata => Etag :" + objectMetadata.getETag() + " \tContentLength :" + objectMetadata.getContentLength());
            if ( file.length() == objectMetadata.getContentLength() && objectMetadata.getETag() == MD5(filePath) ) {
                throw new IOException("Multi Part File Metadata could not match");
            } else {
                System.out.println("Multi Part File MetaData Test passed Successfully");
            }

            System.out.println("HEAD Object Test [End]\n");
            
            // File Download from LeoFS
            System.out.println("GET Object Test [Start]");
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, fileName));
            dumpInputStream(object.getObjectContent(),fileName+".copy");
            File newfile = new File(fileName+".copy");
            if ( file.length() != newfile.length() ) {
                throw new IOException("Downloaded File content-length is not equal");
            }
            System.out.println("GET Object Test [End]\n");

            // File Download not existing from LeoFS
            System.out.println("GET non-existing Object Test [Start]");
            try {
                S3Object object_tmp = s3.getObject(new GetObjectRequest(bucketName, fileName + ".noexist"));
                throw new IOException("Downloaded File must NOT be exist");
            } catch ( AmazonServiceException ase ) {
                System.out.println(ase.getMessage());
                System.out.println(ase.getStatusCode());
            }
            System.out.println("GET non-existing Object Test [End]\n");

            // Range Request from LeoFS
            System.out.println("Range Get Object Test [Start]");
            
            System.out.println("Range Get in Small Object");
            S3Object objectRange = s3.getObject(new GetObjectRequest(bucketName, fileName).withRange(1,4));
            BufferedReader readerRange = new BufferedReader(new InputStreamReader(objectRange.getObjectContent()));
            String lineRange = readerRange.readLine();
            if (!lineRange.equals("his ")) {
                System.out.println(lineRange);
                throw new IOException("Range Get Result does NOT match");
            }

            byte[] baseArr = Arrays.copyOfRange(largeObj, 1048576, 10485760 + 1);

            System.out.println("Range Get in Single Part Large Object");
            S3Object objectRangeLarge1 = s3.getObject(new GetObjectRequest(bucketName, fileName + ".large.one").withRange(1048576, 10485760));
            byte[] rangeBytes1 = IOUtils.toByteArray(objectRangeLarge1.getObjectContent());
            if (!Arrays.equals(baseArr, rangeBytes1)) {
                System.out.println("baseArr Size: " + baseArr.length);
                System.out.println("rangeBytes Size: " + rangeBytes1.length);
                throw new IOException("Range Get Result doe NOT match");
            }

            System.out.println("Range Get in Multi Part Large Object");
            S3Object objectRangeLarge2 = s3.getObject(new GetObjectRequest(bucketName, fileName + ".large.part").withRange(1048576, 10485760));
            byte[] rangeBytes2 = IOUtils.toByteArray(objectRangeLarge2.getObjectContent());
            if (!Arrays.equals(baseArr, rangeBytes2)) {
                System.out.println("baseArr Size: " + baseArr.length);
                System.out.println("rangeBytes Size: " + rangeBytes2.length);
                throw new IOException("Range Get Result doe NOT match");
            }

            System.out.println("Range Get Object Test [End]\n");

            // File copy bucket internally
            System.out.println("COPY Object Test [Start]");
            s3.copyObject( bucketName, file.getName(), bucketName, fileName+".copy");
            if (!doesFileExist(s3, bucketName, fileName+".copy") ) {
                throw new IOException("File could not copy Successfully");
            }
            System.out.println("File copied successfully");
            System.out.println("COPY Object Test [End]");

            // Retrieve list of objects from the LeoFS
            ObjectListing objectListing =
                s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
            System.out.println("-----List objects----");
            for ( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() ) {
                System.out.println(objectSummary.getKey() + " \t Size:" + objectSummary.getSize());
            }

            // DELETE an object from the LeoFS
            System.out.println("DELETE Object Test [Start]");
            for ( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() ) {
                s3.deleteObject(bucketName, objectSummary.getKey());
            }
            System.out.println("-----List objects----");
            for ( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() ) {
                if ( doesFileExist(s3,bucketName,objectSummary.getKey()) ) {
                    throw new IOException("Object Not Deleted: " + objectSummary.getKey());
                } else {
                    System.out.println(objectSummary.getKey() + " \t\t Deleted Successfully");
                }
            }
            
            System.out.println("ALL File deleted Successfully");
            System.out.println("DELETE Object Test [End]\n");

            // Delete multi layered directories
            System.out.println("DELETE multi layered directories Test [Start]");
            String baseDir = "a/b/c/";
            s3.putObject(new PutObjectRequest(bucketName, baseDir + "file", createFile()));
            s3.putObject(new PutObjectRequest(bucketName, baseDir + "file2", createFile()));
            ObjectListing objectListing2 =
                s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(baseDir));
            System.out.println("-----List objects----");
            for ( S3ObjectSummary objectSummary : objectListing2.getObjectSummaries() ) {
                System.out.println(objectSummary.getKey() + " \t Size:" + objectSummary.getSize());
            }
            
            baseDir = "a";
            s3.deleteObject(bucketName, baseDir);
            if ( doesFileExist(s3,bucketName,baseDir) ) {
                throw new IOException("Object Not Deleted");
            } else {
                System.out.println(baseDir + " \t\t Deleted Successfully");
            }
            System.out.println("DELETE multi layered directories Test [End]\n");

            // Listing objects across over mutiple pages
            System.out.println("Listing Objects Test [Start]");
            baseDir = "images/";
            int expectedCount = 1500;
            int actualCount = 0;
            List<S3ObjectSummary> keyList = new ArrayList<S3ObjectSummary>();
            for (int i = 0; i < expectedCount; i++) {
                s3.putObject(new PutObjectRequest(bucketName, baseDir + "file." + i, createFile()));
            }
            ObjectListing objectListing3 =
                s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(baseDir));
            System.out.println("-----List objects----");
            keyList.addAll(objectListing3.getObjectSummaries());
            while (objectListing3.isTruncated()) {
                objectListing3.setBucketName(bucketName);
                objectListing3 = s3.listNextBatchOfObjects(objectListing3);
                keyList.addAll(objectListing3.getObjectSummaries());
            }
            for (S3ObjectSummary objectSummary : keyList) {
                actualCount++;
                System.out.println(objectSummary.getKey() + " \t Size:" + objectSummary.getSize()
                                                              + " \t Count:" + actualCount);
            }
            if (actualCount != expectedCount) {
                throw new IOException("The number of objects returned by listing objects is different." +
                                    " expected:" + expectedCount +
                                    " actual:"   + actualCount);
            }
            System.out.println("Listing Objects Test [End]\n");

            // Multiple Delete Objects
            System.out.println("Multiple Delete Objects Test [Start]");
            List<String> delKeyList = new ArrayList<String>();
            String[] keyArrays = new String[1];
            for (int i = 0; i < 10; i++) {
                String delKey = "delete_test." + i;
                delKeyList.add(delKey);
                s3.putObject(new PutObjectRequest(bucketName, delKey, createFile()));
            }

            DeleteObjectsRequest delObjsReq = new DeleteObjectsRequest(bucketName).withKeys(delKeyList.toArray(keyArrays));
            DeleteObjectsResult delObjsRes = s3.deleteObjects(delObjsReq);
            List<DeleteObjectsResult.DeletedObject> delObjList = delObjsRes.getDeletedObjects();
            System.out.println(" expected:" + delKeyList.size() + " actual:" +  delObjList.size() + "\n");
            if (delObjList.size() != delKeyList.size()) {
                throw new IOException("The number of deleted objects is less than the number of delete requests." +
                                    " expected:" + delKeyList.size() +
                                    " actual:"   + delObjList.size());
            }
            System.out.println("Multiple Delete Objects Test [End]\n");

            // GET-PUT ACL
            System.out.println("Bucket ACL Test [Start]");
            System.out.println("#####Default ACL#####");
            AccessControlList acp = s3.getBucketAcl(bucketName);
            List<String> list = new ArrayList<String>();
            System.out.println("Owner ID : " + acp.getOwner());
            for ( Grant grant : acp.getGrants() ) {
                System.out.println("Grantee : " + grant.getGrantee() + " \t Permissions : " + grant.getPermission());
            list.add(grant.getPermission().toString());
            }
            if ( list.contains("FULL_CONTROL") ) {
                System.out.println("Bucket permission is private");
            } else {
                throw new IOException("Bucket permission is not private");
            }

            System.out.println("\n#####:public_read ACL#####");
            s3.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
            acp = s3.getBucketAcl(bucketName);
            list.clear();
            System.out.println("Owner ID : " + acp.getOwner());
            for ( Grant grant : acp.getGrants() ) {
                System.out.println("Grantee : " + grant.getGrantee() + " \t Permissions : " + grant.getPermission());
                list.add(grant.getPermission().toString());
            }
            if ( list.contains("READ") && list.contains("READ_ACP") ) {
                System.out.println("Bucket permission is public_read");
            } else {
                throw new IOException("Bucket permission is not public_read");
            }

            System.out.println("\n#####:public_read_write ACL#####"); 
            s3.setBucketAcl(bucketName, CannedAccessControlList.PublicReadWrite);
            acp = s3.getBucketAcl(bucketName);
            list.clear();
            System.out.println("Owner ID : " + acp.getOwner());
            for ( Grant grant : acp.getGrants() ) {
                System.out.println("Grantee : " + grant.getGrantee() + " \t Permissions : " + grant.getPermission());
                list.add(grant.getPermission().toString());
            } 
            if ( list.contains("READ") && list.contains("WRITE") && list.contains("READ_ACP") && list.contains("WRITE_ACP") ) {
                System.out.println("Bucket permission is public_read_write");
            } else {
                throw new IOException("Bucket permission is not public_read_write");
            }

            System.out.println("\n#####:private ACL#####"); 
            s3.setBucketAcl(bucketName, CannedAccessControlList.Private);
            acp = s3.getBucketAcl(bucketName);
            list.clear();
            System.out.println("Owner ID : " + acp.getOwner());
            for ( Grant grant : acp.getGrants() ) {
                System.out.println("Grantee : " + grant.getGrantee() + " \t Permissions : " + grant.getPermission());
                list.add(grant.getPermission().toString());
            } 
            if ( list.contains("FULL_CONTROL") ) {
                System.out.println("Bucket permission is private");
            } else {
                throw new IOException("Bucket permission is not private");
            }
            System.out.println("Bucket ACL Test [End]\n");

            // DELETE a bucket from the LeoFS
            System.out.println("DELETE Bucket Test [Start]");
            s3.deleteBucket(bucketName);
            System.out.println("Bucket deleted Successfully");
            System.out.println("DELETE Bucket Test [End]");
        } catch ( AmazonServiceException ase ) {
            System.out.println(ase.getMessage());
            System.out.println(ase.getStatusCode());
        } catch ( AmazonClientException ace ) {
            System.out.println(ace.getMessage());
        } catch ( InterruptedException ie ) {
            System.out.println(ie.getMessage());
        }
        
    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to LeoFS
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */

    private static File createFile() throws IOException {
        File file = File.createTempFile("leofs_test", ".txt");
        file.deleteOnExit();
        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("Hello, world!\n");
        writer.close();
        return file;
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     * The input stream to display as text.
     *
     * @throws IOException
     */

    private static void dumpInputStream(InputStream input,String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        File file=new File(fileName);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file));
        int read = -1;
        while ( ( read = (byte) reader.read() ) != -1 ) {
            writer.write(read);
        }
        writer.flush();
        writer.close();
        reader.close();
    }
    public static boolean doesFileExist( AmazonS3 s3, String bucketName, String key ) throws AmazonClientException, AmazonServiceException {
        boolean isValidFile = true;
        try {
            ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, key);
        } catch ( AmazonS3Exception s3e ) {
            if ( s3e.getStatusCode() == 404 ) {
                // i.e. 404: NoSuchKey - The specified key does not exist
                isValidFile = false;
            }
            else {
                throw s3e; // rethrow all S3 exceptions other than 404 
            }
        }
        catch ( Exception exception ) {
            exception.printStackTrace();
            isValidFile = false;
        }
        return isValidFile;
    }

    public static String MD5( String filePath )
    {
        StringBuffer sb = new StringBuffer();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            FileInputStream fis = new FileInputStream(filePath); 
            byte[] dataBytes = new byte[1024];

            int nread = 0; 
            while ( (nread = fis.read(dataBytes)) != -1 ) {
                md.update(dataBytes, 0, nread);
            };
            byte[] mdbytes = md.digest();

            //convert the byte to hex format method 1
            for (int i = 0; i < mdbytes.length; i++) {
                sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
            }

            System.out.println("Digest(in hex format):: " + sb.toString());
 
        } catch ( java.security.NoSuchAlgorithmException e ) {
            System.out.println(e.getMessage());
        } catch ( Exception e ) {
            System.out.println(e.getMessage());
        }
        return sb.toString();
    }

}
