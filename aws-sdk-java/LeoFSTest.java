// Java Built-in
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

// AWS-SDK-Java
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

// Extra
import org.apache.commons.io.IOUtils;

public class LeoFSTest {
    private static String host = "localhost";   // leo_gateway host
    private static Integer port = 8080;         // leo_gateway port

    private static String accessKeyId       = "05236";
    private static String secretAccessKey   = "802562235";

    private static String signVer   = "v4";

    private static String bucket    = "testj";
    private static String tempData  = "../temp_data/";

    private static final String smallTestF  = tempData + "testFile";
    private static final String largeTestF  = tempData + "testFile.large"; 

    private static ClientConfiguration config;
    private static AWSCredentials credentials;
    private static AmazonS3 s3;


    public static void main(String[] args) throws IOException, InterruptedException {
        // Init
        init(signVer);
        createBucket(bucket);

        // Put Object Test
        putObject(bucket, "test.simple",    smallTestF);
        putObject(bucket, "test.large",     largeTestF);

        // Multipart Upload Test
        mpObject(bucket, "test.simple.mp",  smallTestF);
        mpObject(bucket, "test.large.mp",   largeTestF);

        // Object Metadata Test
        headObject(bucket, "test.simple",   smallTestF);
        headObject(bucket, "test.simple.mp", smallTestF);
        headObject(bucket, "test.large",    largeTestF);
// MP File ETag != MD5
//        headObject(bucket, "test.large.mp", largeTestF);

        // Get Object Test
        getObject(bucket, "test.simple",    smallTestF);
        getObject(bucket, "test.simple.mp", smallTestF);
        getObject(bucket, "test.large",     largeTestF);
// MP File ETag != MD5
//        getObject(bucket, "test.large.mp",  largeTestF);

        // Get Not Exist Object Test
        getNotExist(bucket, "test.noexist");

        // Range Get Object Test
        rangeObject(bucket, "test.simple",      smallTestF, 1, 4); 
        rangeObject(bucket, "test.simple.mp",   smallTestF, 1, 4); 
        rangeObject(bucket, "test.large",       largeTestF, 1048576, 10485760); 
        rangeObject(bucket, "test.large.mp",    largeTestF, 1048576, 10485760); 

        // Copy Object Test
        copyObject(bucket, "test.simple", "test.simple.copy");
        getObject(bucket, "test.simple.copy", smallTestF);

        // List Object Test
        listObject(bucket, "", -1);

        // Delete All Object Test
        deleteAllObjects(bucket);
        listObject(bucket, "", 0);

        // Multiple Page List Object Test
        putDummyObjects(bucket, "list/", 35, smallTestF);
        pageListBucket(bucket, "list/", 35, 10);

        // Multiple Delete
        multiDelete(bucket, "list/", 10);

        // GET-PUT ACL
        setBucketAcl(bucket, "private");
        setBucketAcl(bucket, "public-read");
        setBucketAcl(bucket, "public-read-write");
    }

    public static void init(String SignVer) {
        System.out.println("----- Init Start -----");
        config = new ClientConfiguration();
        config.setProxyHost(host);
        config.setProxyPort(port);
        config.withProtocol(Protocol.HTTP);
        if (SignVer.equals("v4")) {
            config.setSignerOverride("AWSS3V4SignerType");
        }
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        s3 = new AmazonS3Client(credentials, config);
        System.out.println("----- Init End -----");
        System.out.println();
    }

    public static void createBucket(String bucketName) throws IOException {
        System.out.println("===== Create Bucket [" + bucketName + "] Start =====");
        s3.createBucket(bucketName);
        if (!s3.doesBucketExist(bucketName)) {
            throw new IOException("Create Bucket [" + bucketName + "] Failed!");
        }
        System.out.println("===== Create Bucket End =====");
        System.out.println();
    }

    private static void doPutObject(String bucketName, String key, String path) {
        File file = new File(path);
        s3.putObject(new PutObjectRequest(bucketName, key, file));
    }

    public static void putObject(String bucketName, String key, String path) throws IOException {
        System.out.println("===== Put Object [" + bucketName + "/" + key + "] Start =====");
        doPutObject(bucketName, key, path);
        if (!doesFileExist(bucketName, key)) {
            throw new IOException("Put Object [" + bucketName + "/" + key + "] Failed!");
        }
        System.out.println("===== Put Object End =====");
        System.out.println();
    }

    public static void mpObject(String bucketName, String key, String path) throws IOException, InterruptedException {
        System.out.println("===== Multipart Upload Object [" + bucketName + "/" + key + "] Start =====");
        TransferManager tx = new TransferManager(s3);
        BufferedInputStream bufferedStream = new BufferedInputStream(new FileInputStream(path));

        File file = new File(path);
        long fileSize = file.length();
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(fileSize);
        Upload upload = tx.upload(bucketName, key, bufferedStream, meta); 
        while (upload.isDone() == false) {
            System.out.println(" - Progress: " + upload.getProgress().getBytesTransferred() 
                                  + "Byte \t" + upload.getProgress().getPercentTransferred() + "%" );
            Thread.sleep(100);
        } 
        upload.waitForCompletion();
        tx.shutdownNow(Boolean.FALSE.booleanValue());
        if (!doesFileExist(bucketName, key)) {
            throw new IOException("Multipart Upload Object [" + bucketName + "/" + key + "] Failed!");
        }
        System.out.println("===== Multipart Upload Object End =====");
        System.out.println();
    }

    public static void headObject(String bucketName, String key, String path) throws IOException {
        System.out.println("===== Head Object [" + bucketName + "/" + key + "] Start =====");
        ObjectMetadata meta = s3.getObjectMetadata(bucketName, key);
        String etag = meta.getETag();
        long length = meta.getContentLength();
        File file = new File(path);
        String md5 = MD5(path);
        System.out.println("ETag: " + etag + ", Size: " + length);
        if (file.length() != length || !md5.equals(etag)) {
            throw new IOException("Metadata [" + bucketName + "/" + key + "] NOT Match, Size: " + file.length() + ", MD5: " + md5);
        }
        System.out.println("===== Head Object End =====");
        System.out.println();
    }

    public static void getObject(String bucketName, String key, String path) throws IOException {
        System.out.println("===== Get Object [" + bucketName + "/" + key + "] Start =====");
        S3Object object = s3.getObject(bucketName, key);
        if (!IOUtils.contentEquals(object.getObjectContent(), new FileInputStream(path))) {
            throw new IOException("Content NOT Match!");
        }
        System.out.println("===== Get Object End =====");
        System.out.println();
    }

    public static void rangeObject(String bucketName, String key, String path, int start, int end) throws IOException {
        System.out.println("===== Range Get Object [" + bucketName + "/" + key + "] (" + start + "-" + end + ") Start =====");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key).withRange(start, end));
        byte [] res = IOUtils.toByteArray(object.getObjectContent());

        RandomAccessFile raf = new RandomAccessFile(path, "r");
        raf.seek(start);
        byte [] tmp = new byte[end - start + 1];
        raf.readFully(tmp);

        if (!Arrays.equals(res, tmp)) {
            throw new IOException("Content NOT Match!");
        }
        System.out.println("===== Range Get Object End =====");
        System.out.println();
    }

    public static void getNotExist(String bucketName, String key) throws IOException {
        System.out.println("===== Get Not Exist Object [" + bucketName + "/" + key + "] Start =====");
        try {
            S3Object object = s3.getObject(bucketName, key);
            throw new IOException("Should NOT Exist!");
        } catch (AmazonS3Exception s3e) {
            int code = s3e.getStatusCode();
            if (code != 403 && code != 404) {
                throw new IOException("Incorrect Status Code [" + code + "]!");
            }
        }
        System.out.println("===== Get Not Exist Object End =====");
        System.out.println();
    }

    public static void copyObject(String bucketName, String src, String dst) throws IOException {
        System.out.println("===== Copy Object [" + 
                bucketName + "/" + src + "] -> [" + 
                bucketName + "/" + dst + "] Start =====");
        s3.copyObject(bucketName, src, bucketName, dst);
        if (!doesFileExist(bucketName, dst)) {
            throw new IOException("Copy Object Failed!");
        }
        System.out.println("===== Copy Object End =====");
        System.out.println();
    }

    public static void listObject(String bucketName, String prefix, int expected) throws IOException {
        System.out.println("===== List Objects [" + bucketName + "/" + prefix + "*] Start =====");
        ObjectListing objList = s3.listObjects(bucketName, prefix);
        int count = 0;
        for (S3ObjectSummary objectSummary : objList.getObjectSummaries()) {
            if (doesFileExist(bucketName, objectSummary.getKey())) {
                System.out.println(objectSummary.getKey() + " \t Size:" + objectSummary.getSize());
                count ++;
            }
        }
        if (expected >= 0) {
            if (count != expected) {
                throw new IOException("Number of Objects NOT Match!");
            }
        }

        System.out.println("===== List Objects End =====");
        System.out.println();
    }

    public static void deleteAllObjects(String bucketName) throws IOException {
        System.out.println("=====  Delete All Objects [" + bucketName + "] Start =====");
        ObjectListing objList = s3.listObjects(bucketName, "");
        for (S3ObjectSummary objectSummary : objList.getObjectSummaries()) {
            s3.deleteObject(bucketName, objectSummary.getKey());
        }

        System.out.println("===== Delete All Objects End =====");
        System.out.println();
    }

    public static void putDummyObjects(String bucketName, String prefix, int total, String holder) throws IOException {
        for (int i = 0; i < total; i++) {
            doPutObject(bucketName, prefix + i, holder);
        }
    }

    public static void pageListBucket(String bucketName, String prefix, int total, int pageSize) throws IOException {
        System.out.println("===== Multiple Page List Objects [" + bucketName + "/" + prefix + "*] " + total + " Objs @" + pageSize + " Start =====");
        ObjectListing objList = s3.listObjects(new ListObjectsRequest(bucketName, prefix, null, null, pageSize));
        int actualCount = 0;
        while(true){
            System.out.println("===== Page =====");
            for (S3ObjectSummary objectSummary : objList.getObjectSummaries()) {
                actualCount++;
                System.out.println(objectSummary.getKey() 
                        + " \t Size:" + objectSummary.getSize()
                        + " \t Count:" + actualCount);
            }
            if (!objList.isTruncated()) {
                break;
            } else {
                objList.setBucketName(bucketName);
                objList = s3.listNextBatchOfObjects(objList);
            }
        }
        System.out.println("===== End =====");
        if (total != actualCount) {
            throw new IOException("Number of Objects NOT Match!");
        }
        System.out.println("===== Multiple Page List Objects End =====");
        System.out.println();
    }

    public static void multiDelete(String bucketName, String prefix, int total) throws IOException{
        System.out.println("===== Multiple Delete Objects [" + bucketName + "/" + prefix + "] Start =====");
        List<String> delList = new ArrayList<String>();
        for (int i = 0; i < total; i++) { 
            delList.add(prefix + i);
        }
        String [] delKeys = delList.toArray(new String[total]);
        DeleteObjectsResult delRes = s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(delKeys));
        List<DeleteObjectsResult.DeletedObject> delObjList = delRes.getDeletedObjects();
        for (DeleteObjectsResult.DeletedObject delObj : delObjList) {
            System.out.println("Deleted " + bucketName + "/" + delObj.getKey());
        }
        if (delObjList.size() != total) {
            throw new IOException("Number of Objects NOT Match!");
        }

        System.out.println("===== Multiple Delete Objects End =====");
        System.out.println();
    }

    public static void setBucketAcl(String bucketName, String permission) throws IOException {
        System.out.println("===== Set Bucket ACL [" + bucketName + "] (" + permission + ") Start =====");
        CannedAccessControlList targetAcl;
        List<String> checkList = new ArrayList<String>();
        if (permission.equals("private")) {
            targetAcl = CannedAccessControlList.Private;
            checkList.add("FULL_CONTROL");
        } else if (permission.equals("public-read")) {
            targetAcl = CannedAccessControlList.PublicRead;
            checkList.add("READ");
            checkList.add("READ_ACP");
        } else if (permission.equals("public-read-write")) {
            targetAcl = CannedAccessControlList.PublicReadWrite;
            checkList.add("READ");
            checkList.add("READ_ACP");
            checkList.add("WRITE");
            checkList.add("WRITE_ACP");
        } else {
            throw new IOException("Invalid Permission!");
        }
        s3.setBucketAcl(bucketName, targetAcl);
        AccessControlList acl = s3.getBucketAcl(bucketName);
        System.out.println("Owner ID: " + acl.getOwner());
        List<String> list = new ArrayList<String>();
        for (Grant grant : acl.getGrants()) {
            System.out.println("Grantee : " + grant.getGrantee() + " \t Permissions: " + grant.getPermission()); 
            list.add(grant.getPermission().toString());
        }
        for (String checkItem : checkList) {
            if (!list.contains(checkItem)) {
                throw new IOException("ACL NOT Match!");
            }
        }

        System.out.println("===== Set Bucket ACL End =====");
        System.out.println();
    }

    public static boolean doesFileExist(String bucketName, String key ) throws AmazonClientException{
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
 
        } catch ( java.security.NoSuchAlgorithmException e ) {
            System.out.println(e.getMessage());
        } catch ( Exception e ) {
            System.out.println(e.getMessage());
        }
        return sb.toString();
    }
}
