import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import org.jclouds.ContextBuilder;
import org.jclouds.s3.domain.ObjectMetadata;
import org.jclouds.s3.domain.ListBucketResponse;
import org.jclouds.s3.domain.DeleteResult;
import org.jclouds.s3.domain.CannedAccessPolicy;
import org.jclouds.s3.domain.AccessControlList;
import org.jclouds.s3.domain.AccessControlList.Grant;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;

import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;
import static org.jclouds.blobstore.options.GetOptions.Builder.range;
import static org.jclouds.s3.options.ListBucketOptions.Builder.withPrefix;
import static org.jclouds.s3.options.PutBucketOptions.Builder.withBucketAcl;

import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.jclouds.aws.s3.AWSS3Client;

import com.google.common.io.ByteSource;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

import org.apache.commons.io.IOUtils;

public class LeoFSTest {
    private static String host = "localhost";   // leo_gateway host
    private static Integer port = 8080;         // leo_gateway port

    private static String accessKeyId       = "05236";
    private static String secretAccessKey   = "802562235";

    private static String signVer   = "v4";

    private static String bucket    = "testjc";
    private static String tempData  = "../temp_data/";

    private static final String smallTestF  = tempData + "testFile";
    private static final String mediumTestF = tempData + "testFile.medium";
    private static final String largeTestF  = tempData + "testFile.large"; 
    private static BlobStore s3;
    private static BlobStoreContext s3c;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length > 0)
            signVer = args[0];
        if (args.length > 1) {
			host = args[1];
			port = Integer.parseInt(args[2]);
            bucket = args[3];
		}
        System.out.println(signVer);

        // Init
        init(signVer);
        createBucket(bucket);

        // Put Object Test
        putObject(bucket, "test.simple",    smallTestF);
        putObject(bucket, "test.medium",    mediumTestF);
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
        getObject(bucket, "test.medium",    mediumTestF);
        getObject(bucket, "test.large",     largeTestF);
        getObject(bucket, "test.large.mp",  largeTestF);

        // Get Object Again (Cache) Test
        getObject(bucket, "test.simple",    smallTestF);
        getObject(bucket, "test.simple.mp", smallTestF);
        getObject(bucket, "test.medium",    mediumTestF);
        getObject(bucket, "test.large",     largeTestF);

        // Get Not Exist Object Test
        getNotExist(bucket, "test.noexist");

/*
        // LeoFS does not reply "Last-Modified" for Range-GET

        // Range Get Object Test
        rangeObject(bucket, "test.simple",      smallTestF, 1, 4); 
        rangeObject(bucket, "test.simple.mp",   smallTestF, 1, 4); 
        rangeObject(bucket, "test.large",       largeTestF, 1048576, 10485760); 
        rangeObject(bucket, "test.large.mp",    largeTestF, 1048576, 10485760); 
*/
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
        Thread.sleep(3000); // Need to wait for syncing directory metadata
        pageListBucket(bucket, "list/", 35, 10);

        // Multiple Delete
        multiDelete(bucket, "list/", 10);

/*
        //Jclouds does not support directly set with Canned ACL
        //LeoFS only support Canned ACL
        
        // GET-PUT ACL
        setBucketAcl(bucket, "private");
        setBucketAcl(bucket, "public-read");
        setBucketAcl(bucket, "public-read-write");
*/
    }

    private static void init(String signVer) {
        Properties overrides = new Properties();
        overrides.setProperty("jclouds.s3.virtual-host-buckets", "false");
        s3c = ContextBuilder.newBuilder("aws-s3")
            .credentials(accessKeyId, secretAccessKey)
            .endpoint("http://" + host + ":" + port)
            .overrides(overrides)
            .buildView(BlobStoreContext.class);
        s3 = s3c.getBlobStore();
    }

    public static void createBucket(String bucketName) throws IOException {
        System.out.println("===== Create Bucket [" + bucketName + "] Start =====");
        s3.createContainerInLocation(null, bucketName);
        if (!s3.containerExists(bucketName)) {
            throw new IOException("Create Bucket [" + bucketName + "] Failed!");
        }
        System.out.println("===== Create Bucket End =====");
        System.out.println();
    }

    private static void doPutObject(String bucketName, String key, String path) throws IOException {
        File file = new File(path);
        ByteSource byteSource = Files.asByteSource(file);
        Payload payload = Payloads.newByteSourcePayload(byteSource);
        Blob blob = s3.blobBuilder(key)
            .payload(payload)
            .contentLength(byteSource.size())
            .build();
        s3.putBlob(bucketName, blob);
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
        File file = new File(path);
        ByteSource byteSource = Files.asByteSource(file);
        Payload payload = Payloads.newByteSourcePayload(byteSource);
        Blob blob = s3.blobBuilder(key)
            .payload(payload)
            .contentLength(byteSource.size())
            .build();
        s3.putBlob(bucketName, blob, multipart());
        if (!doesFileExist(bucketName, key)) {
            throw new IOException("Multipart Upload Object [" + bucketName + "/" + key + "] Failed!");
        }
        System.out.println("===== Multipart Upload Object End =====");
        System.out.println();
    }

    public static void headObject(String bucketName, String key, String path) throws IOException {
        System.out.println("===== Head Object [" + bucketName + "/" + key + "] Start =====");
        BlobMetadata meta = s3.blobMetadata(bucketName, key);
        String etag = meta.getETag().substring(1,33);
        long length = meta.getSize();
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
        Blob object = s3.getBlob(bucketName, key);
        InputStream stream = object.getPayload().openStream();
        if (!IOUtils.contentEquals(stream, new FileInputStream(path))) {
            throw new IOException("Content NOT Match!");
        }
        System.out.println("===== Get Object End =====");
        System.out.println();
    }

    public static void rangeObject(String bucketName, String key, String path, int start, int end) throws IOException {
        System.out.println("===== Range Get Object [" + bucketName + "/" + key + "] (" + start + "-" + end + ") Start =====");
        Blob object = s3.getBlob(bucketName, key, range(start, end));
        InputStream stream = object.getPayload().openStream();

        byte [] res = IOUtils.toByteArray(stream);

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
        Blob object = s3.getBlob(bucketName, key);
        if (object != null) {
            throw new IOException("Should NOT Exist!");
        }
        System.out.println("===== Get Not Exist Object End =====");
        System.out.println();
    }

    public static void copyObject(String bucketName, String src, String dst) throws IOException {
        System.out.println("===== Copy Object [" + 
                bucketName + "/" + src + "] -> [" + 
                bucketName + "/" + dst + "] Start =====");
        AWSS3Client s3cli = s3c.unwrapApi(AWSS3Client.class);
        s3cli.copyObject(bucketName, src, bucketName, dst);
        if (!doesFileExist(bucketName, dst)) {
            throw new IOException("Copy Object Failed!");
        }
        System.out.println("===== Copy Object End =====");
        System.out.println();
    }

    public static void listObject(String bucketName, String prefix, int expected) throws IOException {
        System.out.println("===== List Objects [" + bucketName + "/" + prefix + "*] Start =====");

        AWSS3Client s3cli = s3c.unwrapApi(AWSS3Client.class);
        ListBucketResponse objList = s3cli.listBucket(bucketName, withPrefix(prefix));
        int count = 0;
        for (ObjectMetadata obj : objList) {
            if (doesFileExist(bucketName, obj.getKey())) {
                System.out.println(obj.getKey() + " \t Size: " + obj.getContentMetadata().getContentLength());
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

    public static void putDummyObjects(String bucketName, String prefix, int total, String holder) throws IOException {
        for (int i = 0; i < total; i++) {
            doPutObject(bucketName, prefix + i, holder);
        }
    }

    public static void pageListBucket(String bucketName, String prefix, int total, int pageSize) throws IOException {
        System.out.println("===== Multiple Page List Objects [" + bucketName + "/" + prefix + "*] " + total + " Objs @" + pageSize + " Start =====");
        AWSS3Client s3cli = s3c.unwrapApi(AWSS3Client.class);
        int actualCount = 0;
        ListBucketResponse objList;
        String marker = "";
        do {
            System.out.println("===== Page =====");
            objList = s3cli.listBucket(bucketName, withPrefix(prefix).maxResults(pageSize).afterMarker(marker));
            for (ObjectMetadata obj : objList) {
                if (doesFileExist(bucketName, obj.getKey())) {
                    actualCount++;
                    System.out.println(obj.getKey() 
                            + " \t Size: " + obj.getContentMetadata().getContentLength()
                            + " \t Count: " + actualCount);
                }
            }
            marker = objList.getNextMarker();
        } while (objList.isTruncated());
        System.out.println("===== End =====");
        if (total != actualCount) {
            throw new IOException("Number of Objects NOT Match!");
        }
        System.out.println("===== Multiple Page List Objects End =====");
        System.out.println();
    }

    public static void deleteAllObjects(String bucketName) throws IOException {
        System.out.println("===== Delete All Objects [" + bucketName + "] Start =====");
        AWSS3Client s3cli = s3c.unwrapApi(AWSS3Client.class);
        ListBucketResponse objList = s3cli.listBucket(bucketName);
        for (ObjectMetadata obj : objList) {
            s3.removeBlob(bucketName, obj.getKey().substring(1));
        }

        System.out.println("===== Delete All Objects End =====");
        System.out.println();
    }

    public static void multiDelete(String bucketName, String prefix, int total) throws IOException{
        System.out.println("===== Multiple Delete Objects [" + bucketName + "/" + prefix + "] Start =====");
        List<String> delList = new ArrayList<String>();
        for (int i = 0; i < total; i++) { 
            delList.add(prefix + i);
        }
        AWSS3Client s3cli = s3c.unwrapApi(AWSS3Client.class);
        DeleteResult delRes = s3cli.deleteObjects(bucketName, delList);
        for (String deleted : delRes.getDeleted()) {
            System.out.println("Deleted " + bucketName + "/" + deleted);
        }
        if (delRes.getDeleted().size() != total) {
            throw new IOException("Number of Objects NOT Match!");
        }

        System.out.println("===== Multiple Delete Objects End =====");
        System.out.println();
    }

    public static void setBucketAcl(String bucketName, String permission) throws IOException {
        System.out.println("===== Set Bucket ACL [" + bucketName + "] (" + permission + ") Start =====");
        CannedAccessPolicy targetCAP;
        List<String> checkList = new ArrayList<String>();
        if (permission.equals("private")) {
            targetCAP = CannedAccessPolicy.PRIVATE;
            checkList.add("FULL_CONTROL");
        } else if (permission.equals("public-read")) {
            targetCAP = CannedAccessPolicy.PUBLIC_READ;
            checkList.add("READ");
            checkList.add("READ_ACP");
        } else if (permission.equals("public-read-write")) {
            targetCAP = CannedAccessPolicy.PUBLIC_READ_WRITE;
            checkList.add("READ");
            checkList.add("READ_ACP");
            checkList.add("WRITE");
            checkList.add("WRITE_ACP");
        } else {
            throw new IOException("Invalid Permission!");
        }
        AWSS3Client s3cli = s3c.unwrapApi(AWSS3Client.class);
        AccessControlList targetAcl = AccessControlList.fromCannedAccessPolicy(targetCAP, accessKeyId);
        s3cli.putBucketACL(bucketName, targetAcl);
        AccessControlList acl = s3cli.getBucketACL(bucketName);
        System.out.println("Owner ID: " + acl.getOwner().getId());

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

    private static boolean doesFileExist(String bucketName, String key) {
        return s3.blobExists(bucketName, key);
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
