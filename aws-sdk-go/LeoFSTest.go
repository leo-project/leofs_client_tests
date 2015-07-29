package main

import (
        "log"
        "os"
        "crypto/md5"
        "fmt"
        "io"
        "io/ioutil"
        "bufio"
        "bytes"
        "strings"

        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/aws/awserr"
        "github.com/aws/aws-sdk-go/aws/credentials"
        "github.com/aws/aws-sdk-go/service/s3"
        "github.com/aws/aws-sdk-go/service/s3/s3manager"
       )

const (
        Host    = "localhost"
        Port    = 8080

        AccessKeyId     = "05236"
        SecretAccessKey = "802562235"

        Bucket      = "testg"
        TempData    = "../temp_data/"

        SmallTestF  = TempData + "testFile"
        LargeTestF  = TempData + "testFile.large"
      )

var svc *s3.S3

func main() {
    initS3()
    createBucket(Bucket)

    // Put Object Test
    putObject(Bucket, "test.simple",    SmallTestF)
    putObject(Bucket, "test.large",     LargeTestF)

    // Multipart Upload Object Test
    mpObject(Bucket, "test.simple.mp",  SmallTestF)
    mpObject(Bucket, "test.large.mp",   LargeTestF)

    // Head Object Test
    headObject(Bucket, "test.simple",   SmallTestF)
    headObject(Bucket, "test.simple.mp",SmallTestF)
    headObject(Bucket, "test.large",    LargeTestF)

    // Get Object Test
    getObject(Bucket, "test.simple",    SmallTestF)
    getObject(Bucket, "test.simple.mp", SmallTestF)
    getObject(Bucket, "test.large",     LargeTestF)
    getObject(Bucket, "test.large.mp",  LargeTestF)

    // Get Not Exist Object Test
    getNotExist(Bucket, "test.noexist")

    // Range Get Object Test
    rangeObject(Bucket, "test.simple",      SmallTestF, 1, 4)
    rangeObject(Bucket, "test.simple.mp",   SmallTestF, 1, 4)
    rangeObject(Bucket, "test.large",       LargeTestF, 1048576, 10485760)
    rangeObject(Bucket, "test.large.mp",    LargeTestF, 1048576, 10485760)

    // Copy Object Test
    copyObject(Bucket, "test.simple", "test.simple.copy")
    getObject(Bucket, "test.simple.copy", SmallTestF)

    // List Object Test
    listObject(Bucket, "", -1)

    // Delete All Object Test
    deleteAllObjects(Bucket)
    listObject(Bucket, "", 0)

    // Multiple Page List Object Test
    putDummyObjects(Bucket, "list/", 35, SmallTestF)
    pageListBucket(Bucket, "list/", 35, 10)

    // Multiple Delete
    multiDelete(Bucket, "list/", 10)

    // GET-PUT ACL
    setBucketAcl(Bucket, "private")
    setBucketAcl(Bucket, "public-read")
    setBucketAcl(Bucket, "public-read-write")
}

func initS3() {
    cred := credentials.NewStaticCredentials(AccessKeyId, SecretAccessKey, "")
    svc = s3.New(&aws.Config{
            Endpoint    : fmt.Sprintf("http://%s:%d", Host, Port),
            Region      : "us-west-2",
            Credentials : cred,
            LogLevel    : 0})
}

func createBucket(bucketName string) {
    log.Printf("===== Create Bucket [%s] Start =====\n", bucketName)
    bucketStr := aws.String(bucketName)
    _, err := svc.CreateBucket(&s3.CreateBucketInput{Bucket: bucketStr})
    if err != nil {
        log.Fatalln(err)
    }
    log.Println("===== Create Bucket End =====")
    log.Println()
}

func putObject(bucketName, key, path string) {
    log.Printf("===== Put Object [%s/%s] Start =====\n", bucketName, key)
    bucketStr := aws.String(bucketName)
    keyStr := aws.String(key)
    reader, _ := os.Open(path)
    defer reader.Close()
    _, err := svc.PutObject(&s3.PutObjectInput{
        Bucket  : bucketStr,
        Key     : keyStr,
        Body    : reader,
    })
    if err != nil {
        log.Fatalln(err)
    }
    if !doesFileExist(bucketName, key) {
        log.Fatalf("Put Object [%s/%s] Failed!\n", bucketName, key)
    }
    log.Println("===== Put Object End =====")
    log.Println()
}

func mpObject(bucketName, key, path string) {
    log.Printf("===== Multipart Upload Object [%s/%s] Start =====\n", bucketName, key)
    bucketStr := aws.String(bucketName)
    keyStr := aws.String(key)
    reader, _ := os.Open(path)
    defer reader.Close()
    uploader := s3manager.NewUploader(&s3manager.UploadOptions{S3 : svc})
    _, err := uploader.Upload(&s3manager.UploadInput{
        Bucket  : bucketStr,
        Key     : keyStr,
        Body    : reader,
    })
    if err != nil {
        log.Fatalln(err)
    }
    if !doesFileExist(bucketName, key) {
        log.Fatalf("Multipart Upload Object [%s/%s] Failed!\n", bucketName, key)
    }
    log.Println("===== Multipart Upload Object End =====")
    log.Println()

}

func headObject(bucketName, key, path string) {
    log.Printf("===== Head Object [%s/%s] Start =====\n", bucketName, key)
    bucketStr := aws.String(bucketName)
    keyStr := aws.String(key)
    res, err := svc.HeadObject(&s3.HeadObjectInput{
        Bucket  : bucketStr,
        Key     : keyStr,
    })
    var result []byte
    if err != nil {
        log.Fatalln(err)
    }
    reader, _ := os.Open(path)
    defer reader.Close()

    hash := md5.New()
    io.Copy(hash, reader)
    md5sum := fmt.Sprintf("%x", hash.Sum(result))
    etag := strings.Replace(*res.ETag, "\"", "", 2)

    stat, _ := reader.Stat()
    size := stat.Size()
    length := *res.ContentLength

    log.Printf("ETag: %s, Size: %d\n", etag, length)
    if (size != length || md5sum != etag) {
        log.Fatalf("Metadata [%s/%s] NOT Match, Size: %d, MD5: %s\n", bucketName, key, size, md5sum)
    }

    log.Println("===== Head Object End =====")
    log.Println()
}

func getObject(bucketName, key, path string) {
    log.Printf("===== Get Object [%s/%s] Start =====\n", bucketName, key)
    bucketStr := aws.String(bucketName)
    keyStr := aws.String(key)
    res, err := svc.GetObject(&s3.GetObjectInput{
        Bucket  : bucketStr,
        Key     : keyStr,
    })
    if err != nil {
        log.Fatalln(err)
    }
    reader, _ := os.Open(path)
    if !doesFileMatch(reader, res.Body) {
        log.Fatalln("Content NOT Match!")
    }
    log.Println("===== Get Object End =====")
    log.Println()
}

func getNotExist(bucketName, key string) {
    log.Printf("===== Get Not Exist Object [%s/%s] Start =====\n", bucketName, key)
    bucketStr := aws.String(bucketName)
    keyStr := aws.String(key)
    _, err := svc.GetObject(&s3.GetObjectInput{
        Bucket  : bucketStr,
        Key     : keyStr,
    })
    if err != nil {
        code := err.(awserr.RequestFailure).StatusCode()
        if code != 404 && code != 403{
            log.Fatalf("Incorrect Status Code [%d]!\n",code)
        }
    } else {
        log.Fatalln("Should NOT Exist!")
    }
    log.Println("===== Get Not Exist Object End =====")
    log.Println()
}

func rangeObject(bucketName, key, path string, start, end int) {
    log.Printf("===== Range Get Object [%s/%s] (%d-%d) Start =====\n", bucketName, key, start, end)
    bucketStr := aws.String(bucketName)
    keyStr := aws.String(key)
    rangeStr := aws.String(fmt.Sprintf("byte=%d-%d", start, end))
    res, err := svc.GetObject(&s3.GetObjectInput{
        Bucket  : bucketStr,
        Key     : keyStr,
        Range   : rangeStr,
    })
    if err != nil {
        log.Fatalln(err)
    }
    reader, _ := os.Open(path)
    reader.Seek(int64(start), 0)
    tmp := make([]byte, end - start + 1)
    io.ReadFull(reader, tmp)
    buf, _ := ioutil.ReadAll(res.Body)
    if !bytes.Equal(tmp, buf) {
        log.Fatalln("Content NOT Match!")
    }

    log.Println("===== Range Get Object End =====")
    log.Println()
}

func copyObject(bucketName, src, dst string) {
    log.Printf("===== Copy Object [%s/%s] -> [%s/%s] Start =====\n",
        bucketName, src, bucketName, dst)
    sourceStr := aws.String(bucketName + "/" + src)
    bucketStr := aws.String(bucketName)
    dstStr := aws.String(dst)

    _, err := svc.CopyObject(&s3.CopyObjectInput{
        CopySource  : sourceStr,
        Bucket  : bucketStr,
        Key     : dstStr,
    })
    if err != nil {
        log.Fatalln(err)
    }

    log.Println("===== Copy Object End =====")
    log.Println()
}

func listObject(bucketName, prefix string, expected int) {
    log.Printf("===== List Objects [%s/%s*] Start =====\n", bucketName, prefix)
    bucketStr := aws.String(bucketName)
    prefixStr := aws.String(prefix)
    res, err := svc.ListObjects(&s3.ListObjectsInput{
        Bucket  : bucketStr,
        Prefix  : prefixStr,
    })
    if err != nil {
        log.Fatalln(err)
    }
    count := 0
    for _, obj := range res.Contents {
        if doesFileExist(bucketName, *obj.Key) {
            log.Printf("%s \t Size: %d\n", *obj.Key, *obj.Size)
            count ++
        }
    }
    if expected >= 0 {
        if count != expected {
            log.Fatalln("Number of Objects NOT Match!")
        }
    }

    log.Println("===== List Objects End =====")
    log.Println()
}

func deleteAllObjects(bucketName string) {
    log.Printf("===== Delete All Objects [%s] Start =====\n", bucketName)
    bucketStr := aws.String(bucketName)
    res, err := svc.ListObjects(&s3.ListObjectsInput{
        Bucket  : bucketStr,
    })
    if err != nil {
        log.Fatalln(err)
    }
    for _, obj := range res.Contents {
        _, errd := svc.DeleteObject(&s3.DeleteObjectInput{
            Bucket  : bucketStr,
            Key     : obj.Key,
        })
        if errd != nil {
            log.Fatalln(errd)
        }
    }

    log.Println("===== Delete All Objects End =====")
    log.Println()
}

func putDummyObjects(bucketName, prefix string, total int, holder string) {
    bucketStr := aws.String(bucketName)
    for i := 0; i < total; i++ {
        reader, _ := os.Open(holder)
        defer reader.Close()
        svc.PutObject(&s3.PutObjectInput{
            Bucket  : bucketStr,
            Key     : aws.String(fmt.Sprintf("%s%d", prefix, i)),
            Body    : reader,
        })
    }
}

func pageListBucket(bucketName, prefix string, total, pageSize int) {
    log.Printf("===== Multiple Page List Objects [%s/%s*] %d Objs @%d Start =====\n", bucketName, prefix, total, pageSize)
    bucketStr := aws.String(bucketName)
    prefixStr := aws.String(prefix)
    count := 0
    marker := ""
    for {
        res, err := svc.ListObjects(&s3.ListObjectsInput{
            Bucket  : bucketStr,
            Prefix  : prefixStr,
            MaxKeys : aws.Long(int64(pageSize)),
            Marker  : aws.String(marker),
        })
        if err != nil {
            log.Fatalln(err)
        }
        log.Println("===== Page =====")
        for _, obj := range res.Contents {
            count++
            log.Printf("%s \t Size: %d \t Count: %d\n", *obj.Key, *obj.Size, count)
        }
        if !*res.IsTruncated {
            break
        } else {
            marker = *res.NextMarker
        }
    }
    log.Println("===== End =====")
    if count != total {
        log.Fatalln("Number of Objects NOT Match!")
    }
    log.Println("===== Multiple Page Lsit Objects End =====")
    log.Println()
}

func multiDelete(bucketName, prefix string, total int) {
    log.Printf("===== Multiple Delete Objects [%s/%s] Start =====\n", bucketName, prefix)
    bucketStr := aws.String(bucketName)
    delKeyList := make([]*s3.ObjectIdentifier, total)
    for i := 0; i < total; i++ {
        delKeyList[i] = &s3.ObjectIdentifier{
            Key : aws.String(fmt.Sprintf("%s%d", prefix, i)),
        }
    }
    res, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
        Bucket : bucketStr,
        Delete : &s3.Delete{
            Objects : delKeyList,
        },
    })
    if err != nil {
        log.Fatalln(err)
    }
    for _, obj := range res.Deleted {
        log.Printf("Deleted %s/%s", bucketName, *obj.Key)
    }
    if len(res.Deleted) != total {
        log.Fatalln("Number of Objects NOT Match!")
    }
    log.Println("===== Multiple Delete Objects End =====")
    log.Println()
}

func setBucketAcl(bucketName, permission string) {
    log.Printf("===== Set Bucket ACL [%s] (%s) Start =====\n", bucketName , permission)
    var checkList []string
    if permission == "private" {
        checkList = []string{"FULL_CONTROL"}
    } else if permission == "public-read" {
        checkList = []string{"READ", "READ_ACP"}
    } else if permission == "public-read-write" {
        checkList = []string{"READ", "READ_ACP", "WRITE", "WRITE_ACP"}
    } else {
        log.Fatalln("Invalid Permission!")
    }
    bucketStr := aws.String(bucketName)
    _, err := svc.PutBucketACL(&s3.PutBucketACLInput{
        Bucket  : bucketStr,
        ACL     : aws.String(permission),
    })
    if err != nil {
        log.Fatalln(err)
    }
    res, err := svc.GetBucketACL(&s3.GetBucketACLInput{
        Bucket  : bucketStr,
    })
    if err != nil {
        log.Fatalln(err)
    }
    log.Printf("Owner ID: S3Owner [name=%s,id=%s]\n", *res.Owner.DisplayName, *res.Owner.ID)
    list := make([]string, len(res.Grants))
    for i, grant := range res.Grants {
        log.Printf("Grantee: %s \t Permissions: %s", *grant.Grantee.URI, *grant.Permission)
        list[i] = *grant.Permission
    }
    for _, item := range checkList {
        found := false
        for _, ele := range list {
            if ele == item {
                found = true
                break
            }
        }
        if !found {
            log.Fatalln("ACL NOT Match!")
        }
    }
}

//Ref: http://play.golang.org/p/S2s3e0nj4s
func doesFileMatch(r1, r2 io.Reader) bool {
    bufio1 := bufio.NewReader(r1)
    bufio2 := bufio.NewReader(r2)
    var buf1 []byte
    var buf2 []byte
    buf1 = make([]byte, 4096)
    buf2 = make([]byte, 4096)
    for {
        n1, err1 := io.ReadFull(bufio1, buf1)
        n2, err2 := io.ReadFull(bufio2, buf2)
        if n1 != n2 {
            return false
        }
        if err1 == io.EOF || err1 == io.ErrUnexpectedEOF ||
           err2 == io.EOF || err2 == io.ErrUnexpectedEOF {
            return err1 == err2
        } else if err1 != nil || err2 != nil {
            return false
        }
        if !bytes.Equal(buf1, buf2) {
            return false
        }
    }
}

func doesFileExist(bucketName, key string) bool{
    bucketStr := aws.String(bucketName)
    keyStr := aws.String(key)
    _, err := svc.HeadObject(&s3.HeadObjectInput{
        Bucket  : bucketStr,
        Key     : keyStr,
    })
    if err != nil {
        code := err.(awserr.RequestFailure).StatusCode()
        if code == 404 || code == 403{
            return false
        }
        log.Fatalln(err)
    }
    return true
}
