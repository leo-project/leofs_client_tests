package main

import (
        "log"
        "io/ioutil"
        "bytes"
        "crypto/rand"
        "os"

        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/aws/awsutil"
        "github.com/aws/aws-sdk-go/service/s3"
        "github.com/aws/aws-sdk-go/aws/credentials"
       )

const (
        BucketName = "testgo"
        FileName   = "testFile"
        LargeFileName   = "testFile.large"
        DataPath   = "../temp_data/"
        ChunkSize  = 5 << 20
        LargeObjSize = 50 << 20
      )


func main() {
    cred := credentials.NewStaticCredentials("05236", "802562235", "")

    svc := s3.New(&aws.Config{Endpoint: "http://localhost:8080",
                              Region: "us-west-2",
                              Credentials: cred,
                              LogLevel: 0})

    bucketStr := aws.String(BucketName)

    _, err := svc.CreateBucket(&s3.CreateBucketInput{Bucket: bucketStr})

    if err != nil {
        log.Printf("Create Bucket %s Failed, %s\n", BucketName, err)
        return
    }
    log.Printf("Create Bucket %s Suceeded\n", BucketName)
    log.Println()

    listRes, listErr := svc.ListBuckets(&s3.ListBucketsInput{})
    if listErr != nil {
        log.Printf("List Bucket Failed, %s\n", listErr)
        return
    }

    log.Println("Buckets:")
    for _, bucket := range listRes.Buckets {
        log.Printf("%s : %s\n", *bucket.Name, bucket.CreationDate)
    }
    log.Println()

    filePath := DataPath + FileName
    basicContent, _ := ioutil.ReadFile(filePath)
    basicReader  := bytes.NewReader(basicContent)

    _, putErr := svc.PutObject(&s3.PutObjectInput{
        Bucket : bucketStr,
        Key    : aws.String(FileName),
        Body   : basicReader,
    })

    if putErr != nil {
        log.Printf("Put Object %s/%s Failed, %s\n", BucketName, FileName, putErr)
        return
    }
    log.Printf("Put Object %s/%s Suceeded\n", BucketName, FileName)
    log.Println()

    largeFilePath := DataPath + LargeFileName
    largeContent := make([]byte, LargeObjSize)
    if _, largeFileErr := os.Stat(largeFilePath); largeFileErr == nil {
        largeContent, _ = ioutil.ReadFile(largeFilePath)
    } else {
        rand.Read(largeContent)
        ioutil.WriteFile(largeFilePath, largeContent, 0644)
    }

    largeReader := bytes.NewReader(largeContent)
    singleFileName := LargeFileName + ".single"
    _, largePutErr := svc.PutObject(&s3.PutObjectInput{
        Bucket : bucketStr,
        Key    : aws.String(singleFileName),
        Body   : largeReader,
    })

    if largePutErr != nil {
        log.Printf("Put Large Object %s/%s Failed, %s\n", BucketName, singleFileName, putErr)
        return
    }
    log.Printf("Put Large Object %s/%s Suceeded\n", BucketName, singleFileName)
    log.Println()

    mpFileName := LargeFileName + ".part"
    log.Printf("Put Multi Part Large Object\n")
    mpCreateRes, mpCreateErr := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
        Bucket  : bucketStr,
        Key     : aws.String(mpFileName),
    })
    if mpCreateErr != nil {
        log.Printf("Create Multi Part %s/%s Failed, %s\n", BucketName, mpFileName, mpCreateErr)
        return
    }
    uploadId := mpCreateRes.UploadID
    log.Printf("Start Multi Part Upload %s/%s [%s]\n", BucketName, mpFileName, awsutil.StringValue(uploadId))

    var completed[]*s3.CompletedPart
    total := int64(LargeObjSize / ChunkSize)

    for i := int64(1); i <= total; i++ {
        chunk := largeContent[(i - 1) * ChunkSize : i * ChunkSize]
        chunkReader := bytes.NewReader(chunk)
        partRes ,partErr := svc.UploadPart(&s3.UploadPartInput{
            Bucket      : bucketStr,
            Key         : aws.String(mpFileName),
            Body        : chunkReader,
            UploadID    : uploadId,
            PartNumber  : aws.Long(i),
        })

        if partErr != nil {
            log.Printf("Part %d Failed, %s\n", i, partErr)
            return
        }

        completed = append(completed, &s3.CompletedPart{
            ETag        : partRes.ETag,
            PartNumber  : aws.Long(i),
        })
    }

    _ , mpCompleteErr := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
        Bucket      : bucketStr,
        Key         : aws.String(mpFileName),
        UploadID    : uploadId,
        MultipartUpload : &s3.CompletedMultipartUpload{Parts : completed},
    })
    if mpCompleteErr != nil {
        log.Printf("Complete Multi Part Failed %s/%s [%s], %s\n", BucketName, mpFileName, awsutil.StringValue(uploadId), mpCompleteErr)
        return
    }
    log.Printf("Multi Part Upload %s/%s Suceeded\n", BucketName, mpFileName)
    log.Println()

    _, copyErr := svc.CopyObject(&s3.CopyObjectInput{
        CopySource : aws.String(BucketName + "/" + FileName),
        Bucket  : bucketStr,
        Key     : aws.String(FileName + ".copy"),
    })
    if copyErr != nil {
        log.Printf("Copy Object %s/%s -> %s/%s Failed, %s\n", BucketName, FileName, BucketName, FileName+".copy", copyErr)
        return
    }
    log.Printf("Copy Object %s/%s -> %s/%s Suceeded\n", BucketName, FileName, BucketName, FileName+".copy")
    log.Println()

    headObjRes, headObjErr := svc.HeadObject(&s3.HeadObjectInput{
        Bucket  : bucketStr,
        Key     : aws.String(FileName),
        })
    if headObjErr != nil {
        log.Printf("Head Object %s/%s Failed, %s\n", BucketName, FileName, headObjErr)
        return
    }
    log.Printf("Head Object %s/%s:\n", BucketName, FileName)
    log.Printf("Size: %d\t, ETag: %s\n", *headObjRes.ContentLength, *headObjRes.ETag)
    log.Println()

    getObjRes, getObjErr := svc.GetObject(&s3.GetObjectInput{
        Bucket  : bucketStr,
        Key     : aws.String(FileName),
    })
    if getObjErr != nil {
        log.Printf("Get Object %s/%s Failed, %s\n", BucketName, FileName, getObjErr)
        return
    }
    content, _ := ioutil.ReadAll(getObjRes.Body)
    if !bytes.Equal(content, basicContent) {
        log.Printf("Get Object %s/%s Failed, Content NOT Match\n", BucketName, FileName)
        return
    }
    log.Printf("Get Object %s/%s Suceeded\n", BucketName, FileName)
    log.Println()

    largeContentPart := largeContent[1048576:10485760+1]

    rngSingleRes, rngSingleErr := svc.GetObject(&s3.GetObjectInput{
        Bucket  : bucketStr,
        Key     : aws.String(singleFileName),
        Range   : aws.String("bytes=1048576-10485760"),
    })
    if rngSingleErr != nil {
        log.Printf("Range Get Single Part Object %s/%s Failed, %s\n", BucketName, singleFileName, rngSingleErr)
        return
    }
    rngSingleCon, _ := ioutil.ReadAll(rngSingleRes.Body)
    if !bytes.Equal(rngSingleCon, largeContentPart){
        log.Printf("Range Get Single Part Object %s/%s Failed, Content NOT Match\n", BucketName, singleFileName)
        return
    }
    log.Printf("Range Get Single Part Object %s/%s Suceeded\n", BucketName, singleFileName)
    log.Println()

    rngMpRes, rngMpErr := svc.GetObject(&s3.GetObjectInput{
        Bucket  : bucketStr,
        Key     : aws.String(mpFileName),
        Range   : aws.String("bytes=1048576-10485760"),
    })
    if rngMpErr != nil {
        log.Printf("Range Get Multi Part Object %s/%s Failed, %s\n", BucketName, mpFileName, rngMpErr)
        return
    }
    rngMpCon, _ := ioutil.ReadAll(rngMpRes.Body)
    if !bytes.Equal(rngMpCon, largeContentPart){
        log.Printf("Range Get Multi Part Object %s/%s Failed, Content NOT Match\n", BucketName, mpFileName)
        return
    }
    log.Printf("Range Get Multi Part Object %s/%s Suceeded\n", BucketName, mpFileName)
    log.Println()

    listObjRes, listObjErr := svc.ListObjects(&s3.ListObjectsInput{Bucket: bucketStr})
    if listObjErr != nil {
        log.Printf("List Object %s/ Failed, %s\n", BucketName, listObjErr)
        return
    }
    log.Printf("List Object Res:\n")
    for _, obj := range listObjRes.Contents {
        log.Printf("Key: %s\t, Size: %d\t, ETag: %s\n", *obj.Key, *obj.Size, *obj.ETag)
    }

    _, delObjErr := svc.DeleteObject(&s3.DeleteObjectInput{
        Bucket  : bucketStr,
        Key     : aws.String(FileName),
    })
    if delObjErr != nil {
        log.Printf("Delete Object %s/%s Failed, %s\n", BucketName, FileName, delObjErr)
        return
    }
    log.Printf("Delete Object %s/%s Suceeded\n", BucketName, FileName)
    log.Println()

    _, err = svc.DeleteBucket(&s3.DeleteBucketInput{Bucket: bucketStr})
    if err != nil {
        log.Printf("Delete Bucket %s Failed, %s\n", BucketName, err)
        return
    }
    log.Printf("Delete Bucket %s Suceeded\n", BucketName)
}
