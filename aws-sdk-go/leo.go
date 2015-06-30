package main

import (
        "log"
        "time"

        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/service/s3"
        "github.com/aws/aws-sdk-go/aws/credentials"
       )

const (
        BucketName = "testgo"
        FileName   = "testFile"
      )


func main() {
    cred := credentials.NewStaticCredentials("05236", "802562235", "")

    svc := s3.New(&aws.Config{Endpoint: "http://localhost:8080", 
                              Region: "us-west-2",
                              Credentials: cred,
                              LogLevel: 1})

    _, err := svc.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(BucketName)})
                                                        
    if err != nil {
        log.Printf("Create Bucket %s Failed, %s\n", BucketName, err)
        return
    }
    log.Printf("Create Bucket %s Suceeded\n", BucketName)

//    filePath := "../temp_data/" + FileName   
    time.Sleep(3 * time.Second)
    
    _, err = svc.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(BucketName)})
    if err != nil {
        log.Printf("Delete Bucket %s Failed, %s\n", BucketName, err)
        return
    }
    log.Printf("Delete Bucket %s Suceeded\n", BucketName)
}
