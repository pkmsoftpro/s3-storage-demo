package com.s3storage.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;

@Service
@Slf4j
public class S3Service {

    private final S3Client s3Client;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    public S3Service(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public String uploadTextFile(String content) {
        String objectKey = "textfile_" + UUID.randomUUID().toString() + ".txt";

        try {
            Path tempFile = Files.createTempFile("s3-upload-", ".tmp");
            Files.writeString(tempFile, content);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, RequestBody.fromFile(tempFile));

            log.info("File uploaded successfully to S3 with ETag: {}", putObjectResponse.eTag());

            Files.delete(tempFile);

            return "File uploaded: " + objectKey;

        } catch (IOException e) {
            //log.error("Error creating temporary file for upload", e);
            throw new RuntimeException("Error uploading file to S3", e);
        } catch (Exception e) {
            //log.error("Error during S3 upload", e);
            throw new RuntimeException("Error uploading file to S3", e);
        }
    }
}
