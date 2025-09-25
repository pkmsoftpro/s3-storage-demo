package com.s3storage.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

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

    /**
     * Fetches all object keys in the S3 bucket using an iterator.
     * This is an efficient way to handle large buckets by fetching results one page at a time.
     *
     * @return An Iterator of S3 object keys.
     */
    public Iterator<String> getObjectKeysIterator() {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Iterable listObjectsV2Iterable = s3Client.listObjectsV2Paginator(listObjectsRequest);

        // Map the paginated responses to a stream of S3Object and then to a stream of their keys
        return listObjectsV2Iterable.stream()
                .flatMap(response -> response.contents().stream())
                .map(S3Object::key)
                .iterator();
    }

    /**
     * Reads a text file from the S3 bucket and returns its content as a String.
     *
     * @param objectKey The key (path) of the object to read.
     * @return The content of the file as a String.
     */
    public String readTextFile(String objectKey) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        try {
            try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest) ;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object))) {
                String content = reader.lines().collect(Collectors.joining(System.lineSeparator()));
                log.info("File read successfully from S3: {}", objectKey);
                return content;
            }
        } catch (S3Exception e) {
            log.error("S3 error during file read: {}", e.awsErrorDetails().errorMessage());
            throw new RuntimeException("Error reading file from S3: " + e.awsErrorDetails().errorMessage(), e);
        } catch (IOException e) {
            log.error("IO error during file read: {}", e.getMessage());
            throw new RuntimeException("Error processing file content from S3", e);
        }
    }

    /**
     * Deletes a file from the S3 bucket.
     *
     * @param objectKey The key (path) of the object to delete.
     * @return A message indicating the result of the deletion.
     */
    public String deleteFile(String objectKey) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        try {
            s3Client.deleteObject(deleteObjectRequest);
            log.info("File deleted successfully from S3: {}", objectKey);
            return "File deleted: " + objectKey;

        } catch (S3Exception e) {
            log.error("S3 error during file deletion: {}", e.awsErrorDetails().errorMessage());
            throw new RuntimeException("Error deleting file from S3: " + e.awsErrorDetails().errorMessage(), e);
        }
    }
}
