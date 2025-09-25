package com.s3storage.controller;

import com.s3storage.service.S3Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Iterator;

@RestController
@RequestMapping("/api/s3")
@Slf4j
public class S3Controller {

    private final S3Service s3Service;

    public S3Controller(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @PostMapping("/upload-text")
    public ResponseEntity<String> uploadTextFile(@RequestBody String content) {
        //log.info("Received request to upload text content: {}", content);
        try {
            String result = s3Service.uploadTextFile(content);
            return ResponseEntity.ok(result);
        } catch (RuntimeException e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    @GetMapping("/get-keys")
    public ResponseEntity<String> getFiles(@RequestBody String content) {
        // log.info("Received request to upload text content: {}", content);
        StringBuilder sb = new StringBuilder();
        try {
            Iterator<String> result = s3Service.getObjectKeysIterator();
            while(result.hasNext()){
                sb.append(result.next() + "::");
            }
            return ResponseEntity.ok(sb.toString());
        } catch (RuntimeException e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    @GetMapping("/read-file/{filename}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<String> readFile(@PathVariable("filename") String fileName) {
        // log.info("Received request to upload text content: {}", content);
        try {
            String result = s3Service.readTextFile(fileName);
            return ResponseEntity.ok(result);
        } catch (RuntimeException e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    @DeleteMapping("/delete-file")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public ResponseEntity<String> deleteFile(@RequestBody String fileName) {
        // log.info("Received request to upload text content: {}", content);
        try {
            String result = s3Service.deleteFile(fileName);
            return ResponseEntity.ok(result);
        } catch (RuntimeException e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }


}
