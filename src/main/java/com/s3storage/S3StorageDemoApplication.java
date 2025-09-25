package com.s3storage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(basePackages = {"com.s3storage", "com.s3storage.controller", "com.s3storage.config"})
public class S3StorageDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(S3StorageDemoApplication.class, args);
	}

}
