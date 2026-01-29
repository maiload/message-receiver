package com.maiload.messagereceiver.orchestrator.config;

import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MinioConfig {

    @Value("${orchestrator.minio.endpoint}")
    private String endpoint;

    @Value("${orchestrator.minio.access-key}")
    private String accessKey;

    @Value("${orchestrator.minio.secret-key}")
    private String secretKey;

    @Bean
    public MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
    }
}
