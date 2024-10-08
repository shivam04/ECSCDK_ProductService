package com.shvmsnha.productservice.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.amazonaws.xray.interceptors.TracingInterceptor;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.Topic;

@Configuration
public class SnsConfig {

    private static final Logger log = LoggerFactory.getLogger(SnsConfig.class);

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.sns.topic.product.events}")
    private String productEventsTopic;
    
    @Bean
    public SnsAsyncClient snsAsyncClient() {
        log.info("awsRegion: [{}], productEventsTopic: [{}]", awsRegion,productEventsTopic);
        return SnsAsyncClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(Region.of(awsRegion))
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .addExecutionInterceptor(new TracingInterceptor())
                    .build())
            .build();
    }

    @Bean(name = "productEventsTopic")
    public Topic productEventsTopic() {
        return Topic.builder()
            .topicArn(productEventsTopic)
            .build();
    }
}
