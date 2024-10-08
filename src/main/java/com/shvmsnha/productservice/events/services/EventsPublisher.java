package com.shvmsnha.productservice.events.services;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.amazonaws.xray.AWSXRay;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shvmsnha.productservice.events.dto.EventType;
import com.shvmsnha.productservice.events.dto.ProductEventDto;
import com.shvmsnha.productservice.events.dto.ProductFailureEventDto;
import com.shvmsnha.productservice.products.models.Product;

import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.Topic;

@Service
public class EventsPublisher {

    private final SnsAsyncClient snsAsyncClient;
    private final Topic productsEvenetsTopic;
    private final ObjectMapper objectMapper;

    @Autowired
    public EventsPublisher(SnsAsyncClient snsAsyncClient, 
        @Qualifier("productEventsTopic") Topic productsEvenetsTopic, 
        ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            this.productsEvenetsTopic = productsEvenetsTopic;
            this.snsAsyncClient = snsAsyncClient;
    }

    public CompletableFuture<PublishResponse> sendProductFailureEvent(ProductFailureEventDto productFailureEventDto) throws JsonProcessingException {
        return this.sendEvents(objectMapper.writeValueAsString(productFailureEventDto), EventType.PRODUCT_FAILED);
    }


    public CompletableFuture<PublishResponse> sendProductEvent(Product product, EventType eventType, String email) throws JsonProcessingException {
        ProductEventDto productEventDto = new ProductEventDto(
            product.getId(),
            product.getCode(),
            email,
            product.getPrice()
        );
        return this.sendEvents(objectMapper.writeValueAsString(productEventDto), eventType);
    }

    private CompletableFuture<PublishResponse> sendEvents(String data, EventType eventType) {
        return this.snsAsyncClient.publish(PublishRequest.builder()
            .message(data)
            .messageAttributes(Map.of(
                "eventType", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(eventType.name())
                        .build(),
                "requestId",  MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(ThreadContext.get("requestId"))
                        .build(),
                    "traceId", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(Objects.requireNonNull( 
                            AWSXRay.getCurrentSegment()).getTraceId().toString())
                        .build()
            ))
            .topicArn(this.productsEvenetsTopic.topicArn())
            .build());
    }

}
