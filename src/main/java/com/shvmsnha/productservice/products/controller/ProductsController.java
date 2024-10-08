package com.shvmsnha.productservice.products.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.xray.spring.aop.XRayEnabled;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.shvmsnha.productservice.events.dto.EventType;
import com.shvmsnha.productservice.events.services.EventsPublisher;
import com.shvmsnha.productservice.products.dto.ProductDto;
import com.shvmsnha.productservice.products.enums.ProductErrors;
import com.shvmsnha.productservice.products.exceptions.ProductException;
import com.shvmsnha.productservice.products.models.Product;
import com.shvmsnha.productservice.products.repositories.ProductsRepository;

import software.amazon.awssdk.services.sns.model.PublishResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;


@RestController
@RequestMapping("/api/products")
@XRayEnabled
public class ProductsController {

    private static final Logger LOG = LogManager.getLogger(ProductsController.class);
    private final ProductsRepository productsRepository;
    private final EventsPublisher eventsPublisher;

    @Autowired
    public ProductsController(ProductsRepository productsRepository, 
        EventsPublisher eventsPublisher) {
        this.productsRepository = productsRepository;
        this.eventsPublisher = eventsPublisher;
    }

    @GetMapping
    public ResponseEntity<?> getAllProducts(@RequestParam(required = false) String code) throws ProductException {
        if (code != null) {
            LOG.info("Get Product by code: [{}]", code);
            Product productByCode = productsRepository.getByCode(code).join();
            if (productByCode != null) {
                return new ResponseEntity<>(new ProductDto(productByCode), HttpStatus.OK);
            }
            throw new ProductException(ProductErrors.PRODUCT_NOT_FOUND, null);
        }
        LOG.info("Get all products");
        List<ProductDto> productDtos = new ArrayList<>();
        productsRepository.getAll().items().subscribe(product -> {
            productDtos.add(new ProductDto(product));
        }).join();
        return new ResponseEntity<>(productDtos, HttpStatus.OK);
    }

    @GetMapping("{id}")
    public ResponseEntity<ProductDto> getProductById(@PathVariable("id") String id) throws ProductException {
        LOG.info("Get product: [{}]", id);
        Product product = productsRepository.getById(id).join();
        if (product != null) {
            LOG.info("Product Fetched - ID: [{}]", id);
            return new ResponseEntity<>(new ProductDto(product), HttpStatus.OK);
        }
        throw new ProductException(ProductErrors.PRODUCT_NOT_FOUND, id);
    }

    @PostMapping
    public ResponseEntity<ProductDto> createProduct(@RequestBody ProductDto productDto) throws ProductException, 
            JsonProcessingException, InterruptedException, ExecutionException {
        LOG.info("Creating product: [{}]", productDto.name());
        Product productCreated = ProductDto.toProduct(productDto);
        productCreated.setId(UUID.randomUUID().toString());
        CompletableFuture<Void> productCreateFuture = productsRepository.create(productCreated);


        CompletableFuture<PublishResponse> responseFuture = eventsPublisher.sendProductEvent(productCreated, 
                EventType.PRODUCT_CREATED, "shivamtest@test.com");
        
        CompletableFuture.allOf(productCreateFuture, responseFuture).join();

        PublishResponse response = responseFuture.get();

        ThreadContext.put("messageId", response.messageId());
        LOG.info("Publish create message message Id: [{}]", response.messageId());
        LOG.info("Product created - ID: [{}]", productCreated.getId());
        return new ResponseEntity<>(new ProductDto(productCreated), HttpStatus.CREATED);
    }

    @DeleteMapping("{id}")
    public ResponseEntity<ProductDto> deleteProduct(@PathVariable("id") String id) throws ProductException, JsonProcessingException {
        LOG.info("Delete product: [{}]", id);
        Product productDeleted = productsRepository.deleteById(id).join();
        if (productDeleted != null) {
            PublishResponse response = eventsPublisher.sendProductEvent(productDeleted, 
                EventType.PRODUCT_DELETED, "shivamtest@test.com").join();
            ThreadContext.put("messageId", response.messageId());
            LOG.info("Publish delete message message Id: [{}]", response.messageId());
            LOG.info("Product created - ID: [{}]", productDeleted.getId());
            return new ResponseEntity<>(new ProductDto(productDeleted), HttpStatus.OK);
        }
        throw new ProductException(ProductErrors.PRODUCT_NOT_FOUND, id);
    }

    @PutMapping("{id}")
    public ResponseEntity<ProductDto> updateProduct(@PathVariable("id") String id, @RequestBody ProductDto productDto) throws ProductException, JsonProcessingException {
        try {
            LOG.info("Updating product: [{}]", id);
            Product productUpdated = productsRepository.update(ProductDto.toProduct(productDto), id).join();
            PublishResponse response = eventsPublisher.sendProductEvent(productUpdated, 
                EventType.PRODUCT_UPDATED, "shivamtest@test.com").join();
            ThreadContext.put("messageId", response.messageId());
            LOG.info("Publish update message message Id: [{}]", response.messageId());
            LOG.info("Product updated - ID: [{}]", productUpdated.getId());
            return new ResponseEntity<>(new ProductDto(productUpdated), HttpStatus.OK);
        } catch(CompletionException e) {
            throw new ProductException(ProductErrors.PRODUCT_NOT_FOUND, id);
        }
    }
    
}
