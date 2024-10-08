package com.shvmsnha.productservice.products.repositories;

import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import com.amazonaws.xray.spring.aop.XRayEnabled;
import com.shvmsnha.productservice.products.enums.ProductErrors;
import com.shvmsnha.productservice.products.exceptions.ProductException;
import com.shvmsnha.productservice.products.models.Product;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;

import java.util.List;
import java.util.ArrayList;

@Repository
@XRayEnabled
public class ProductsRepository {

    private static final Logger LOG = LogManager.getLogger(ProductsRepository.class);
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<Product> productsTable;

    @Autowired
    public ProductsRepository(DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient, 
        @Value("${aws.productsddb.name}") String productsDdbName
    ) {
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
        this.productsTable = this.dynamoDbEnhancedAsyncClient.table(productsDdbName, TableSchema.fromBean(Product.class));
    }

    private CompletableFuture<Product> checkIfCodeExists(String code) {
        List<Product> products = new ArrayList<>();
        productsTable.index("codeIdx").query(QueryEnhancedRequest.builder()
            .limit(1)
            .queryConditional(QueryConditional.keyEqualTo(Key.builder()
                .partitionValue(code)
                .build()))
            .build()
        ).subscribe(productPage -> {
            products.addAll(productPage.items());
        }).join();

        if (products.size() > 0) {
            return CompletableFuture.supplyAsync(() -> products.get(0));
        }
        return CompletableFuture.supplyAsync(() -> null);
    }

    public CompletableFuture<Product> getByCode(String code) {
        LOG.info("Get ProductId By Code: {}", code);
        Product productByCode = checkIfCodeExists(code).join();
        if (productByCode != null) {
            return getById(productByCode.getId());
        } 
        return CompletableFuture.supplyAsync(() -> null);
    }

    public PagePublisher<Product> getAll() {
        // DO NOT DO THIS IN PRODUCTION
        LOG.info("Get All Products");
        return productsTable.scan();
    }

    public CompletableFuture<Product> getById(String productId) {
        LOG.info("Get ProductId: {}", productId);
        return productsTable.getItem(Key.builder()
            .partitionValue(productId)
            .build());
    }

    public CompletableFuture<Void> create(Product product) throws ProductException {
        LOG.info("Create Product");
        Product productWithSameCode = checkIfCodeExists(product.getCode()).join();
        if (productWithSameCode != null) {
            LOG.error("Can not create a product with same code.");
            throw new ProductException(ProductErrors.PRODUCT_CODE_ALREADY_EXISTS, productWithSameCode.getId());
        }
        return productsTable.putItem(product);
    }

    public CompletableFuture<Product> deleteById(String productId) {
        LOG.info("Delete ProductId: {}", productId);
        return productsTable.deleteItem(Key.builder()
            .partitionValue(productId)
            .build());
    }

    public CompletableFuture<Product> update(Product product, String productId) throws ProductException {
        LOG.info("Update ProductId: {}", productId);
        product.setId(productId);
        Product productWithSameCode = checkIfCodeExists(product.getCode()).join();
        if (productWithSameCode != null && !productWithSameCode.getId().equals(productId)) {
            throw new ProductException(ProductErrors.PRODUCT_CODE_ALREADY_EXISTS, productWithSameCode.getId());
        }
        return productsTable.updateItem(
            UpdateItemEnhancedRequest.builder(Product.class)
                .item(product)
                .conditionExpression(Expression.builder()
                    .expression("attribute_exists(id)")
                    .build())
                .build()
        );
    }
}
