package com.shvmsnha.productservice.products.exceptions;

import org.springframework.lang.Nullable;

import com.shvmsnha.productservice.products.enums.ProductErrors;

public class ProductException extends Exception {

    private final ProductErrors productErrors;
    @Nullable
    private final String productId;

    public ProductException(ProductErrors productErrors, @Nullable String productId) {
        this.productErrors = productErrors;
        this.productId = productId;
    }

    public ProductErrors getProductErrors() {
        return this.productErrors;
    }

    @Nullable
    public String getProductId() {
        return this.productId;
    }
}
