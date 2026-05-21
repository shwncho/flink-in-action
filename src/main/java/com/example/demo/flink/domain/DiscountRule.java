package com.example.demo.flink.domain;

import java.math.BigDecimal;

public record DiscountRule(String productId, BigDecimal discountPercent) {}
