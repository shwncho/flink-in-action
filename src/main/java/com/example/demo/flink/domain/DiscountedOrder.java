package com.example.demo.flink.domain;

import java.math.BigDecimal;

public record DiscountedOrder(
        Order order,
        BigDecimal appliedDiscountPercent,
        BigDecimal finalAmount
) {}
