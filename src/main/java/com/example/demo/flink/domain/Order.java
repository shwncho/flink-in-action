package com.example.demo.flink.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record Order(
        String orderId,
        String userId,
        String productId,
        BigDecimal amount,
        Instant ts
) {}
