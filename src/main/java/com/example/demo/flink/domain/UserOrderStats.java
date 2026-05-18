package com.example.demo.flink.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record UserOrderStats(
        String userId,
        long orderCount,
        BigDecimal totalAmount,
        Instant lastOrderAt
) {}
