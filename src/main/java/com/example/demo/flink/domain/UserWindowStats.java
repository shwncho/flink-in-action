package com.example.demo.flink.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record UserWindowStats(
        String userId,
        Instant windowStart,
        Instant windowEnd,
        long orderCount,
        BigDecimal totalAmount
) {}
