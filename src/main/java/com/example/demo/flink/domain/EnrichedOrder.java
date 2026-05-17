package com.example.demo.flink.domain;

import java.time.Instant;

public record EnrichedOrder(Order order, Instant processedAt) {}
