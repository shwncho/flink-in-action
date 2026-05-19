package com.example.demo.flink.function;

import com.example.demo.flink.domain.Order;
import java.io.Serializable;
import java.math.BigDecimal;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OrderAggregator
        implements AggregateFunction<Order, OrderAggregator.Accumulator, OrderAggregator.Accumulator> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(0L, BigDecimal.ZERO);
    }

    @Override
    public Accumulator add(Order order, Accumulator acc) {
        return new Accumulator(acc.count() + 1L, acc.total().add(order.amount()));
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        return new Accumulator(a.count() + b.count(), a.total().add(b.total()));
    }

    public record Accumulator(long count, BigDecimal total) implements Serializable {}
}
