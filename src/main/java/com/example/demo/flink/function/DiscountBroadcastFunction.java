package com.example.demo.flink.function;

import com.example.demo.flink.domain.DiscountRule;
import com.example.demo.flink.domain.DiscountedOrder;
import com.example.demo.flink.domain.Order;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class DiscountBroadcastFunction
        extends BroadcastProcessFunction<Order, DiscountRule, DiscountedOrder> {

    public static final MapStateDescriptor<String, BigDecimal> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("discount-rules", String.class, BigDecimal.class);

    private static final BigDecimal HUNDRED = new BigDecimal("100");

    @Override
    public void processBroadcastElement(
            DiscountRule rule,
            Context ctx,
            Collector<DiscountedOrder> out) throws Exception {
        ctx.getBroadcastState(RULE_STATE_DESCRIPTOR)
                .put(rule.productId(), rule.discountPercent());
    }

    @Override
    public void processElement(
            Order order,
            ReadOnlyContext ctx,
            Collector<DiscountedOrder> out) throws Exception {
        BigDecimal discount = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR).get(order.productId());
        BigDecimal effective = discount == null ? BigDecimal.ZERO : discount;
        out.collect(applyDiscount(order, effective));
    }

    public static DiscountedOrder applyDiscount(Order order, BigDecimal discountPercent) {
        BigDecimal multiplier = BigDecimal.ONE.subtract(
                discountPercent.divide(HUNDRED, 4, RoundingMode.HALF_UP));
        BigDecimal finalAmount = order.amount().multiply(multiplier)
                .setScale(2, RoundingMode.HALF_UP);
        return new DiscountedOrder(order, discountPercent, finalAmount);
    }
}
