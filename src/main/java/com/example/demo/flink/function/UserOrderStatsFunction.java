package com.example.demo.flink.function;

import com.example.demo.flink.domain.Order;
import com.example.demo.flink.domain.UserOrderStats;
import java.math.BigDecimal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UserOrderStatsFunction
        extends KeyedProcessFunction<String, Order, UserOrderStats> {

    private transient ValueState<Long> countState;
    private transient ValueState<BigDecimal> totalState;

    @Override
    public void open(OpenContext openContext) {
        countState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("order-count", Long.class));
        totalState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("order-total", BigDecimal.class));
    }

    @Override
    public void processElement(Order order, Context ctx, Collector<UserOrderStats> out)
            throws Exception {
        Long currentCount = countState.value();
        BigDecimal currentTotal = totalState.value();

        long newCount = (currentCount == null ? 0L : currentCount) + 1L;
        BigDecimal newTotal = (currentTotal == null ? BigDecimal.ZERO : currentTotal)
                .add(order.amount());

        countState.update(newCount);
        totalState.update(newTotal);

        out.collect(new UserOrderStats(order.userId(), newCount, newTotal, order.ts()));
    }
}
