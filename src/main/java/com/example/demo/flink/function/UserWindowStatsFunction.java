package com.example.demo.flink.function;

import com.example.demo.flink.domain.UserWindowStats;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserWindowStatsFunction
        extends ProcessWindowFunction<OrderAggregator.Accumulator, UserWindowStats, String, TimeWindow> {

    @Override
    public void process(
            String userId,
            Context context,
            Iterable<OrderAggregator.Accumulator> aggregates,
            Collector<UserWindowStats> out) {
        OrderAggregator.Accumulator agg = aggregates.iterator().next();
        TimeWindow window = context.window();
        out.collect(new UserWindowStats(
                userId,
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd()),
                agg.count(),
                agg.total()
        ));
    }
}
