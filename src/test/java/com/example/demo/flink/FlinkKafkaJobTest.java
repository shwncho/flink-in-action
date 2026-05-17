package com.example.demo.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.demo.flink.domain.EnrichedOrder;
import com.example.demo.flink.domain.Order;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class FlinkKafkaJobTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void enrichesEachOrderWithProcessedAt() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Order o1 = new Order("o1", "u1", "p1", new BigDecimal("10.00"), Instant.parse("2026-01-01T00:00:00Z"));
        Order o2 = new Order("o2", "u2", "p2", new BigDecimal("20.00"), Instant.parse("2026-01-01T00:01:00Z"));

        Instant before = Instant.now();
        DataStream<Order> input = env.fromData(TypeInformation.of(Order.class), o1, o2);
        DataStream<EnrichedOrder> result = FlinkKafkaJob.applyProcessing(input);

        List<EnrichedOrder> collected = new ArrayList<>();
        try (CloseableIterator<EnrichedOrder> it = result.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }
        Instant after = Instant.now();

        assertThat(collected).hasSize(2);
        assertThat(collected).extracting(EnrichedOrder::order).containsExactly(o1, o2);
        assertThat(collected).allSatisfy(e ->
                assertThat(e.processedAt()).isBetween(before, after));
    }
}
