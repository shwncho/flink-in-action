package com.example.demo.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.demo.flink.domain.Order;
import com.example.demo.flink.domain.UserOrderStats;
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

class UserOrderStatsFunctionTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void accumulatesCountAndTotalPerUser() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Order u1a = new Order("o1", "u1", "p1", new BigDecimal("10.00"),
                Instant.parse("2026-01-01T00:00:00Z"));
        Order u1b = new Order("o2", "u1", "p2", new BigDecimal("20.00"),
                Instant.parse("2026-01-01T00:01:00Z"));
        Order u2 = new Order("o3", "u2", "p1", new BigDecimal("5.00"),
                Instant.parse("2026-01-01T00:02:00Z"));

        DataStream<Order> input = env.fromData(TypeInformation.of(Order.class), u1a, u1b, u2);
        DataStream<UserOrderStats> result = FlinkKafkaJob.applyStatefulProcessing(input);

        List<UserOrderStats> collected = new ArrayList<>();
        try (CloseableIterator<UserOrderStats> it = result.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }

        assertThat(collected).hasSize(3);

        List<UserOrderStats> u1Stats = collected.stream()
                .filter(s -> s.userId().equals("u1"))
                .toList();
        assertThat(u1Stats).hasSize(2);
        assertThat(u1Stats.get(0).orderCount()).isEqualTo(1L);
        assertThat(u1Stats.get(0).totalAmount()).isEqualByComparingTo("10.00");
        assertThat(u1Stats.get(0).lastOrderAt()).isEqualTo(u1a.ts());
        assertThat(u1Stats.get(1).orderCount()).isEqualTo(2L);
        assertThat(u1Stats.get(1).totalAmount()).isEqualByComparingTo("30.00");
        assertThat(u1Stats.get(1).lastOrderAt()).isEqualTo(u1b.ts());

        List<UserOrderStats> u2Stats = collected.stream()
                .filter(s -> s.userId().equals("u2"))
                .toList();
        assertThat(u2Stats).hasSize(1);
        assertThat(u2Stats.get(0).orderCount()).isEqualTo(1L);
        assertThat(u2Stats.get(0).totalAmount()).isEqualByComparingTo("5.00");
        assertThat(u2Stats.get(0).lastOrderAt()).isEqualTo(u2.ts());
    }
}
