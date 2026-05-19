package com.example.demo.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.demo.flink.domain.Order;
import com.example.demo.flink.domain.UserWindowStats;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class UserWindowStatsFunctionTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void aggregatesPerUserPerOneMinuteWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Order u1a = new Order("o1", "u1", "p1", new BigDecimal("10.00"),
                Instant.parse("2026-01-01T00:00:10Z"));
        Order u1b = new Order("o2", "u1", "p2", new BigDecimal("20.00"),
                Instant.parse("2026-01-01T00:00:40Z"));
        Order u1c = new Order("o3", "u1", "p3", new BigDecimal("5.00"),
                Instant.parse("2026-01-01T00:01:15Z"));
        Order u2 = new Order("o4", "u2", "p1", new BigDecimal("7.00"),
                Instant.parse("2026-01-01T00:00:45Z"));

        DataStream<Order> input = env.fromData(TypeInformation.of(Order.class), u1a, u1b, u1c, u2);
        DataStream<UserWindowStats> result = FlinkKafkaJob.applyWindowedProcessing(input);

        List<UserWindowStats> collected = new ArrayList<>();
        try (CloseableIterator<UserWindowStats> it = result.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }

        assertThat(collected).hasSize(3);

        Instant w0Start = Instant.parse("2026-01-01T00:00:00Z");
        Instant w0End = Instant.parse("2026-01-01T00:01:00Z");
        Instant w1Start = w0End;
        Instant w1End = Instant.parse("2026-01-01T00:02:00Z");

        List<UserWindowStats> u1Windows = collected.stream()
                .filter(s -> s.userId().equals("u1"))
                .sorted(Comparator.comparing(UserWindowStats::windowStart))
                .toList();
        assertThat(u1Windows).hasSize(2);
        assertThat(u1Windows.get(0).windowStart()).isEqualTo(w0Start);
        assertThat(u1Windows.get(0).windowEnd()).isEqualTo(w0End);
        assertThat(u1Windows.get(0).orderCount()).isEqualTo(2L);
        assertThat(u1Windows.get(0).totalAmount()).isEqualByComparingTo("30.00");
        assertThat(u1Windows.get(1).windowStart()).isEqualTo(w1Start);
        assertThat(u1Windows.get(1).windowEnd()).isEqualTo(w1End);
        assertThat(u1Windows.get(1).orderCount()).isEqualTo(1L);
        assertThat(u1Windows.get(1).totalAmount()).isEqualByComparingTo("5.00");

        List<UserWindowStats> u2Windows = collected.stream()
                .filter(s -> s.userId().equals("u2"))
                .toList();
        assertThat(u2Windows).hasSize(1);
        assertThat(u2Windows.get(0).windowStart()).isEqualTo(w0Start);
        assertThat(u2Windows.get(0).windowEnd()).isEqualTo(w0End);
        assertThat(u2Windows.get(0).orderCount()).isEqualTo(1L);
        assertThat(u2Windows.get(0).totalAmount()).isEqualByComparingTo("7.00");
    }
}
