package com.example.demo.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.demo.flink.domain.DiscountRule;
import com.example.demo.flink.domain.DiscountedOrder;
import com.example.demo.flink.domain.Order;
import com.example.demo.flink.function.DiscountBroadcastFunction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class DiscountBroadcastFunctionTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void applyDiscount_appliesPercentageToAmount() {
        Order order = new Order("o1", "u1", "p1", new BigDecimal("100.00"), Instant.now());

        DiscountedOrder result = DiscountBroadcastFunction.applyDiscount(order, new BigDecimal("10"));

        assertThat(result.appliedDiscountPercent()).isEqualByComparingTo("10");
        assertThat(result.finalAmount()).isEqualByComparingTo("90.00");
        assertThat(result.order()).isEqualTo(order);
    }

    @Test
    void applyDiscount_withZeroDiscount_keepsOriginalAmount() {
        Order order = new Order("o1", "u1", "p1", new BigDecimal("33.33"), Instant.now());

        DiscountedOrder result = DiscountBroadcastFunction.applyDiscount(order, BigDecimal.ZERO);

        assertThat(result.finalAmount()).isEqualByComparingTo("33.33");
    }

    @Test
    void appliesRulesFromBroadcastStreamInPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant ts = Instant.parse("2026-01-01T00:00:00Z");
        Map<String, BigDecimal> knownRules = Map.of(
                "p1", new BigDecimal("10"),
                "p2", new BigDecimal("20")
        );

        DataStream<DiscountRule> rules = env.fromData(
                TypeInformation.of(DiscountRule.class),
                new DiscountRule("p1", knownRules.get("p1")),
                new DiscountRule("p2", knownRules.get("p2"))
        );
        DataStream<Order> orders = env.fromData(
                TypeInformation.of(Order.class),
                new Order("o1", "u1", "p1", new BigDecimal("100.00"), ts),
                new Order("o2", "u2", "p2", new BigDecimal("50.00"), ts),
                new Order("o3", "u3", "p3", new BigDecimal("30.00"), ts)
        );

        DataStream<DiscountedOrder> result = FlinkKafkaJob.applyBroadcastProcessing(orders, rules);

        List<DiscountedOrder> collected = new ArrayList<>();
        try (CloseableIterator<DiscountedOrder> it = result.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }

        assertThat(collected).hasSize(3);
        for (DiscountedOrder d : collected) {
            BigDecimal applied = d.appliedDiscountPercent();
            BigDecimal expectedFinal = DiscountBroadcastFunction
                    .applyDiscount(d.order(), applied).finalAmount();
            assertThat(d.finalAmount()).isEqualByComparingTo(expectedFinal);

            BigDecimal ruleForProduct = knownRules.getOrDefault(d.order().productId(), BigDecimal.ZERO);
            assertThat(applied)
                    .satisfiesAnyOf(
                            v -> assertThat(v).isEqualByComparingTo(BigDecimal.ZERO),
                            v -> assertThat(v).isEqualByComparingTo(ruleForProduct));

            if (d.order().productId().equals("p3")) {
                assertThat(applied).isEqualByComparingTo(BigDecimal.ZERO);
            }
        }
    }
}
