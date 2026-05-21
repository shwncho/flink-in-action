package com.example.demo.flink;

import com.example.demo.flink.common.FlinkEnvironments;
import com.example.demo.flink.common.JsonSerde;
import com.example.demo.flink.domain.DiscountRule;
import com.example.demo.flink.domain.DiscountedOrder;
import com.example.demo.flink.domain.EnrichedOrder;
import com.example.demo.flink.domain.Order;
import com.example.demo.flink.domain.UserOrderStats;
import com.example.demo.flink.domain.UserWindowStats;
import com.example.demo.flink.function.DiscountBroadcastFunction;
import com.example.demo.flink.function.OrderAggregator;
import com.example.demo.flink.function.UserOrderStatsFunction;
import com.example.demo.flink.function.UserWindowStatsFunction;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public class FlinkKafkaJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "orders";
    private static final String OUTPUT_TOPIC = "enriched-orders";
    private static final String STATS_TOPIC = "user-order-stats";
    private static final String WINDOW_TOPIC = "user-order-windows";
    private static final String RULES_TOPIC = "discount-rules";
    private static final String DISCOUNTED_TOPIC = "discounted-orders";
    private static final String GROUP_ID = "flink-demo-consumer";
    private static final String RULES_GROUP_ID = "flink-demo-rules-consumer";
    private static final String CHECKPOINT_DIR = "file:///tmp/practice-flink-checkpoints";

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        FlinkEnvironments.configureCheckpointing(env, CHECKPOINT_DIR);

        KafkaSource<Order> source = KafkaSource.<Order>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonSerde<>(Order.class))
                .build();

        DataStream<Order> orders = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "orders-source"
        );

        DataStream<EnrichedOrder> processed = applyProcessing(orders);

        KafkaSink<EnrichedOrder> sink = KafkaSink.<EnrichedOrder>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<EnrichedOrder>builder()
                                .setTopic(OUTPUT_TOPIC)
                                .setValueSerializationSchema(new JsonSerde<>(EnrichedOrder.class))
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        processed.sinkTo(sink).name("enriched-orders-sink");
        processed.print().name("debug-print");

        DataStream<UserOrderStats> stats = applyStatefulProcessing(orders);

        KafkaSink<UserOrderStats> statsSink = KafkaSink.<UserOrderStats>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserOrderStats>builder()
                                .setTopic(STATS_TOPIC)
                                .setValueSerializationSchema(new JsonSerde<>(UserOrderStats.class))
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        stats.sinkTo(statsSink).name("user-order-stats-sink");
        stats.print().name("debug-stats-print");

        DataStream<UserWindowStats> windowStats = applyWindowedProcessing(orders);

        KafkaSink<UserWindowStats> windowSink = KafkaSink.<UserWindowStats>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserWindowStats>builder()
                                .setTopic(WINDOW_TOPIC)
                                .setValueSerializationSchema(new JsonSerde<>(UserWindowStats.class))
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        windowStats.sinkTo(windowSink).name("user-order-windows-sink");
        windowStats.print().name("debug-window-print");

        KafkaSource<DiscountRule> rulesSource = KafkaSource.<DiscountRule>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(RULES_TOPIC)
                .setGroupId(RULES_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonSerde<>(DiscountRule.class))
                .build();

        DataStream<DiscountRule> rules = env.fromSource(
                rulesSource,
                WatermarkStrategy.noWatermarks(),
                "discount-rules-source"
        );

        DataStream<DiscountedOrder> discounted = applyBroadcastProcessing(orders, rules);

        KafkaSink<DiscountedOrder> discountedSink = KafkaSink.<DiscountedOrder>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<DiscountedOrder>builder()
                                .setTopic(DISCOUNTED_TOPIC)
                                .setValueSerializationSchema(new JsonSerde<>(DiscountedOrder.class))
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        discounted.sinkTo(discountedSink).name("discounted-orders-sink");
        discounted.print().name("debug-discounted-print");

        env.execute("Flink Kafka Demo Job");
    }

    public static DataStream<EnrichedOrder> applyProcessing(DataStream<Order> source) {
        return source
                .map(order -> new EnrichedOrder(order, Instant.now()))
                .returns(TypeInformation.of(EnrichedOrder.class))
                .name("enrich-order");
    }

    public static DataStream<UserOrderStats> applyStatefulProcessing(DataStream<Order> source) {
        return source
                .keyBy(Order::userId)
                .process(new UserOrderStatsFunction())
                .returns(TypeInformation.of(UserOrderStats.class))
                .name("user-order-stats");
    }

    public static DataStream<UserWindowStats> applyWindowedProcessing(DataStream<Order> source) {
        return source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((order, ts) -> order.ts().toEpochMilli()))
                .keyBy(Order::userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new OrderAggregator(), new UserWindowStatsFunction())
                .returns(TypeInformation.of(UserWindowStats.class))
                .name("user-order-windows");
    }

    public static DataStream<DiscountedOrder> applyBroadcastProcessing(
            DataStream<Order> orders,
            DataStream<DiscountRule> rules) {
        BroadcastStream<DiscountRule> broadcastRules =
                rules.broadcast(DiscountBroadcastFunction.RULE_STATE_DESCRIPTOR);

        return orders
                .connect(broadcastRules)
                .process(new DiscountBroadcastFunction())
                .returns(TypeInformation.of(DiscountedOrder.class))
                .name("apply-discount");
    }
}
