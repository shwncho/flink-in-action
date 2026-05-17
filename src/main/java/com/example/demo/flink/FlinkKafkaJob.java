package com.example.demo.flink;

import com.example.demo.flink.common.JsonSerde;
import com.example.demo.flink.domain.EnrichedOrder;
import com.example.demo.flink.domain.Order;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "orders";
    private static final String OUTPUT_TOPIC = "enriched-orders";
    private static final String GROUP_ID = "flink-demo-consumer";

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

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

        env.execute("Flink Kafka Demo Job");
    }

    public static DataStream<EnrichedOrder> applyProcessing(DataStream<Order> source) {
        return source
                .map(order -> new EnrichedOrder(order, Instant.now()))
                .returns(TypeInformation.of(EnrichedOrder.class))
                .name("enrich-order");
    }
}
