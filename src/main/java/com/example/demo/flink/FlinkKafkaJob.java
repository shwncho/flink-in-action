package com.example.demo.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "demo-topic";
    private static final String GROUP_ID = "flink-demo-consumer";

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        applyProcessing(stream).print();

        env.execute("Flink Kafka Demo Job");
    }

    public static DataStream<String> applyProcessing(DataStream<String> source) {
        return source.map(value -> "[Flink processed] " + value.toUpperCase());
    }
}
