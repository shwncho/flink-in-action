package com.example.demo.flink.common;

import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class KafkaSinks {

    public static final Duration TRANSACTION_TIMEOUT = Duration.ofMinutes(2);

    private KafkaSinks() {}

    public static <T> KafkaSink<T> exactlyOnce(
            String bootstrapServers,
            String topic,
            String transactionalIdPrefix,
            SerializationSchema<T> valueSerializer) {
        Properties producerProps = new Properties();
        producerProps.setProperty(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                String.valueOf(TRANSACTION_TIMEOUT.toMillis()));

        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<T>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(valueSerializer)
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalIdPrefix)
                .setKafkaProducerConfig(producerProps)
                .build();
    }
}
