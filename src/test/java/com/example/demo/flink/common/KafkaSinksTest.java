package com.example.demo.flink.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.junit.jupiter.api.Test;

class KafkaSinksTest {

    @Test
    void exactlyOnce_buildsKafkaSinkWithoutThrowing() {
        KafkaSink<String> sink = KafkaSinks.exactlyOnce(
                "localhost:9092",
                "test-topic",
                "test-prefix-",
                new SimpleStringSchema());

        assertThat(sink).isNotNull();
    }
}
