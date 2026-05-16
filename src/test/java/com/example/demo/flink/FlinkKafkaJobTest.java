package com.example.demo.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
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
    void appliesPrefixAndUppercase() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> input = env.fromData("hello", "flink", "miniCluster");
        DataStream<String> result = FlinkKafkaJob.applyProcessing(input);

        List<String> collected = new ArrayList<>();
        try (CloseableIterator<String> it = result.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }

        assertThat(collected).containsExactly(
                "[Flink processed] HELLO",
                "[Flink processed] FLINK",
                "[Flink processed] MINICLUSTER"
        );
    }

    @Test
    void preservesInputOrderWithParallelismOne() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> input = env.fromData("a", "b", "c");
        DataStream<String> result = FlinkKafkaJob.applyProcessing(input);

        List<String> collected = new ArrayList<>();
        try (CloseableIterator<String> it = result.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }

        assertThat(collected).containsExactly(
                "[Flink processed] A",
                "[Flink processed] B",
                "[Flink processed] C"
        );
    }
}
