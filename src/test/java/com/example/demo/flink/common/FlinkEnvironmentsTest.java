package com.example.demo.flink.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.demo.flink.FlinkKafkaJob;
import com.example.demo.flink.domain.Order;
import com.example.demo.flink.domain.UserOrderStats;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class FlinkEnvironmentsTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void appliesCheckpointSettingsToEnvironment(@TempDir Path tempDir) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String checkpointUri = asFileUri(tempDir.resolve("cp"));
        String savepointUri = asFileUri(tempDir.resolve("sp"));
        FlinkEnvironments.configureCheckpointing(env, checkpointUri, savepointUri);

        CheckpointConfig cc = env.getCheckpointConfig();
        assertThat(cc.getCheckpointInterval()).isEqualTo(FlinkEnvironments.CHECKPOINT_INTERVAL_MS);
        assertThat(cc.getMinPauseBetweenCheckpoints())
                .isEqualTo(FlinkEnvironments.MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        assertThat(cc.getCheckpointTimeout()).isEqualTo(FlinkEnvironments.CHECKPOINT_TIMEOUT_MS);
        assertThat(cc.getMaxConcurrentCheckpoints())
                .isEqualTo(FlinkEnvironments.MAX_CONCURRENT_CHECKPOINTS);
        assertThat(cc.getExternalizedCheckpointRetention())
                .isEqualTo(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        ReadableConfig configuration = env.getConfiguration();
        assertThat(configuration.get(CheckpointingOptions.SAVEPOINT_DIRECTORY))
                .isEqualTo(savepointUri);
    }

    @Test
    void statefulPipelineRunsWithRocksDbAndCheckpointing(@TempDir Path tempDir) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        FlinkEnvironments.configureCheckpointing(
                env, asFileUri(tempDir.resolve("cp")), asFileUri(tempDir.resolve("sp")));

        Order o1 = new Order("o1", "u1", "p1", new BigDecimal("10.00"),
                Instant.parse("2026-01-01T00:00:00Z"));
        Order o2 = new Order("o2", "u1", "p2", new BigDecimal("20.00"),
                Instant.parse("2026-01-01T00:00:30Z"));

        DataStream<Order> input = env.fromData(TypeInformation.of(Order.class), o1, o2);
        DataStream<UserOrderStats> result = FlinkKafkaJob.applyStatefulProcessing(input);

        List<UserOrderStats> collected = new ArrayList<>();
        try (CloseableIterator<UserOrderStats> it = result.executeAndCollect()) {
            it.forEachRemaining(collected::add);
        }

        assertThat(collected).hasSize(2);
        assertThat(collected.get(0).orderCount()).isEqualTo(1L);
        assertThat(collected.get(0).totalAmount()).isEqualByComparingTo("10.00");
        assertThat(collected.get(1).orderCount()).isEqualTo(2L);
        assertThat(collected.get(1).totalAmount()).isEqualByComparingTo("30.00");
    }

    private static String asFileUri(Path path) {
        return new File(path.toString()).toURI().toString();
    }
}
