package com.example.demo.flink.common;

import java.time.Duration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class FlinkEnvironments {

    public static final long CHECKPOINT_INTERVAL_MS = 10_000L;
    public static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 5_000L;
    public static final long CHECKPOINT_TIMEOUT_MS = 60_000L;
    public static final int MAX_CONCURRENT_CHECKPOINTS = 1;
    public static final int RESTART_ATTEMPTS = 3;
    public static final Duration RESTART_DELAY = Duration.ofSeconds(10);

    private FlinkEnvironments() {}

    public static void configureCheckpointing(
            StreamExecutionEnvironment env, String checkpointDir, String savepointDir) {
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, RESTART_ATTEMPTS);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, RESTART_DELAY);
        env.configure(config);

        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        cc.setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        cc.setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS);
        cc.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }
}
