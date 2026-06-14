package com.yourco.beam.runner;

import com.yourco.beam.io.checkpoint.BigQueryCheckpointAdapter;
import com.yourco.beam.io.checkpoint.CheckpointAdapter;
import com.yourco.beam.model.CheckpointRecord;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import com.yourco.beam.utils.db.DatabaseAdapter;
import com.yourco.beam.utils.db.DatabaseAdapterFactory;
import com.yourco.beam.utils.db.ParameterRepository;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Entry point for the Beam Pipeline Framework.
 *
 * <h2>Routing by process type</h2>
 * <pre>
 *   --processType=DATA_SOURCE_DOWNLOAD  →  DataSourcePipelineFactory
 *   --processType=REPORT_PROCESSING     →  PipelineFactory (existing general-purpose factory)
 * </pre>
 *
 * <h2>DATA_SOURCE_DOWNLOAD lifecycle with checkpoints</h2>
 * <pre>
 *   1. DataSourcePipelineFactory.assemble()   — validates, fetches configs, writes
 *                                               STARTED checkpoints, assembles graph
 *   2. pipeline.run().waitUntilFinish()        — data flows
 *   3. On success: write FINISHED checkpoints
 *   4. On failure: write FAILED checkpoints
 * </pre>
 *
 * <h2>REPORT_PROCESSING lifecycle</h2>
 * <pre>
 *   1. PipelineFactory.assemble()             — assembles transform chain
 *   2. pipeline.run()                         — data flows
 *   3. waitUntilFinish() for batch; streaming runs indefinitely
 * </pre>
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOG.info("Starting Beam Pipeline Framework");

        FrameworkOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(FrameworkOptions.class);

        LOG.info("Process type:  {}", options.getProcessType());
        LOG.info("Job run ID:    {}", options.getJobRunId());
        LOG.info("Sink:          {}", options.getSinkType());

        switch (options.getProcessType()) {
            case DATA_SOURCE_DOWNLOAD -> runDataSourceDownload(options);
            case REPORT_PROCESSING    -> runReportProcessing(options);
        }
    }

    // ── DATA_SOURCE_DOWNLOAD ─────────────────────────────────────────────────

    private static void runDataSourceDownload(FrameworkOptions options) {
        LOG.info("DATA_SOURCE_DOWNLOAD | datasource={} | period={} | subprocess={}",
                 options.getDatasourceName(), options.getPeriodId(), options.getSubprocessName());

        DataSourcePipelineFactory factory = new DataSourcePipelineFactory();
        Pipeline pipeline = factory.assemble(options);

        // Fetch source configs again for checkpoint writing after the run.
        // We need the configs to write FINISHED/FAILED records per-source.
        List<SourceConfig> processedConfigs = fetchProcessedConfigs(options);

        LOG.info("Submitting to runner: {}", options.getRunner().getSimpleName());
        PipelineResult result = pipeline.run();

        CheckpointAdapter checkpoint = new BigQueryCheckpointAdapter(options);
        try {
            result.waitUntilFinish();

            // All sources succeeded — write FINISHED checkpoints
            PipelineResult.State state = result.getState();
            if (state == PipelineResult.State.DONE) {
                LOG.info("Pipeline finished successfully.");
                processedConfigs.forEach(config -> {
                    LOG.info("Writing FINISHED checkpoint for: {}", config.datasourceName);
                    checkpoint.writeCheckpoint(
                        CheckpointRecord.finished(options.getJobRunId(), config, 0L));
                });
            } else {
                LOG.warn("Pipeline finished with state: {} — writing FAILED checkpoints.", state);
                writeFailedCheckpoints(processedConfigs, options, checkpoint,
                    "Pipeline finished with state: " + state);
            }
        } catch (Exception e) {
            LOG.error("Pipeline run failed: {}", e.getMessage(), e);
            writeFailedCheckpoints(processedConfigs, options, checkpoint, e.getMessage());
            throw new RuntimeException("DATA_SOURCE_DOWNLOAD failed", e);
        }
    }

    // ── REPORT_PROCESSING ────────────────────────────────────────────────────

    private static void runReportProcessing(FrameworkOptions options) {
        LOG.info("REPORT_PROCESSING | source={} | chain={} | sink={}",
                 options.getSourceType(), options.getTransformChain(), options.getSinkType());

        Pipeline pipeline = new PipelineFactory().assemble(options);

        LOG.info("Submitting to runner: {}", options.getRunner().getSimpleName());
        PipelineResult result = pipeline.run();

        if (isBatchSource(options.getSourceType())) {
            result.waitUntilFinish();
            LOG.info("Pipeline finished with state: {}", result.getState());
        } else {
            LOG.info("Streaming pipeline submitted. Job running indefinitely until cancelled.");
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Re-fetches the source configs that were processed in this run.
     * Used to write FINISHED/FAILED checkpoints after the pipeline completes.
     * If the DB is unavailable, returns empty list (checkpoints best-effort).
     */
    private static List<SourceConfig> fetchProcessedConfigs(FrameworkOptions options) {
        if (options.getParamDbUrl() == null) return Collections.emptyList();
        try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
            ParameterRepository repo = new ParameterRepository(db, options);
            return repo.fetchSourceConfigs(
                options.getDatasourceName(), options.getPeriodId(), options.getSubprocessName());
        } catch (Exception e) {
            LOG.warn("Could not fetch source configs for post-run checkpoint update: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private static void writeFailedCheckpoints(List<SourceConfig> configs, FrameworkOptions options,
                                                CheckpointAdapter checkpoint, String errorMsg) {
        configs.forEach(config ->
            checkpoint.writeCheckpoint(
                CheckpointRecord.failed(options.getJobRunId(), config, errorMsg)));
    }

    /** Returns {@code true} for bounded sources. Null sourceType defaults to batch. */
    private static boolean isBatchSource(SourceType sourceType) {
        if (sourceType == null) return true;
        return switch (sourceType) {
            case GCS, BQ, API, FILE -> true;
            case PUBSUB              -> false;
        };
    }
}
