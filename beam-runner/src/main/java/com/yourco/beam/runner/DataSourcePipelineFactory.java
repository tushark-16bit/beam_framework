package com.yourco.beam.runner;

import com.yourco.beam.io.checkpoint.BigQueryCheckpointAdapter;
import com.yourco.beam.io.checkpoint.CheckpointAdapter;
import com.yourco.beam.io.sink.DeadLetterSinkTransform;
import com.yourco.beam.io.sink.SinkRouter;
import com.yourco.beam.io.source.SourceRouter;
import com.yourco.beam.model.CheckpointRecord;
import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.DateUtils;
import java.time.LocalDate;
import java.util.stream.Collectors;
import com.yourco.beam.utils.db.DatabaseAdapter;
import com.yourco.beam.utils.db.DatabaseAdapterFactory;
import com.yourco.beam.utils.db.ParameterRepository;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Assembles and runs a {@code DATA_SOURCE_DOWNLOAD} pipeline.
 *
 * <h2>Orchestration steps</h2>
 * <ol>
 *   <li><b>Connect to parameter DB</b> — JDBC adapter from CLI options + Secret Manager password.</li>
 *   <li><b>Validate required parameters</b> — fail fast if source configuration is incomplete.
 *       Better to fail before launching Dataflow than to fail mid-run.</li>
 *   <li><b>Fetch source configs</b> — one or more {@link SourceConfig} objects keyed by
 *       (datasourceName, periodId, subprocessName).</li>
 *   <li><b>Filter by checkpoint</b> — skip sources already marked {@code FINISHED_ACCESSING}
 *       unless {@code --overrideDownload=true}.</li>
 *   <li><b>Write STARTED checkpoints</b> — one BQ row per source, before {@code pipeline.run()}.
 *       This ensures partial runs are visible even if the job is killed.</li>
 *   <li><b>Assemble parallel sources</b> — each {@link SourceConfig} becomes an independent
 *       Beam branch. Dataflow runs all branches concurrently on worker machines.</li>
 *   <li><b>Flatten and write to sink</b> — all source outputs merged into one PCollection
 *       and written to the configured sink (GCS or BQ).</li>
 *   <li><b>Wire DLQ</b> — failed records routed to {@code --deadLetterSink}.</li>
 *   <li><b>Write FINISHED/FAILED checkpoints</b> — after {@code waitUntilFinish()} returns,
 *       state is persisted to the checkpoint table.</li>
 * </ol>
 *
 * <h2>Why the DB connection is closed before pipeline.run()</h2>
 * All the information we need from the DB ({@link SourceConfig} objects) is fetched upfront
 * and stored in memory as serializable objects. The pipeline then ships those objects to
 * Dataflow workers as DoFn constructor fields. There is no need for a live DB connection
 * during the Beam graph execution phase, so we close it early to release the pool.
 *
 * <h2>Checkpoint guarantees</h2>
 * Checkpoints are best-effort: if the driver JVM is killed between {@code pipeline.run()}
 * and the FINISHED write, the checkpoint will be stuck in STARTED state. The next run
 * will therefore re-download (because STARTED ≠ FINISHED). This is safe — downloads are
 * idempotent by design (the sink uses TRUNCATE or time-partitioned output paths).
 */
public final class DataSourcePipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourcePipelineFactory.class);

    /**
     * Full orchestration: validate, checkpoint, assemble Beam graph, run, update checkpoints.
     *
     * @return the assembled {@link Pipeline} ready for {@code run()} in Main
     */
    public Pipeline assemble(FrameworkOptions options) {

        // ── Resolve job run ID ─────────────────────────────────────────────
        String jobRunId = options.getJobRunId();
        if (jobRunId == null || jobRunId.isBlank()) {
            jobRunId = UUID.randomUUID().toString();
            options.setJobRunId(jobRunId);
        }
        LOG.info("DATA_SOURCE_DOWNLOAD | jobRunId={} | datasource={} | period={} | subprocess={}",
                 jobRunId, options.getDatasourceName(), options.getPeriodId(), options.getSubprocessName());

        // ── Step 1-3: DB validation and source config fetch ────────────────
        // Close the adapter immediately after fetching — the pipeline doesn't need
        // a live DB connection during graph assembly or execution.
        List<SourceConfig> sourceConfigs;
        try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
            ParameterRepository repo = new ParameterRepository(db, options);
            validateRequiredParameters(repo, options);
            sourceConfigs = repo.fetchSourceConfigs(
                options.getDatasourceName(), options.getPeriodId(), options.getSubprocessName());
        }

        LOG.info("Found {} source config(s) for this run", sourceConfigs.size());

        // ── Step 4: Filter by checkpoint ──────────────────────────────────
        CheckpointAdapter checkpoint = new BigQueryCheckpointAdapter(options);
        List<SourceConfig> toProcess = filterByCheckpoint(sourceConfigs, checkpoint, options);

        if (toProcess.isEmpty()) {
            LOG.info("All {} source(s) already downloaded. "
                     + "Set --overrideDownload=true to force re-download.", sourceConfigs.size());
            // Return a no-op pipeline so Main doesn't fail
            return Pipeline.create(options);
        }
        LOG.info("Will process {} of {} source(s) (skipping already-finished ones)",
                 toProcess.size(), sourceConfigs.size());

        // ── Step 5: Write STARTED checkpoints ─────────────────────────────
        // Written before pipeline.run() so partial runs are visible in the checkpoint table.
        String finalJobRunId = jobRunId;
        toProcess.forEach(config -> {
            LOG.info("Writing STARTED checkpoint for: {}", config.datasourceName);
            checkpoint.writeCheckpoint(CheckpointRecord.started(finalJobRunId, config));
        });

        // ── Step 6-8: Assemble the Beam pipeline ──────────────────────────
        return assemblePipeline(options, toProcess, checkpoint, finalJobRunId);
    }

    // ── Graph assembly ────────────────────────────────────────────────────────

    private Pipeline assemblePipeline(FrameworkOptions options, List<SourceConfig> configs,
                                      CheckpointAdapter checkpoint, String jobRunId) {
        Pipeline pipeline = Pipeline.create(options);

        // Resolve the effective run date once for all sources in this job
        LocalDate runDate = DateUtils.resolveRunDate(options);
        LOG.info("Effective run date: {}", runDate);

        // Each source config becomes an independent Beam branch.
        // Dataflow runs all branches in parallel on worker machines.
        List<PCollection<Row>> sourceBranches = new ArrayList<>();
        List<PCollection<FailedRecord>> dlqBranches = new ArrayList<>();

        for (SourceConfig config : configs) {
            LOG.info("Adding source branch: {} ({})", config.datasourceName, config.sourceType);
            PCollection<Row> sourceRows = SourceRouter.routeFromConfig(pipeline, config, options, runDate);
            sourceBranches.add(sourceRows);
            // Source transforms currently don't produce DLQ outputs (raw data ingestion).
            // Add DLQ wiring here if you add per-source validation transforms.
        }

        // Flatten all source branches into a single PCollection for the shared sink.
        // Schema compatibility: all sources produce Schemas.RAW_JSON so this is safe.
        PCollection<Row> allRows = PCollectionList.of(sourceBranches)
            .apply("FlattenSources", Flatten.pCollections());

        // ── Step 7: Write to sink ──────────────────────────────────────────
        LOG.info("Configuring sink: {}", options.getSinkType());
        SinkRouter.route(allRows, options);

        // ── DLQ wiring (for future per-source validation) ─────────────────
        if (!dlqBranches.isEmpty()) {
            String dlqSink = options.getDeadLetterSink();
            if (dlqSink != null && !dlqSink.isBlank()) {
                PCollection<FailedRecord> allFailures = PCollectionList.of(dlqBranches)
                    .apply("FlattenDlq", Flatten.pCollections());
                allFailures.apply("WriteDLQ", new DeadLetterSinkTransform(options));
            }
        }

        return pipeline;
    }

    // ── Validation ────────────────────────────────────────────────────────────

    private void validateRequiredParameters(ParameterRepository repo, FrameworkOptions options) {
        String datasource  = options.getDatasourceName();
        String period      = options.getPeriodId();
        String subprocess  = options.getSubprocessName();

        if (datasource == null || datasource.isBlank()) {
            throw new PipelineConfigurationException("--datasourceName is required for DATA_SOURCE_DOWNLOAD");
        }
        if (period == null || period.isBlank()) {
            throw new PipelineConfigurationException("--periodId is required for DATA_SOURCE_DOWNLOAD");
        }

        LOG.info("Validating required parameters in DB for datasource={}, period={}, subprocess={}",
                 datasource, period, subprocess);
        List<String> missing = repo.getMissingParameters(datasource, period, subprocess);
        if (!missing.isEmpty()) {
            throw new PipelineConfigurationException(
                "Required parameters missing from DB — cannot start pipeline. Missing: " + missing
                + ". Datasource=" + datasource + ", period=" + period + ", subprocess=" + subprocess);
        }
        LOG.info("All required parameters present.");
    }

    // ── Checkpoint filtering ──────────────────────────────────────────────────

    private List<SourceConfig> filterByCheckpoint(List<SourceConfig> configs,
                                                   CheckpointAdapter checkpoint,
                                                   FrameworkOptions options) {
        if (options.getOverrideDownload()) {
            LOG.info("--overrideDownload=true: skipping checkpoint check, re-downloading all sources");
            return configs;
        }
        return configs.stream()
            .filter(config -> {
                boolean done = checkpoint.isDownloadComplete(
                    config.datasourceName, config.periodId, config.subprocessName);
                if (done) {
                    LOG.info("Skipping {} — FINISHED_ACCESSING checkpoint found", config.datasourceName);
                }
                return !done;
            })
            .collect(Collectors.toList());
    }

    // ── Exception type ────────────────────────────────────────────────────────

    public static final class PipelineConfigurationException extends RuntimeException {
        public PipelineConfigurationException(String message) { super(message); }
    }
}
