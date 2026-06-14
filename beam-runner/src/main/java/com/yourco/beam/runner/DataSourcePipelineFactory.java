package com.yourco.beam.runner;

import com.google.api.services.bigquery.model.TableRow;
import com.yourco.beam.io.checkpoint.BigQueryCheckpointAdapter;
import com.yourco.beam.io.checkpoint.CheckpointAdapter;
import com.yourco.beam.io.sink.GcsSinkTransform;
import com.yourco.beam.io.source.SourceRouter;
import com.yourco.beam.io.status.BigQueryProcessStatusAdapter;
import com.yourco.beam.io.status.ProcessStatusAdapter;
import com.yourco.beam.model.BncRule;
import com.yourco.beam.model.CheckpointRecord;
import com.yourco.beam.model.OutputConfig;
import com.yourco.beam.model.ProcessStatusRecord;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.model.ValidationConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.ProcessType;
import com.yourco.beam.utils.DateUtils;
import com.yourco.beam.utils.db.DatabaseAdapter;
import com.yourco.beam.utils.db.DatabaseAdapterFactory;
import com.yourco.beam.utils.db.ParameterRepository;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Assembles and runs a {@code DATA_SOURCE_DOWNLOAD} pipeline.
 *
 * <h2>Revised architecture (per-source independent branches)</h2>
 * Sources are <em>never</em> merged. Each {@link SourceConfig} produces an independent
 * Beam DAG branch that flows:
 * <pre>
 *   source read → query param resolution → transform chain → per-source sink
 *                                              ↑
 *                              LOOKUP / GROUP_BY / SORT_BY transforms
 * </pre>
 *
 * <h2>Orchestration steps</h2>
 * <ol>
 *   <li><b>Connect to parameter DB</b> — JDBC adapter + Secret Manager password.
 *       Connection is opened, configs fetched, then immediately closed.</li>
 *   <li><b>Validate required parameters</b> — fail fast before launching Dataflow.</li>
 *   <li><b>Fetch source configs</b> — one or more {@link SourceConfig} objects, each carrying
 *       their {@code outputConfig}, {@code queryConfig}, {@code sourceTransforms}, and
 *       {@code validationConfig}.</li>
 *   <li><b>Filter by checkpoint</b> — skip FINISHED sources unless {@code --overrideDownload=true}.</li>
 *   <li><b>Write STARTED checkpoints + PENDING status</b> — one BQ row per source, written in
 *       the driver JVM before {@code pipeline.run()} so partial runs are visible.</li>
 *   <li><b>Assemble parallel source branches</b> — for each SourceConfig:
 *       source read → {@link SourceTransformChainAssembler} → per-source sink.</li>
 *   <li><b>Run pipeline</b> (called by {@link Main})</li>
 *   <li><b>Post-pipeline validation</b> — header check, row count, BnC against output table.
 *       Results determine COMPLETED vs VALIDATION_FAILED status.</li>
 *   <li><b>Write final status + checkpoints</b> — COMPLETED/FAILED/VALIDATION_FAILED per source.</li>
 * </ol>
 *
 * <h2>On-demand parameter access</h2>
 * DB connections are created and closed per access, not held open throughout. The driver JVM
 * accesses the DB in step 1-3 (config fetch) and step 8 (BnC queries against parameter DB).
 * Workers never hold a long-lived DB connection — they use {@code @Setup}/{@code @Teardown}
 * to create and close connections per-bundle.
 */
public final class DataSourcePipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourcePipelineFactory.class);

    // Held after assembly so Main can call runPostPipelineValidation()
    private List<SourceConfig> processedConfigs;
    private List<ProcessStatusRecord> pendingStatusRecords;
    private ProcessStatusAdapter statusAdapter;
    private CheckpointAdapter checkpointAdapter;
    private String jobRunId;

    /**
     * Validates parameters, assembles the Beam pipeline graph, and returns the pipeline
     * ready for {@code run()} in Main.
     *
     * <p>Does NOT call {@code pipeline.run()} — that is the caller's responsibility.
     */
    public Pipeline assemble(FrameworkOptions options) {
        // ── Resolve job run ID ─────────────────────────────────────────────
        jobRunId = options.getJobRunId();
        if (jobRunId == null || jobRunId.isBlank()) {
            jobRunId = UUID.randomUUID().toString();
            options.setJobRunId(jobRunId);
        }
        LOG.info("DATA_SOURCE_DOWNLOAD | jobRunId={} | datasource={} | period={} | subprocess={} "
                 + "| periodStart={} | periodEnd={}",
                 jobRunId, options.getDatasourceName(), options.getPeriodId(),
                 options.getSubprocessName(), options.getPeriodStart(), options.getPeriodEnd());

        // ── Step 1-3: DB validation and source config fetch ────────────────
        // Pattern: open → fetch → close. DB is not held open during graph assembly.
        List<SourceConfig> sourceConfigs;
        try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
            ParameterRepository repo = new ParameterRepository(db, options);
            validateRequiredParameters(repo, options);
            sourceConfigs = repo.fetchSourceConfigs(
                options.getDatasourceName(), options.getPeriodId(), options.getSubprocessName());
        }
        LOG.info("Found {} source config(s) for this run", sourceConfigs.size());

        // ── Step 4: Filter by checkpoint ──────────────────────────────────
        checkpointAdapter = new BigQueryCheckpointAdapter(options);
        statusAdapter     = new BigQueryProcessStatusAdapter(options);
        List<SourceConfig> toProcess = filterByCheckpoint(sourceConfigs, checkpointAdapter, options);

        if (toProcess.isEmpty()) {
            LOG.info("All {} source(s) already downloaded. "
                     + "Set --overrideDownload=true to force re-download.", sourceConfigs.size());
            return Pipeline.create(options);
        }
        LOG.info("Will process {} of {} source(s)", toProcess.size(), sourceConfigs.size());

        // ── Step 5: Write STARTED checkpoints + PENDING status rows ───────
        pendingStatusRecords = new ArrayList<>();
        for (SourceConfig config : toProcess) {
            LOG.info("Writing STARTED checkpoint + PENDING status for: {}", config.datasourceName);
            checkpointAdapter.writeCheckpoint(CheckpointRecord.started(jobRunId, config));

            ProcessStatusRecord pending = ProcessStatusRecord.pending(
                jobRunId, ProcessType.DATA_SOURCE_DOWNLOAD.name(), config,
                options.getPeriodStart(), options.getPeriodEnd());
            statusAdapter.write(pending);
            pendingStatusRecords.add(pending);
        }

        // ── Step 6: Assemble per-source independent pipeline branches ──────
        processedConfigs = toProcess;
        return assemblePipeline(options, toProcess);
    }

    /**
     * Called by {@link Main} after {@code waitUntilFinish()} completes.
     * Runs post-pipeline validation (row count, BnC) against the output tables and
     * writes final COMPLETED / VALIDATION_FAILED / FAILED status rows.
     *
     * @param pipelineState result from Beam's {@code waitUntilFinish()}
     * @param pipelineError exception from the pipeline, or null if it succeeded
     */
    public void runPostPipelineSteps(PipelineResult.State pipelineState, Throwable pipelineError) {
        if (processedConfigs == null) return;

        boolean pipelineSucceeded = (pipelineState == PipelineResult.State.DONE
                                     || pipelineState == PipelineResult.State.UPDATED)
                                    && pipelineError == null;

        for (int i = 0; i < processedConfigs.size(); i++) {
            SourceConfig config = processedConfigs.get(i);
            ProcessStatusRecord pending = pendingStatusRecords.get(i);

            if (!pipelineSucceeded) {
                // Pipeline failed — write FAILED status and FAILED checkpoint
                String errorMsg = pipelineError != null ? pipelineError.getMessage()
                                                        : "Pipeline ended in state: " + pipelineState;
                LOG.warn("Pipeline failed for source {}: {}", config.datasourceName, errorMsg);
                statusAdapter.write(ProcessStatusRecord.failed(pending, errorMsg));
                checkpointAdapter.writeCheckpoint(
                    CheckpointRecord.failed(jobRunId, config, errorMsg));
            } else {
                // Pipeline succeeded — run validation against output table
                runValidationAndWriteStatus(config, pending);
                checkpointAdapter.writeCheckpoint(
                    CheckpointRecord.finished(jobRunId, config, -1));
            }
        }
    }

    // ── Graph assembly ────────────────────────────────────────────────────────

    private Pipeline assemblePipeline(FrameworkOptions options, List<SourceConfig> configs) {
        Pipeline pipeline = Pipeline.create(options);
        LocalDate runDate = DateUtils.resolveRunDate(options);
        LOG.info("Effective run date: {}", runDate);

        for (SourceConfig config : configs) {
            LOG.info("Assembling source branch: {} ({})", config.datasourceName, config.sourceType);

            // 1. Read from source
            PCollection<Row> sourceData = SourceRouter.routeFromConfig(
                pipeline, config, options, runDate);

            // 2. Apply per-source transform chain (LOOKUP, GROUP_BY, SORT_BY)
            PCollection<Row> transformed = SourceTransformChainAssembler.assemble(
                sourceData, config, options, pipeline);

            // 3. Wire per-source output sink (never merged with other sources)
            wirePerSourceSink(transformed, config, options);
        }

        return pipeline;
    }

    // ── Per-source sink wiring ────────────────────────────────────────────────

    private static void wirePerSourceSink(PCollection<Row> data, SourceConfig config,
                                          FrameworkOptions options) {
        OutputConfig output = config.outputConfig;

        if (output == null) {
            LOG.warn("No output_config for source '{}' — data will not be written. "
                     + "Set output_type, output_bq_* or output_gcs_path in source_config.",
                     config.datasourceName);
            return;
        }

        String label = config.datasourceName;

        if (output.isBq()) {
            BigQueryIO.Write.WriteDisposition disposition = output.isTruncate()
                ? BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
                : BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

            String tableSpec = output.bqProjectId + ":" + output.bqDataset + "." + output.bqTable;
            LOG.info("Writing source '{}' → BQ table: {} ({})", label, tableSpec, output.writeMode);

            data.apply("RowToTableRow-" + label,
                       MapElements.into(TypeDescriptor.of(TableRow.class))
                                  .via(DataSourcePipelineFactory::rowToTableRow))
                .apply("BqSink-" + label,
                       BigQueryIO.writeTableRows()
                                 .to(tableSpec.replace(":", "."))
                                 .withWriteDisposition(disposition)
                                 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        } else if (output.isGcs()) {
            LOG.info("Writing source '{}' → GCS path: {}", label, output.gcsPath);
            data.apply("GcsSink-" + label, new GcsSinkTransform(options));
        } else {
            LOG.warn("Unknown output_type '{}' for source '{}' — skipping sink wiring",
                     output.outputType, label);
        }
    }

    // ── Row → TableRow converter ──────────────────────────────────────────────

    private static TableRow rowToTableRow(Row row) {
        TableRow tableRow = new TableRow();
        if (row == null || row.getSchema() == null) return tableRow;
        for (org.apache.beam.sdk.schemas.Schema.Field field : row.getSchema().getFields()) {
            Object val = row.getValue(field.getName());
            tableRow.set(field.getName(), val != null ? val.toString() : null);
        }
        return tableRow;
    }

    // ── Post-pipeline validation ──────────────────────────────────────────────

    /**
     * Runs header check, row count check, and BnC checks against the output BQ table.
     * Writes COMPLETED or VALIDATION_FAILED to the process_status table.
     *
     * <h2>Validation checks</h2>
     * <ol>
     *   <li>Header check: queries BQ table schema and confirms required columns exist.</li>
     *   <li>Row count: queries COUNT(*) and checks against min/max bounds.</li>
     *   <li>BnC: queries SUM(field) for each BnC rule and checks against expected total.</li>
     * </ol>
     *
     * All checks run even if an earlier one fails — results are aggregated into a JSON
     * validation_details blob written to the process_status table.
     */
    private void runValidationAndWriteStatus(SourceConfig config, ProcessStatusRecord pending) {
        ValidationConfig validation = config.validationConfig;
        OutputConfig output = config.outputConfig;

        if (!validation.hasAnyCheck() || output == null || !output.isBq()) {
            // No validation configured (or not a BQ output) — mark as COMPLETED
            statusAdapter.write(ProcessStatusRecord.completed(pending, -1, null));
            return;
        }

        String tableRef = output.bqTableRef();
        List<String> failures = new ArrayList<>();
        Map<String, Object> details = new HashMap<>();

        // Row count check
        long rowCount = -1L;
        if (validation.hasMinRowCheck() || validation.hasMaxRowCheck()) {
            rowCount = statusAdapter.queryRowCount(tableRef);
            details.put("row_count", rowCount);

            if (validation.hasMinRowCheck() && rowCount < validation.minRowCount) {
                failures.add("row_count " + rowCount + " < min " + validation.minRowCount);
            }
            if (validation.hasMaxRowCheck() && rowCount > validation.maxRowCount) {
                failures.add("row_count " + rowCount + " > max " + validation.maxRowCount);
            }
        }

        // BnC checks
        List<Map<String, Object>> bncResults = new ArrayList<>();
        for (BncRule rule : validation.bncRules) {
            double actualSum = statusAdapter.querySum(tableRef, rule.field);
            boolean passes   = !Double.isNaN(actualSum) && rule.passes(actualSum);
            Map<String, Object> bncDetail = new HashMap<>();
            bncDetail.put("field",         rule.field);
            bncDetail.put("expected_total", rule.expectedTotal);
            bncDetail.put("actual_total",  actualSum);
            bncDetail.put("tolerance_pct", rule.tolerancePct);
            bncDetail.put("passed",        passes);
            bncResults.add(bncDetail);
            if (!passes) {
                failures.add("BnC SUM(" + rule.field + ") actual=" + actualSum
                    + " expected=" + rule.expectedTotal + " ±" + rule.tolerancePct * 100 + "%");
            }
        }
        if (!bncResults.isEmpty()) details.put("bnc_checks", bncResults);

        String validationJson;
        try {
            validationJson = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(details);
        } catch (Exception e) {
            validationJson = details.toString();
        }

        if (failures.isEmpty()) {
            LOG.info("Validation PASSED for source '{}': {}", config.datasourceName, validationJson);
            statusAdapter.write(ProcessStatusRecord.completed(pending, rowCount, validationJson));
        } else {
            LOG.warn("Validation FAILED for source '{}': {}", config.datasourceName, failures);
            statusAdapter.write(ProcessStatusRecord.validationFailed(pending, rowCount, validationJson));
        }
    }

    // ── Validation helpers ────────────────────────────────────────────────────

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
                    LOG.info("Skipping '{}' — FINISHED_ACCESSING checkpoint found", config.datasourceName);
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
