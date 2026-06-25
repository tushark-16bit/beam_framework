package com.yourco.beam.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.checkpoint.DataSourceCheckpointAdapter;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.io.records.BigQueryDataSourceRecordAdapter;
import com.yourco.beam.io.records.DataSourceRecordAdapter;
import com.yourco.beam.io.sink.DataSourceRecordSinkTransform;
import com.yourco.beam.io.source.SourceRouter;
import com.yourco.beam.model.BncRule;
import com.yourco.beam.model.DataSourceCheckpoint;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.model.ValidationConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.DateUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Assembles and runs a {@code DATA_SOURCE_DOWNLOAD} pipeline.
 *
 * <h2>Per-source independent branches</h2>
 * Each {@link SourceConfig} produces an independent Beam DAG branch:
 * <pre>
 *   source read → transform chain → DataSourceRecordSinkTransform
 *                                          ↓
 *                               data_source_records table (JSON blobs)
 * </pre>
 *
 * <h2>Orchestration steps</h2>
 * <ol>
 *   <li><b>Fetch source config from BQ</b> — {@link BigQuerySourceConfigRepository}.</li>
 *   <li><b>Validate required parameters</b> — fail fast before launching Dataflow.</li>
 *   <li><b>Filter by checkpoint</b> — skip COMPLETED sources unless {@code --overrideDownload=true}.</li>
 *   <li><b>Create LOADING checkpoints</b> — one BQ row per source; returns the {@code dataSourceId}
 *       used for all record rows and the final status update.</li>
 *   <li><b>Assemble parallel source branches</b> — source read → transforms →
 *       {@link DataSourceRecordSinkTransform} (all rows stored as JSON blobs, keyed by dataSourceId).</li>
 *   <li><b>Run pipeline</b> (called by {@link Main})</li>
 *   <li><b>Post-pipeline validation</b> — row count and BnC against the record table.
 *       Results written to {@code BalAndCntlSmryTx}; checkpoint updated to COMPLETED / FAILED_BNC / FAILED.</li>
 * </ol>
 */
public final class DataSourcePipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourcePipelineFactory.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Held after assembly so Main can call runPostPipelineSteps()
    private List<SourceConfig>               processedConfigs;
    private Map<String, Long>                dataSourceIds;   // datasourceName → dataSourceId
    private DataSourceCheckpointAdapter      checkpointAdapter;
    private DataSourceRecordAdapter          recordAdapter;

    /**
     * Validates parameters, assembles the Beam pipeline graph, and returns the pipeline
     * ready for {@code run()} in Main.
     *
     * <p>Does NOT call {@code pipeline.run()} — that is the caller's responsibility.
     */
    public Pipeline assemble(FrameworkOptions options) {
        // ── Resolve job run ID ─────────────────────────────────────────────
        String jobRunId = options.getJobRunId();
        if (jobRunId == null || jobRunId.isBlank()) {
            jobRunId = UUID.randomUUID().toString();
            options.setJobRunId(jobRunId);
        }
        LOG.info("DATA_SOURCE_DOWNLOAD | jobRunId={} | datasource={} | period={} | subprocess={}",
                 jobRunId, options.getDatasourceName(), options.getPeriodId(),
                 options.getSubprocessName());

        // ── Step 1-2: BQ source config fetch and parameter validation ─────
        BigQuerySourceConfigRepository bqRepo = new BigQuerySourceConfigRepository(options);
        validateRequiredParameters(bqRepo, options);
        List<SourceConfig> sourceConfigs = bqRepo.fetchSourceConfigs(
            options.getParentId(), options.getDatasourceName(),
            options.getSubprocessName(), options.getPeriodId());
        LOG.info("Found {} source config(s) for this run", sourceConfigs.size());

        // ── Step 3: Filter by checkpoint ──────────────────────────────────
        checkpointAdapter = new BigQueryDataSourceCheckpointAdapter(options);
        recordAdapter     = new BigQueryDataSourceRecordAdapter(options);
        List<SourceConfig> toProcess = filterByCheckpoint(sourceConfigs, options);

        if (toProcess.isEmpty()) {
            LOG.info("All {} source(s) already completed. "
                     + "Set --overrideDownload=true to force re-download.", sourceConfigs.size());
            return Pipeline.create(options);
        }
        LOG.info("Will process {} of {} source(s)", toProcess.size(), sourceConfigs.size());

        // ── Step 4: Create LOADING checkpoints ────────────────────────────
        dataSourceIds = new HashMap<>();
        for (SourceConfig config : toProcess) {
            String dsNm = extractDsNm(config);
            long dsId = checkpointAdapter.createCheckpoint(
                config.datasourceName, config.periodId, dsNm);
            dataSourceIds.put(config.datasourceName, dsId);
            LOG.info("LOADING checkpoint created for '{}': dataSourceId={}", config.datasourceName, dsId);
        }

        // ── Step 5-6: Assemble per-source independent pipeline branches ────
        processedConfigs = toProcess;
        return assemblePipeline(options, toProcess);
    }

    /**
     * Called by {@link Main} after {@code waitUntilFinish()} completes.
     * Runs post-pipeline validation (row count, BnC) against the record table
     * and updates each checkpoint to COMPLETED / FAILED_BNC / FAILED.
     *
     * @param pipelineState result from Beam's {@code waitUntilFinish()}
     * @param pipelineError exception from the pipeline, or null if it succeeded
     */
    public void runPostPipelineSteps(PipelineResult.State pipelineState, Throwable pipelineError) {
        if (processedConfigs == null) return;

        boolean pipelineSucceeded = (pipelineState == PipelineResult.State.DONE
                                     || pipelineState == PipelineResult.State.UPDATED)
                                    && pipelineError == null;

        for (SourceConfig config : processedConfigs) {
            long dsId = dataSourceIds.get(config.datasourceName);

            if (!pipelineSucceeded) {
                String errorMsg = pipelineError != null ? pipelineError.getMessage()
                                                        : "Pipeline ended in state: " + pipelineState;
                LOG.warn("Pipeline failed for source '{}': {}", config.datasourceName, errorMsg);
                checkpointAdapter.updateStatus(dsId, DataSourceCheckpoint.STA_FAILED, null);
            } else {
                try {
                    runValidationAndUpdateCheckpoint(config, dsId);
                } catch (Exception e) {
                    LOG.error("Post-pipeline validation failed for '{}' (dataSourceId={}): {}",
                              config.datasourceName, dsId, e.getMessage(), e);
                    checkpointAdapter.updateStatus(dsId, DataSourceCheckpoint.STA_FAILED, null);
                }
            }
        }
    }

    // ── Graph assembly ────────────────────────────────────────────────────────

    private Pipeline assemblePipeline(FrameworkOptions options, List<SourceConfig> configs) {
        Pipeline pipeline = Pipeline.create(options);
        LocalDate runDate = DateUtils.resolveRunDate(options);
        LOG.info("Effective run date: {}", runDate);

        for (SourceConfig config : configs) {
            long dsId = dataSourceIds.get(config.datasourceName);
            LOG.info("Assembling source branch: {} ({}) → record table (dataSourceId={})",
                     config.datasourceName, config.sourceType, dsId);

            PCollection<Row> sourceData = SourceRouter.routeFromConfig(
                pipeline, config, options, runDate);

            PCollection<Row> transformed = SourceTransformChainAssembler.assemble(
                sourceData, config, options, pipeline);

            transformed.apply("RecordSink-" + config.datasourceName,
                              new DataSourceRecordSinkTransform(options, dsId));
        }

        return pipeline;
    }

    // ── Post-pipeline validation ──────────────────────────────────────────────

    /**
     * Validates row count and BnC rules against the record table, then updates the
     * checkpoint to COMPLETED, FAILED_BNC, or COMPLETED (with BnC mismatch in summary).
     *
     * <p>The BnC summary ({@code BalAndCntlSmryTx}) is a JSON object:
     * <pre>{@code
     * {
     *   "status":    "Matched",
     *   "srcCount":  1000,
     *   "srcAmount": 5000000.00,
     *   "dstCount":  1000,
     *   "dstAmount": 5000000.00
     * }
     * }</pre>
     */
    private void runValidationAndUpdateCheckpoint(SourceConfig config, long dsId) {
        ValidationConfig validation = config.validationConfig;
        long rowCount = recordAdapter.countRecords(dsId);

        // -1 means the count query itself failed — treat as infrastructure error, not a BnC miss
        if (rowCount == -1L) {
            LOG.error("Record count query failed for '{}' (dataSourceId={}) — marking FAILED",
                      config.datasourceName, dsId);
            checkpointAdapter.updateStatus(dsId, DataSourceCheckpoint.STA_FAILED,
                "{\"error\":\"record count query failed — see pipeline logs\"}");
            return;
        }
        LOG.info("Record count for '{}' (dataSourceId={}): {}", config.datasourceName, dsId, rowCount);

        List<String> failures = new ArrayList<>();
        boolean infraError = false;

        // Row count check
        if (validation.hasMinRowCheck() && rowCount < validation.minRowCount) {
            failures.add("row_count " + rowCount + " < min " + validation.minRowCount);
        }
        if (validation.hasMaxRowCheck() && rowCount > validation.maxRowCount) {
            failures.add("row_count " + rowCount + " > max " + validation.maxRowCount);
        }

        // BnC checks — query SUM(JSON_VALUE(...)) per rule
        Map<String, Object> bncSummary = new LinkedHashMap<>();
        bncSummary.put("srcCount", rowCount);
        bncSummary.put("dstCount", rowCount);

        for (BncRule rule : validation.bncRules) {
            double actual = recordAdapter.sumField(dsId, rule.field);
            if (Double.isNaN(actual)) {
                // NaN means the BQ query failed, not a data mismatch — mark FAILED, not FAILED_BNC
                failures.add("BnC SUM(" + rule.field + ") query failed (infrastructure error — check logs)");
                infraError = true;
            } else {
                bncSummary.put("srcAmount_" + rule.field, rule.expectedTotal);
                bncSummary.put("dstAmount_" + rule.field, actual);
                if (!rule.passes(actual)) {
                    failures.add("BnC SUM(" + rule.field + ") actual=" + actual
                        + " expected=" + rule.expectedTotal + " ±" + rule.tolerancePct * 100 + "%");
                }
            }
        }

        String staCd;
        if (failures.isEmpty()) {
            bncSummary.put("status", "Matched");
            staCd = DataSourceCheckpoint.STA_COMPLETED;
            LOG.info("Validation PASSED for '{}'", config.datasourceName);
        } else {
            bncSummary.put("status", "Not Matched");
            bncSummary.put("failures", failures);
            // Infrastructure failures (query errors) map to FAILED; data mismatches to FAILED_BNC
            staCd = infraError ? DataSourceCheckpoint.STA_FAILED : DataSourceCheckpoint.STA_FAILED_BNC;
            LOG.warn("Validation FAILED for '{}': {}", config.datasourceName, failures);
        }

        String bncJson = toJson(bncSummary);
        checkpointAdapter.updateStatus(dsId, staCd, bncJson);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String extractDsNm(SourceConfig config) {
        if (config.bqFetchConfig != null) {
            return config.bqFetchConfig.projectId + "."
                + config.bqFetchConfig.dataset + "."
                + config.bqFetchConfig.table;
        }
        if (config.fileConfig != null && config.fileConfig.location != null) {
            return config.fileConfig.location;
        }
        if (config.apiConfig != null && config.apiConfig.endpoint != null) {
            return config.apiConfig.endpoint;
        }
        return config.datasourceName;
    }

    private static String toJson(Map<String, Object> map) {
        try {
            return MAPPER.writeValueAsString(map);
        } catch (Exception e) {
            return map.toString();
        }
    }

    private void validateRequiredParameters(BigQuerySourceConfigRepository repo, FrameworkOptions options) {
        String datasource = options.getDatasourceName();
        String period     = options.getPeriodId();
        String subprocess = options.getSubprocessName();

        if (datasource == null || datasource.isBlank()) {
            throw new PipelineConfigurationException("--datasourceName is required for DATA_SOURCE_DOWNLOAD");
        }
        if (period == null || period.isBlank()) {
            throw new PipelineConfigurationException("--periodId is required for DATA_SOURCE_DOWNLOAD");
        }

        LOG.info("Validating required parameters in BQ for datasource={}, period={}, subprocess={}",
                 datasource, period, subprocess);
        List<String> missing = repo.getMissingParameters(
            options.getParentId(), datasource, subprocess, period);
        if (!missing.isEmpty()) {
            throw new PipelineConfigurationException(
                "Required parameters missing from BQ — cannot start pipeline. Missing: " + missing
                + ". Datasource=" + datasource + ", period=" + period + ", subprocess=" + subprocess);
        }
        LOG.info("All required parameters present.");
    }

    private List<SourceConfig> filterByCheckpoint(List<SourceConfig> configs, FrameworkOptions options) {
        if (options.getOverrideDownload()) {
            LOG.info("--overrideDownload=true: skipping checkpoint check, re-downloading all sources");
            return configs;
        }
        return configs.stream()
            .filter(config -> {
                boolean done = checkpointAdapter.isCompleted(config.datasourceName, config.periodId);
                if (done) {
                    LOG.info("Skipping '{}' — COMPLETED checkpoint found", config.datasourceName);
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
