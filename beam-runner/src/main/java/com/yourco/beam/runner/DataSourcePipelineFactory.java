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
import com.yourco.beam.model.BqFetchConfig;
import com.yourco.beam.model.DataSourceCheckpoint;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.model.ValidationConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import com.yourco.beam.utils.BigQuerySchemaUtils;
import com.yourco.beam.utils.DateUtils;
import com.yourco.beam.utils.QueryParameterResolver;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
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
 *                               DaRec table (JSON blobs keyed by da_id)
 * </pre>
 *
 * <h2>Orchestration steps</h2>
 * <ol>
 *   <li><b>Fetch source config from BQ</b> — {@link BigQuerySourceConfigRepository}.</li>
 *   <li><b>Validate required parameters</b> — fail fast before launching Dataflow.</li>
 *   <li><b>Filter by checkpoint</b> — skip COMPLETED sources unless {@code --overrideDownload=true}.</li>
 *   <li><b>Create LOADING DaRefer rows</b> — one row per source; returns the {@code da_id}
 *       used for all record rows and the final status update.</li>
 *   <li><b>Assemble parallel source branches</b> — source read → transforms →
 *       {@link DataSourceRecordSinkTransform} (all rows stored as JSON blobs in DaRec, keyed by da_id).</li>
 *   <li><b>Run pipeline</b> (called by {@link Main})</li>
 *   <li><b>Post-pipeline validation</b> — row count and BnC against the record table.
 *       Results written to {@code bal_and_cntl_smry_tx}; checkpoint updated to COMPLETED / FAILED_BNC / FAILED.</li>
 * </ol>
 */
public final class DataSourcePipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourcePipelineFactory.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Held after assembly so Main can call runPostPipelineSteps()
    private List<SourceConfig>               processedConfigs;
    private Map<String, Long>                dataSourceIds;   // datasourceName → da_id
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

        // ── Step 1-2: Validate CLI args, then fetch source config from BQ ──
        validateRequiredParameters(options);
        BigQuerySourceConfigRepository bqRepo = new BigQuerySourceConfigRepository(options);
        List<SourceConfig> sourceConfigs = bqRepo.fetchSourceConfigs(
            options.getParentId(), options.getDatasourceName(),
            options.getSubprocessName(), options.getPeriodId());
        LOG.info("Found {} source config(s) for this run", sourceConfigs.size());

        // ── Classic Template detection ─────────────────────────────────────
        // In Classic Template creation mode (--templateLocation set), pipeline.run() only
        // serialises the graph — no workers run and waitUntilFinish() throws.
        // Skip checkpoint creation and filtering; daId comes from --daId at launch time.
        if (isTemplateCreationMode(options)) {
            LOG.info("Classic Template creation mode: skipping checkpoint creation. "
                   + "Airflow pre-setup task must create the LOADING row and pass --daId "
                   + "as a runtime parameter when launching the template.");
            processedConfigs = sourceConfigs;
            return assemblePipelineForTemplate(options, sourceConfigs);
        }

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
            LOG.info("DaRefer LOADING row created for '{}': da_id={}", config.datasourceName, dsId);
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
                sendFailureEmailIfConfigured(config, DataSourceCheckpoint.STA_FAILED, errorMsg, null);
            } else {
                try {
                    runValidationAndUpdateCheckpoint(config, dsId);
                } catch (Exception e) {
                    LOG.error("Post-pipeline validation failed for '{}' (DaId={}): {}",
                              config.datasourceName, dsId, e.getMessage(), e);
                    checkpointAdapter.updateStatus(dsId, DataSourceCheckpoint.STA_FAILED, null);
                    sendFailureEmailIfConfigured(config, DataSourceCheckpoint.STA_FAILED, e.getMessage(), null);
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
            LOG.info("Assembling source branch: {} ({}) → DaRec (da_id={})",
                     config.datasourceName, config.sourceType, dsId);

            SourceConfig resolved = resolveQueryTokens(config, options);
            Schema bqSchema = fetchBqSchema(config);
            PCollection<Row> sourceData = SourceRouter.routeFromConfig(
                pipeline, resolved, options, runDate, bqSchema);

            PCollection<Row> transformed = SourceTransformChainAssembler.assemble(
                sourceData, config, options, pipeline);

            transformed.apply("RecordSink-" + config.datasourceName,
                new DataSourceRecordSinkTransform(options,
                    ValueProvider.StaticValueProvider.of(dsId)));
        }

        return pipeline;
    }

    /**
     * Classic Template mode: daId is a runtime ValueProvider resolved from {@code --daId}
     * when the template is launched. The LOADING row must already exist (created by the
     * Airflow pre-setup task). All sources share the same daId (one template = one source).
     */
    private Pipeline assemblePipelineForTemplate(FrameworkOptions options, List<SourceConfig> configs) {
        Pipeline pipeline = Pipeline.create(options);
        LocalDate runDate = DateUtils.resolveRunDate(options);
        LOG.info("Template graph assembly: daId will be resolved from --daId at launch time");

        ValueProvider<Long> daIdProvider = options.getDaId();

        for (SourceConfig config : configs) {
            LOG.info("Assembling template source branch: {} ({})", config.datasourceName, config.sourceType);

            SourceConfig resolved = resolveQueryTokens(config, options);
            Schema bqSchema = fetchBqSchema(config);
            PCollection<Row> sourceData = SourceRouter.routeFromConfig(
                pipeline, resolved, options, runDate, bqSchema);

            PCollection<Row> transformed = SourceTransformChainAssembler.assemble(
                sourceData, config, options, pipeline);

            transformed.apply("RecordSink-" + config.datasourceName,
                new DataSourceRecordSinkTransform(options, daIdProvider));
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
            LOG.error("DaRec count query failed for '{}' (da_id={}) — marking FAILED",
                      config.datasourceName, dsId);
            checkpointAdapter.updateStatus(dsId, DataSourceCheckpoint.STA_FAILED,
                "{\"error\":\"record count query failed — see pipeline logs\"}");
            sendFailureEmailIfConfigured(config, DataSourceCheckpoint.STA_FAILED,
                "Record count query failed — see pipeline logs", null);
            return;
        }
        LOG.info("DaRec count for '{}' (da_id={}): {}", config.datasourceName, dsId, rowCount);

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
        if (!DataSourceCheckpoint.STA_COMPLETED.equals(staCd)) {
            sendFailureEmailIfConfigured(config, staCd, String.join("; ", failures), bncJson);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Runs BnC validation and final checkpoint update for a Classic Template pipeline whose
     * Dataflow job has already finished. Called by {@code Main} for
     * {@code --processType=POST_DOWNLOAD_VALIDATION} after the Airflow DataflowJobStateSensor
     * confirms the job completed.
     *
     * <p>Requires {@code --daId} to be set (the da_id created by the pre-setup task).
     * Reloads source configs from BQ to get the current ValidationConfig.
     */
    public void runPostPipelineValidation(FrameworkOptions options) {
        long daId = options.getDaId().get();
        if (daId <= 0) {
            throw new PipelineConfigurationException(
                "--daId must be set for POST_DOWNLOAD_VALIDATION (got: " + daId + ")");
        }

        checkpointAdapter = new BigQueryDataSourceCheckpointAdapter(options);
        recordAdapter     = new BigQueryDataSourceRecordAdapter(options);

        BigQuerySourceConfigRepository bqRepo = new BigQuerySourceConfigRepository(options);
        List<SourceConfig> sourceConfigs = bqRepo.fetchSourceConfigs(
            options.getParentId(), options.getDatasourceName(),
            options.getSubprocessName(), options.getPeriodId());

        LOG.info("POST_DOWNLOAD_VALIDATION | da_id={} | sources={}", daId, sourceConfigs.size());

        // All sources in this run share the same daId (one template = one source group)
        for (SourceConfig config : sourceConfigs) {
            try {
                runValidationAndUpdateCheckpoint(config, daId);
            } catch (Exception e) {
                LOG.error("Post-pipeline validation failed for '{}' (da_id={}): {}",
                          config.datasourceName, daId, e.getMessage(), e);
                checkpointAdapter.updateStatus(daId, DataSourceCheckpoint.STA_FAILED, null);
                sendFailureEmailIfConfigured(config, DataSourceCheckpoint.STA_FAILED, e.getMessage(), null);
            }
        }
    }

    private static boolean isTemplateCreationMode(FrameworkOptions options) {
        String loc = options.getTemplateLocation();
        return loc != null && !loc.isBlank();
    }

    /**
     * For BQ sources, resolves {periodStart}/{periodEnd}/{periodId}/{runDate} and custom
     * tokens in {@code bqFetchConfig.query} before the query reaches BigQueryIO.
     * Must run here in beam-runner (not in beam-io SourceRouter) because
     * QueryParameterResolver is in beam-utils and beam-io cannot depend on beam-utils.
     * Non-BQ sources are returned unchanged.
     */
    private static SourceConfig resolveQueryTokens(SourceConfig config, FrameworkOptions options) {
        if (config.sourceType != SourceType.BQ
                || config.bqFetchConfig == null
                || !config.bqFetchConfig.hasQuery()) {
            return config;
        }
        BqFetchConfig bq = config.bqFetchConfig;
        String resolvedQuery = QueryParameterResolver.resolve(bq.query, bq.queryParams, options);
        BqFetchConfig resolvedBq = new BqFetchConfig(
            bq.projectId, bq.dataset, bq.table, resolvedQuery, bq.queryParams);
        return SourceConfig.builder()
            .parentId(config.parentId)
            .datasourceName(config.datasourceName)
            .periodId(config.periodId)
            .subprocessName(config.subprocessName)
            .sourceType(config.sourceType)
            .bqFetchConfig(resolvedBq)
            .queryConfig(config.queryConfig)
            .sourceTransforms(new java.util.ArrayList<>(config.sourceTransforms))
            .validationConfig(config.validationConfig)
            .failureEmailConfig(config.failureEmailConfig)
            .build();
    }

    /**
     * Fetches the Beam Schema for a BQ table source at driver-JVM time.
     * Returns null for non-BQ sources, query-only sources (no static table), or when the
     * fetch fails (logs a warning and continues with the generic all-string fallback).
     *
     * <p>Must be called here in beam-runner because {@link BigQuerySchemaUtils} is in
     * beam-utils and beam-io cannot depend on beam-utils.
     */
    private static Schema fetchBqSchema(SourceConfig config) {
        if (config.sourceType != SourceType.BQ || config.bqFetchConfig == null) {
            return null;
        }
        BqFetchConfig bq = config.bqFetchConfig;
        // Query-only sources have no table to inspect; fall back to generic schema.
        if (bq.table == null || bq.table.isBlank()) {
            LOG.debug("BQ source '{}' is query-only — using generic schema fallback",
                      config.datasourceName);
            return null;
        }
        try {
            Schema schema = BigQuerySchemaUtils.fetchBeamSchema(bq.tableRef());
            LOG.info("Fetched typed schema ({} fields) for BQ source '{}'",
                     schema.getFieldCount(), config.datasourceName);
            return schema;
        } catch (Exception e) {
            LOG.warn("Could not fetch BQ schema for source '{}' ({}): {} — using generic fallback",
                     config.datasourceName, bq.tableRef(), e.getMessage());
            return null;
        }
    }

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

    private static void validateRequiredParameters(FrameworkOptions options) {
        if (options.getDatasourceName() == null || options.getDatasourceName().isBlank()) {
            throw new PipelineConfigurationException("--datasourceName is required for DATA_SOURCE_DOWNLOAD");
        }
        if (options.getPeriodId() <= 0) {
            throw new PipelineConfigurationException("--periodId is required for DATA_SOURCE_DOWNLOAD");
        }
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

    // ── Failure email ─────────────────────────────────────────────────────────

    private void sendFailureEmailIfConfigured(SourceConfig config, String staCd,
                                              String errorMessage, String bncSummary) {
        com.yourco.beam.model.SourceFailureEmailConfig emailConfig = config.failureEmailConfig;
        if (emailConfig == null || !emailConfig.isPresent()) return;
        try {
            String subject = resolveEmailTokens(emailConfig.subjectTemplate,
                config.datasourceName, config.periodId, staCd, errorMessage, bncSummary);
            String body = resolveEmailTokens(emailConfig.bodyTemplate,
                config.datasourceName, config.periodId, staCd, errorMessage, bncSummary);
            new SmtpReportEmailAdapter(
                emailConfig.smtpHost, emailConfig.smtpPort,
                emailConfig.smtpPasswordSecretId, emailConfig.fromAddress)
                .send(subject, body, emailConfig.to, emailConfig.cc, java.util.List.of());
            LOG.info("Failure email sent for '{}' (staCd={})", config.datasourceName, staCd);
        } catch (Exception e) {
            LOG.error("Failed to send failure email for '{}': {}", config.datasourceName, e.getMessage(), e);
        }
    }

    private static String resolveEmailTokens(String template, String datasourceName,
                                             int periodId, String staCd,
                                             String errorMessage, String bncSummary) {
        if (template == null) return "";
        return template
            .replace("{datasourceName}", datasourceName)
            .replace("{periodId}",       String.valueOf(periodId))
            .replace("{staCd}",          staCd        != null ? staCd        : "")
            .replace("{errorMessage}",   errorMessage != null ? errorMessage : "")
            .replace("{bncSummary}",     bncSummary   != null ? bncSummary   : "");
    }

    // ── Exception type ────────────────────────────────────────────────────────

    public static final class PipelineConfigurationException extends RuntimeException {
        public PipelineConfigurationException(String message) { super(message); }
    }
}
