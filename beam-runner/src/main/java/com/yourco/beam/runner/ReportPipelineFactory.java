package com.yourco.beam.runner;

import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.checkpoint.DataSourceCheckpointAdapter;
import com.yourco.beam.io.report.BigQueryCommonReportDetailAdapter;
import com.yourco.beam.io.email.EmailAttachment;
import com.yourco.beam.io.email.ReportEmailAdapter;
import com.yourco.beam.io.report.BigQueryJobService;
import com.yourco.beam.model.DataSourceCheckpoint;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.ReportOutputConfig;
import com.yourco.beam.model.ReportPreprocessingStep;
import com.yourco.beam.model.ReportTransformStep;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.utils.DateUtils;
import com.yourco.beam.utils.GcsUtils;
import com.yourco.beam.utils.QueryParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Orchestrates the full REPORT_PROCESSING lifecycle in the driver JVM.
 *
 * <p>Unlike {@link DataSourcePipelineFactory} (which submits a Beam pipeline),
 * this factory executes entirely in the driver JVM using:
 * <ul>
 *   <li>BigQuery Jobs API — for transformation queries and GCS exports</li>
 *   <li>GCS client — to download exported files for email attachment</li>
 *   <li>Jakarta Mail — to send the report email</li>
 * </ul>
 * This makes reports fast to start (no Dataflow cluster spin-up) and suitable
 * for aggregated output tables that are too small to need distributed processing.
 *
 * <h2>Execution phases</h2>
 * <ol>
 *   <li>Load {@link ReportConfig} from parameter DB</li>
 *   <li>Write {@code PENDING} status to {@code process_status}</li>
 *   <li>Run preprocessing steps (BQ queries or API enrichment)</li>
 *   <li>Verify each required datasource has {@code COMPLETED} status for this period</li>
 *   <li>Build alias registry: alias → BQ output table ref of its data</li>
 *   <li>Run transformation chain (BQ jobs, each materialised to a BQ table)</li>
 *   <li>Export each output to GCS (BQ extract job)</li>
 *   <li>Download GCS files and send email with attachments</li>
 *   <li>Write {@code COMPLETED} or {@code FAILED} status</li>
 * </ol>
 */
public final class ReportPipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ReportPipelineFactory.class);
    private static final String PROCESS_TYPE = "REPORT_PROCESSING";

    private final BigQueryJobService     bqJobService;
    private final ReportOutputSinkRouter sinkRouter;

    public ReportPipelineFactory() {
        this(new BigQueryJobService());
    }

    ReportPipelineFactory(BigQueryJobService bqJobService) {
        this.bqJobService = bqJobService;
        this.sinkRouter   = new ReportOutputSinkRouter(bqJobService);
    }

    ReportPipelineFactory(BigQueryJobService bqJobService, ReportOutputSinkRouter sinkRouter) {
        this.bqJobService = bqJobService;
        this.sinkRouter   = sinkRouter;
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    /**
     * Executes the full report run for the given options.
     *
     * @throws RuntimeException if any phase fails (status is marked FAILED before throwing)
     */
    public void execute(FrameworkOptions options) {
        String reportName      = options.getReportName();
        String reportSubprocess = options.getReportSubprocess();
        String periodId         = options.getPeriodId();

        LOG.info("REPORT_PROCESSING | report={} subprocess={} period={}",
                 reportName, reportSubprocess, periodId);

        // ── 1. Load config from BigQuery ──────────────────────────────────────
        BigQueryReportRepository repo = new BigQueryReportRepository(options);
        ReportConfig config = repo.fetchReportConfig(reportName, reportSubprocess, periodId);

        // ── 2. DaRefer: LOADING ───────────────────────────────────────────────
        DataSourceCheckpointAdapter checkpointAdapter = new BigQueryDataSourceCheckpointAdapter(options);
        BigQueryCommonReportDetailAdapter cmnRptAdapter = new BigQueryCommonReportDetailAdapter(options);
        long dsId = checkpointAdapter.createCheckpoint(reportName, periodId, reportName);
        LOG.info("REPORT_PROCESSING DaRefer LOADING row created: DaId={}", dsId);

        int outputCount = 0;
        try {
            // ── 3. Preprocessing ──────────────────────────────────────────────
            if (config.hasPreprocessing()) {
                runPreprocessing(config, options);
            }

            // ── 4. Datasource availability check ──────────────────────────────
            checkDatasourceAvailability(config, options, checkpointAdapter);

            // ── 5. Build alias registry ───────────────────────────────────────
            Map<String, String> aliasRegistry = buildAliasRegistry(config, options);

            // ── 6. Transformation chain ───────────────────────────────────────
            if (config.hasTransforms()) {
                runTransformChain(config, options, aliasRegistry);
            }

            // ── 7. Route outputs to sinks (GCS / BQ / API) ───────────────────
            List<ReportOutputSinkRouter.OutputResult> outputResults =
                exportOutputs(config, options, aliasRegistry);
            outputCount = outputResults.size();

            // ── 8. Write to COM_CmnRptDtl ─────────────────────────────────────
            for (ReportOutputSinkRouter.OutputResult r : outputResults) {
                String flNm = r.fileName() != null ? r.fileName() : r.destination();
                cmnRptAdapter.insertDetail(reportName, flNm, null,
                                           r.rowCount(), options.getJobRunId());
            }

            // ── 9. Send email (GCS outputs only, as attachments) ──────────────
            if (config.hasEmail()) {
                List<ExportedFile> attachments = outputResults.stream()
                    .filter(ReportOutputSinkRouter.OutputResult::hasAttachment)
                    .map(r -> new ExportedFile(r.destination(), r.fileName(), r.contentType()))
                    .toList();
                sendEmail(config, options, attachments);
            }

            // ── 10. DaRefer: COMPLETED ────────────────────────────────────────
            checkpointAdapter.updateStatus(dsId, DataSourceCheckpoint.STA_COMPLETED, null);
            LOG.info("REPORT_PROCESSING completed: {} files exported", outputCount);

        } catch (Exception e) {
            LOG.error("REPORT_PROCESSING failed: {}", e.getMessage(), e);
            checkpointAdapter.updateStatus(dsId, DataSourceCheckpoint.STA_FAILED, null);
            throw new RuntimeException("REPORT_PROCESSING failed for report=" + reportName, e);
        }
    }

    // ── Phase implementations ─────────────────────────────────────────────────

    private void runPreprocessing(ReportConfig config, FrameworkOptions options) {
        LOG.info("Running {} preprocessing step(s)", config.preprocessingSteps.size());
        for (ReportPreprocessingStep step : config.preprocessingSteps) {
            LOG.info("Preprocessing step {}: type={} name={}",
                     step.stepOrder, step.stepType, step.stepName);
            switch (step.stepType) {
                case ReportPreprocessingStep.BQ_QUERY -> {
                    String sql = QueryParameterResolver.resolve(
                            step.bqQuery, step.queryParams, options);
                    if (step.bqOutputTable != null && !step.bqOutputTable.isBlank()) {
                        bqJobService.runQueryToTable(sql, step.bqOutputTable);
                    } else {
                        bqJobService.runQuery(sql);
                    }
                }
                case ReportPreprocessingStep.API_ENRICHMENT ->
                    LOG.warn("API_ENRICHMENT preprocessing step is not yet implemented: {}",
                             step.stepName);
                default ->
                    throw new IllegalArgumentException(
                        "Unknown preprocessing step type: " + step.stepType);
            }
        }
    }

    private void checkDatasourceAvailability(ReportConfig config, FrameworkOptions options,
                                              DataSourceCheckpointAdapter checkpointAdapter) {
        LOG.info("Checking availability of {} datasource(s)", config.datasources.size());
        List<String> missing = new ArrayList<>();
        for (ReportDatasourceRef ref : config.datasources) {
            if (!ref.required) continue;
            boolean completed = checkpointAdapter.isCompleted(ref.datasourceName, config.periodId);
            if (!completed) {
                missing.add(ref.datasourceName + "/" + ref.datasourceSubprocess
                            + " (not COMPLETED for period=" + config.periodId + ")");
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                "Required datasource(s) not yet COMPLETED for period="
                + config.periodId + ": " + missing);
        }
        LOG.info("All required datasources are available");
    }

    private Map<String, String> buildAliasRegistry(ReportConfig config,
                                                    FrameworkOptions options) {
        Map<String, String> registry = new LinkedHashMap<>();
        BigQueryReportRepository bqRepo = new BigQueryReportRepository(options);
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        String recordTable = project + "." + options.getCheckpointBqDataset()
                           + "." + options.getDaRecTable();

        for (ReportDatasourceRef ref : config.datasources) {
            // Resolve the DaId of the most recent COMPLETED run for this datasource.
            // The alias expands to a DaRec subquery; transform SQL uses JSON_VALUE to
            // extract individual columns: JSON_VALUE({alias}.RowDaJsonTx, '$.fieldName').
            long daDatId;
            try {
                daDatId = bqRepo.fetchDatasourceDaId(
                    ref.datasourceName, config.periodId, options);
            } catch (IllegalArgumentException e) {
                if (ref.required) {
                    throw e;  // required datasource must be present — re-throw
                }
                LOG.warn("Optional datasource '{}' has no COMPLETED DaRefer row for period={} "
                         + "— alias '{}' will not be registered",
                         ref.datasourceName, config.periodId, ref.transformAlias);
                continue;
            }
            String subquery = "(SELECT RowDaJsonTx FROM `" + recordTable
                            + "` WHERE DaId = " + daDatId + ")";
            registry.put(ref.transformAlias, subquery);
            LOG.info("Alias '{}' → DaRec subquery (DaId={})", ref.transformAlias, daDatId);
        }
        return registry;
    }

    private void runTransformChain(ReportConfig config, FrameworkOptions options,
                                   Map<String, String> aliasRegistry) {
        LOG.info("Running {} transformation step(s)", config.transformSteps.size());
        for (ReportTransformStep step : config.transformSteps) {
            LOG.info("Transform step {}: '{}' → alias '{}'",
                     step.stepOrder, step.stepName, step.outputAlias);

            // Alias tokens first ({trades} → `project.dataset.table`),
            // then standard + custom params ({periodStart}, {exchange}, etc.)
            String sql = resolveAliasTokens(step.queryTemplate, aliasRegistry);
            sql = QueryParameterResolver.resolve(sql, step.queryParams, options);

            bqJobService.runQueryToTable(sql, step.outputBqTable);
            aliasRegistry.put(step.outputAlias, step.outputBqTable);
            LOG.info("Step '{}' result registered as alias '{}' → {}",
                     step.stepName, step.outputAlias, step.outputBqTable);
        }
    }

    private List<ReportOutputSinkRouter.OutputResult> exportOutputs(
            ReportConfig config, FrameworkOptions options,
            Map<String, String> aliasRegistry) {
        List<ReportOutputSinkRouter.OutputResult> result = new ArrayList<>();

        for (ReportOutputConfig output : config.outputConfigs) {
            String sourceTable = aliasRegistry.get(output.inputAlias);
            if (sourceTable == null) {
                throw new IllegalArgumentException(
                    "Output alias '" + output.inputAlias + "' not found in alias registry. "
                    + "Available: " + aliasRegistry.keySet());
            }
            // If the alias points to a record-table subquery, materialise it first
            if (sourceTable.startsWith("(")) {
                String tempTable = options.getProject() + "."
                    + options.getCheckpointBqDataset()
                    + ".tmp_" + config.reportName + "_" + output.inputAlias;
                bqJobService.runQueryToTable("SELECT * FROM " + sourceTable, tempTable);
                sourceTable = tempTable;
            }

            LOG.info("Output {}: sinkType={} alias='{}' source={}",
                     output.outputOrder, output.sinkType, output.inputAlias, sourceTable);

            ReportOutputSinkRouter.OutputResult outputResult =
                sinkRouter.route(output, sourceTable, config, options);
            result.add(outputResult);

            LOG.info("Output {} done → {}", output.outputOrder, outputResult.destination());
        }
        return result;
    }

    private void sendEmail(ReportConfig config, FrameworkOptions options,
                           List<ExportedFile> exportedFiles) {
        String subject = resolveEmailTokens(config.emailConfig.subjectTemplate, config, options);
        String body    = resolveEmailTokens(config.emailConfig.bodyTemplate,    config, options);

        List<EmailAttachment> attachments = new ArrayList<>();
        for (ExportedFile file : exportedFiles) {
            byte[] bytes = GcsUtils.readBytes(file.gcsUri);
            attachments.add(new EmailAttachment(
                new ByteArrayInputStream(bytes), file.fileName, file.contentType));
        }

        ReportEmailAdapter emailAdapter = new SmtpReportEmailAdapter(options);
        emailAdapter.send(subject, body,
                          config.emailConfig.toList,
                          config.emailConfig.ccList,
                          attachments);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Replaces {@code {alias}} tokens in a query template.
     *
     * <p>Values that start with {@code (} are subqueries — inserted as-is (no backtick-wrapping).
     * All other values are treated as BQ table refs and wrapped with backticks.
     *
     * <p>For datasource aliases backed by the record table, the subquery form is:
     * {@code (SELECT RowDaJsonTx FROM `DaRec` WHERE DaId = X)}.
     * Transform SQL should use {@code JSON_VALUE({alias}.RowDaJsonTx, '$.fieldName')}
     * to extract individual columns.
     */
    private static String resolveAliasTokens(String template,
                                              Map<String, String> aliasRegistry) {
        String result = template;
        for (Map.Entry<String, String> entry : aliasRegistry.entrySet()) {
            String ref = entry.getValue();
            String expanded = ref.startsWith("(") ? ref : "`" + ref + "`";
            result = result.replace("{" + entry.getKey() + "}", expanded);
        }
        return result;
    }

    /**
     * Replaces email template tokens with actual values.
     */
    private static String resolveEmailTokens(String template, ReportConfig config,
                                              FrameworkOptions options) {
        if (template == null) return "";
        return template
            .replace("{reportName}",       config.reportName)
            .replace("{reportSubprocess}", config.reportSubprocess)
            .replace("{periodId}",         config.periodId)
            .replace("{periodStart}",      nvl(options.getPeriodStart()))
            .replace("{periodEnd}",        nvl(options.getPeriodEnd()))
            .replace("{runDate}",          nvl(options.getRunDate()));
    }

    private static String nvl(String v) { return v != null ? v : ""; }

    // ── Inner types ───────────────────────────────────────────────────────────

    private record ExportedFile(String gcsUri, String fileName, String contentType) {}
}
