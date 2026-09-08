package com.yourco.beam.runner;

import com.yourco.beam.exception.ReportProcessingException;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.checkpoint.BigQueryReportCheckpointAdapter;
import com.yourco.beam.io.checkpoint.DataSourceCheckpointAdapter;
import com.yourco.beam.io.checkpoint.ReportCheckpointAdapter;
import com.yourco.beam.io.email.EmailSendUtility;
import com.yourco.beam.io.report.BigQueryJobService;
import com.yourco.beam.model.EmailAttachment;
import com.yourco.beam.model.EmailParams;
import com.yourco.beam.model.ReportCheckpoint;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.ReportOutputConfig;
import com.yourco.beam.model.ReportPreprocessingStep;
import com.yourco.beam.model.ReportTransformStep;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.utils.QueryParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

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
 *   <li>Load {@link ReportConfig} from parameter_store</li>
 *   <li>Insert RptRefer row with {@code sta_cd=LOADING}</li>
 *   <li>Run preprocessing steps (BQ queries or API enrichment)</li>
 *   <li>Verify each required datasource has {@code sta_cd=COMPLETED} in DaRefer for this period</li>
 *   <li>Build alias registry: datasource alias → RptStageDa subquery (after staging DaRec rows)</li>
 *   <li>Run transformation chain (BQ jobs, each materialised to a BQ table)</li>
 *   <li>Write final result to per-report BQ table ({@code output_bq_table} from config, if set)</li>
 *   <li>Route each output via {@link ReportOutputSinkRouter} (GCS / BQ / API)</li>
 *   <li>Write one RptOutput row per output step</li>
 *   <li>Clear staged data from RptStageDa</li>
 *   <li>Send email — GCS outputs as file attachments; BQ/API outputs noted in body</li>
 *   <li>Update RptRefer to {@code sta_cd=COMPLETED} or {@code FAILED}</li>
 * </ol>
 */
public final class ReportPipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ReportPipelineFactory.class);

    private final BigQueryJobService     bqJobService;
    private final ReportOutputSinkRouter sinkRouter;
    private final EmailSendUtility       emailUtility;

    public ReportPipelineFactory() {
        this(new BigQueryJobService());
    }

    ReportPipelineFactory(BigQueryJobService bqJobService) {
        this(bqJobService, new ReportOutputSinkRouter(bqJobService), discoverEmailUtility());
    }

    ReportPipelineFactory(BigQueryJobService bqJobService, ReportOutputSinkRouter sinkRouter) {
        this(bqJobService, sinkRouter, discoverEmailUtility());
    }

    /**
     * @param emailUtility the {@link EmailSendUtility} to send report-completion email with.
     *                     Pass explicitly to inject an implementation that isn't discoverable via
     *                     SPI (e.g. in a test, or a runtime not using the fat-jar's
     *                     {@code META-INF/services} merge). May be {@code null} — {@link #execute}
     *                     then logs a warning and skips sending rather than failing the report.
     */
    public ReportPipelineFactory(BigQueryJobService bqJobService, ReportOutputSinkRouter sinkRouter,
                                 EmailSendUtility emailUtility) {
        this.bqJobService = bqJobService;
        this.sinkRouter   = sinkRouter;
        this.emailUtility = emailUtility;
    }

    /**
     * Discovers an {@link EmailSendUtility} implementation via Java SPI — a JAR on the classpath
     * declaring one in {@code META-INF/services/com.yourco.beam.io.email.EmailSendUtility}, the
     * same mechanism {@code TransformRegistry} uses for {@code BeamTransform}. This repository
     * ships no implementation of its own, so this returns {@code null} unless the deployment's
     * classpath (e.g. an organization's own separately-built JAR, merged into the fat jar by
     * {@code maven-shade-plugin}'s {@code ServicesResourceTransformer}) provides one.
     */
    private static EmailSendUtility discoverEmailUtility() {
        Iterator<EmailSendUtility> found = ServiceLoader.load(EmailSendUtility.class).iterator();
        return found.hasNext() ? found.next() : null;
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    /**
     * Executes the full report run for the given options.
     *
     * @throws ReportProcessingException if any phase fails (status is marked FAILED before
     *         throwing). {@link ReportProcessingException#reason} identifies which phase — the
     *         {@code currentReason} local below is updated right before each phase runs, so the
     *         catch block always wraps with the reason matching where the failure actually
     *         occurred, not a generic catch-all.
     */
    public void execute(FrameworkOptions options) {
        String reportName       = options.getReportName();
        String reportSubprocess = options.getReportSubprocess();
        int    periodId         = options.getPeriodId();

        LOG.info("REPORT_PROCESSING | report={} subprocess={} period={}",
                 reportName, reportSubprocess, periodId);

        // ── 1. Load config from BigQuery ──────────────────────────────────────
        BigQueryReportRepository repo = new BigQueryReportRepository(options);
        ReportConfig config;
        try {
            config = repo.fetchReportConfig(reportName, reportSubprocess, periodId);
        } catch (Exception e) {
            throw ReportProcessingException.wrap(ReportProcessingException.Reason.CONFIG_NOT_FOUND,
                reportName, reportSubprocess, periodId, e);
        }

        // ── 2. RptRefer: LOADING ──────────────────────────────────────────────
        ReportCheckpointAdapter      reportAdapter = new BigQueryReportCheckpointAdapter(options);
        DataSourceCheckpointAdapter  dsAdapter     = new BigQueryDataSourceCheckpointAdapter(options);
        long rptId = reportAdapter.createCheckpoint(reportName, periodId, reportName);
        LOG.info("REPORT_PROCESSING RptRefer LOADING row created: rpt_id={}", rptId);

        int outputCount = 0;
        ReportProcessingException.Reason currentReason = ReportProcessingException.Reason.UNKNOWN;
        try {
            // ── 3. Preprocessing ──────────────────────────────────────────────
            currentReason = ReportProcessingException.Reason.PREPROCESSING_FAILURE;
            if (config.hasPreprocessing()) {
                runPreprocessing(config, options);
            }

            // ── 4. Datasource availability check ──────────────────────────────
            currentReason = ReportProcessingException.Reason.DATASOURCE_UNAVAILABLE;
            checkDatasourceAvailability(config, dsAdapter);

            // ── 5. Build alias registry (stage DaRec rows into RptStageDa) ───
            currentReason = ReportProcessingException.Reason.STAGING_FAILURE;
            Map<String, String> aliasRegistry = buildAliasRegistry(config, rptId, dsAdapter, reportAdapter);

            // ── 6. Transformation chain ───────────────────────────────────────
            currentReason = ReportProcessingException.Reason.TRANSFORM_FAILURE;
            if (config.hasTransforms()) {
                runTransformChain(config, options, aliasRegistry);
            }

            // ── 6b. Write final result to per-report BQ table ─────────────────
            currentReason = ReportProcessingException.Reason.OUTPUT_FAILURE;
            if (config.hasOutputBqTable()) {
                writeOutputBqTable(config, aliasRegistry);
            }

            // ── 7. Route outputs to sinks (GCS / BQ / API) ───────────────────
            List<ReportOutputSinkRouter.OutputResult> outputResults =
                exportOutputs(config, options, aliasRegistry);
            outputCount = outputResults.size();

            // ── 8. Write RptOutput rows ───────────────────────────────────────
            for (ReportOutputSinkRouter.OutputResult r : outputResults) {
                String outptCd  = r.fileName() != null ? r.fileName() : r.destination();
                String outputDs = r.destination();
                reportAdapter.writeOutput(rptId, outptCd, outputDs, null, null, 0.0, null);
            }

            // ── 9. Clear staged data ──────────────────────────────────────────
            reportAdapter.clearStagedData(rptId);
            LOG.info("Staged data cleared for rpt_id={}", rptId);

            // ── 10. Send email (GCS outputs only, as attachments) ─────────────
            currentReason = ReportProcessingException.Reason.EMAIL_FAILURE;
            if (config.hasEmail()) {
                List<ExportedFile> attachments = outputResults.stream()
                    .filter(ReportOutputSinkRouter.OutputResult::hasAttachment)
                    .map(r -> new ExportedFile(r.destination(), r.fileName(), r.contentType()))
                    .toList();
                sendEmail(config, options, attachments);
            }

            // ── 11. RptRefer: COMPLETED ───────────────────────────────────────
            reportAdapter.updateStatus(rptId, ReportCheckpoint.STA_COMPLETED);
            LOG.info("REPORT_PROCESSING completed: {} output(s) written", outputCount);

        } catch (ReportProcessingException e) {
            LOG.error("REPORT_PROCESSING failed ({}): {}", e.reason, e.getMessage(), e);
            reportAdapter.updateStatus(rptId, ReportCheckpoint.STA_FAILED);
            throw e;
        } catch (Exception e) {
            LOG.error("REPORT_PROCESSING failed during phase '{}': {}", currentReason, e.getMessage(), e);
            reportAdapter.updateStatus(rptId, ReportCheckpoint.STA_FAILED);
            throw ReportProcessingException.wrap(currentReason, reportName, reportSubprocess, periodId, e);
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

    private void checkDatasourceAvailability(ReportConfig config,
                                              DataSourceCheckpointAdapter dsAdapter) {
        LOG.info("Checking availability of {} datasource(s)", config.datasources.size());
        List<String> missing = new ArrayList<>();
        for (ReportDatasourceRef ref : config.datasources) {
            if (!ref.required) continue;
            boolean completed = dsAdapter.isCompleted(ref.datasourceName, config.periodId);
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

    /**
     * Builds the alias registry by:
     * 1. Resolving the latest completed da_id for each datasource ref
     * 2. Creating a RptDaMap row linking rpt_id to da_id
     * 3. Staging DaRec rows for that da_id into RptStageDa
     * 4. Registering the alias as a RptStageDa subquery
     */
    private Map<String, String> buildAliasRegistry(ReportConfig config, long rptId,
                                                    DataSourceCheckpointAdapter dsAdapter,
                                                    ReportCheckpointAdapter reportAdapter) {
        Map<String, String> registry = new LinkedHashMap<>();

        for (ReportDatasourceRef ref : config.datasources) {
            long daId;
            try {
                daId = dsAdapter.fetchLatestCompletedDaId(ref.datasourceName, config.periodId);
            } catch (IllegalArgumentException e) {
                if (ref.required) {
                    throw e;  // required datasource must be present
                }
                LOG.warn("Optional datasource '{}' has no COMPLETED DaRefer row for period={} "
                         + "— alias '{}' will not be registered",
                         ref.datasourceName, config.periodId, ref.transformAlias);
                continue;
            }

            long mapId = reportAdapter.addDaMapping(rptId, daId);
            reportAdapter.stageFromDaRec(mapId, daId);
            String subquery = reportAdapter.stagedDataSubquery(mapId);
            registry.put(ref.transformAlias, subquery);
            LOG.info("Alias '{}' → RptStageDa subquery (da_id={} map_id={})",
                     ref.transformAlias, daId, mapId);
        }
        return registry;
    }

    private void runTransformChain(ReportConfig config, FrameworkOptions options,
                                   Map<String, String> aliasRegistry) {
        LOG.info("Running {} transformation step(s)", config.transformSteps.size());
        for (ReportTransformStep step : config.transformSteps) {
            LOG.info("Transform step {}: '{}' → alias '{}'",
                     step.stepOrder, step.stepName, step.outputAlias);

            // Alias tokens first ({trades} → staged subquery or `project.dataset.table`),
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
            // If the alias points to a subquery (staged data), materialise it first
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

    /**
     * Writes the final report result to the per-report BQ table declared in
     * {@link ReportConfig#outputBqTable}.
     *
     * <p>Source alias resolution order:
     * <ol>
     *   <li>{@link ReportConfig#outputBqInputAlias} if set and non-blank</li>
     *   <li>Last transform step's {@code outputAlias} if transforms exist</li>
     *   <li>First datasource's {@code transformAlias} if no transforms exist</li>
     * </ol>
     */
    private void writeOutputBqTable(ReportConfig config, Map<String, String> aliasRegistry) {
        String sourceAlias = config.outputBqInputAlias;
        if (sourceAlias == null || sourceAlias.isBlank()) {
            if (!config.transformSteps.isEmpty()) {
                sourceAlias = config.transformSteps.get(config.transformSteps.size() - 1).outputAlias;
            } else if (!config.datasources.isEmpty()) {
                sourceAlias = config.datasources.get(0).transformAlias;
            }
        }
        if (sourceAlias == null) {
            throw new IllegalStateException(
                "Cannot determine source alias for outputBqTable=" + config.outputBqTable
                + " — set output_bq_input_alias in parameters_val_json");
        }
        String sourceRef = aliasRegistry.get(sourceAlias);
        if (sourceRef == null) {
            throw new IllegalArgumentException(
                "output_bq_input_alias '" + sourceAlias + "' not found in alias registry. "
                + "Available: " + aliasRegistry.keySet());
        }
        String fromClause = sourceRef.startsWith("(") ? sourceRef : "`" + sourceRef + "`";
        String sql = "SELECT * FROM " + fromClause;
        LOG.info("Writing final report: alias='{}' → {}", sourceAlias, config.outputBqTable);
        bqJobService.runQueryToTable(sql, config.outputBqTable);
        LOG.info("Final report written to {}", config.outputBqTable);
    }

    private void sendEmail(ReportConfig config, FrameworkOptions options,
                           List<ExportedFile> exportedFiles) {
        if (emailUtility == null) {
            LOG.warn("No EmailSendUtility available (none injected, none discovered via SPI) — "
                     + "skipping report-completion email for report={}", config.reportName);
            return;
        }

        String subject = resolveEmailTokens(config.emailConfig.subjectTemplate, config, options);
        String body    = resolveEmailTokens(config.emailConfig.bodyTemplate,    config, options);

        List<EmailAttachment> attachments = new ArrayList<>();
        for (ExportedFile file : exportedFiles) {
            attachments.add(new EmailAttachment(
                file.fileName(), emailUtility.FetchFileFromGcs(file.gcsUri()), file.contentType()));
        }

        EmailParams emailParams = emailUtility.SetEmailParams(
            config.emailConfig.fromAddress, subject,
            config.emailConfig.toList, config.emailConfig.ccList,
            config.emailConfig.encrypted);
        emailUtility.CreateEmailRequest(emailParams, body, attachments);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Replaces {@code {alias}} tokens in a query template.
     *
     * <p>Values that start with {@code (} are subqueries — inserted as-is (no backtick-wrapping).
     * All other values are treated as BQ table refs and wrapped with backticks.
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

    private static String resolveEmailTokens(String template, ReportConfig config,
                                              FrameworkOptions options) {
        if (template == null) return "";
        return template
            .replace("{reportName}",       config.reportName)
            .replace("{reportSubprocess}", config.reportSubprocess)
            .replace("{periodId}",         String.valueOf(config.periodId))
            .replace("{periodStart}",      nvl(options.getPeriodStart()))
            .replace("{periodEnd}",        nvl(options.getPeriodEnd()))
            .replace("{runDate}",          nvl(options.getRunDate()));
    }

    private static String nvl(String v) { return v != null ? v : ""; }

    // ── Inner types ───────────────────────────────────────────────────────────

    private record ExportedFile(String gcsUri, String fileName, String contentType) {}
}
