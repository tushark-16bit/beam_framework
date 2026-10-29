package com.yourco.beam.runner.report;

import com.yourco.beam.exception.ReportProcessingException;
import com.yourco.beam.io.bq.BigQueryJobService;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.checkpoint.BigQueryReportCheckpointAdapter;
import com.yourco.beam.io.checkpoint.DataSourceCheckpointAdapter;
import com.yourco.beam.io.checkpoint.ReportCheckpointAdapter;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.io.email.EmailSendUtility;
import com.yourco.beam.model.checkpoint.ReportCheckpoint;
import com.yourco.beam.model.email.EmailAttachment;
import com.yourco.beam.model.email.EmailParams;
import com.yourco.beam.model.report.ReportConfig;
import com.yourco.beam.model.report.ReportDataPlan;
import com.yourco.beam.model.report.ReportDatasourceRef;
import com.yourco.beam.model.report.ReportOutputConfig;
import com.yourco.beam.model.report.ReportPreprocessingStep;
import com.yourco.beam.model.report.ReportTransformStep;
import com.yourco.beam.model.schedule.RunDates;
import com.yourco.beam.model.schedule.RunScheduleConfig;
import com.yourco.beam.model.source.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.runner.datasource.DataSourcePipelineFactory;
import com.yourco.beam.runner.notification.EmailSendUtilities;
import com.yourco.beam.utils.date.BigQueryBusinessCalendarProvider;
import com.yourco.beam.utils.date.BusinessCalendar;
import com.yourco.beam.utils.date.BusinessCalendarProvider;
import com.yourco.beam.utils.date.RunDateCalculator;
import com.yourco.beam.utils.query.JsonRowFlattener;
import com.yourco.beam.utils.query.QueryParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>Load {@link ReportConfig} from parameter_store</li>
 *   <li>Evaluate the report's own schedule via {@code RunDateCalculator.evaluateReport()}
 *       (Finance Automation rules): not eligible today → stop, no RptRefer row; eligible → its
 *       {@link RunDates} (the reporting period) are what every later phase reads. Scheduled
 *       reports already COMPLETED for that period are skipped unless {@code --manualOverrun}.</li>
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
        this(bqJobService, new ReportOutputSinkRouter(bqJobService), EmailSendUtilities.create());
    }

    ReportPipelineFactory(BigQueryJobService bqJobService, ReportOutputSinkRouter sinkRouter) {
        this(bqJobService, sinkRouter, EmailSendUtilities.create());
    }

    /**
     * @param emailUtility the {@link EmailSendUtility} to send report-completion email with.
     *                     Pass explicitly to inject an implementation other than
     *                     {@code EmailSendUtilities.create()}'s (e.g. in a test). May be {@code null} — {@link #execute}
     *                     then logs a warning and skips sending rather than failing the report.
     */
    public ReportPipelineFactory(BigQueryJobService bqJobService, ReportOutputSinkRouter sinkRouter,
                                 EmailSendUtility emailUtility) {
        this.bqJobService = bqJobService;
        this.sinkRouter   = sinkRouter;
        this.emailUtility = emailUtility;
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
        // Driver-JVM only — BigQueryReportRepository must never run inside a DoFn (CLAUDE.md §12).
        BigQueryReportRepository repo = new BigQueryReportRepository(options);
        ReportConfig config;
        try {
            config = repo.fetchReportConfig(reportName, reportSubprocess, periodId);
        } catch (Exception e) {
            throw ReportProcessingException.wrap(ReportProcessingException.Reason.CONFIG_NOT_FOUND,
                reportName, reportSubprocess, periodId, e);
        }

        // ── 1b. Finance Automation scheduling: may the report run today, for which period? ──
        // Reports are scheduled independently of their data sources (BAU): the report's own
        // freqDtl/maxFreqRunDay window decides WHEN, its own frequency + dayLag decides WHICH
        // period. Every date the report uses afterwards (RptRefer per_id, DaRefer lookups, query
        // tokens, output file names, email tokens) comes from decision.dates. A report with no
        // run_details schedule or calendar is NOT_EVALUABLE and is not processed (owner decision D5).
        RunDateCalculator.ScheduleDecision decision;
        try {
            decision = RunDateCalculator.evaluateReport(config.runScheduleConfig, options);
        } catch (Exception e) {
            // e.g. an invalid --runDate / --periodStart / --businessTimeZone
            throw ReportProcessingException.wrap(ReportProcessingException.Reason.UNKNOWN,
                reportName, reportSubprocess, periodId, e);
        }
        if (decision.status == RunDateCalculator.ScheduleDecision.Status.NOT_EVALUABLE) {
            // Configuration/calendar problem — surfaced as a failure (non-zero exit + failure
            // notification via Main); BAU re-evaluates it on the next execution.
            throw new ReportProcessingException(ReportProcessingException.Reason.UNKNOWN,
                reportName, reportSubprocess, periodId,
                "Run schedule for report '" + reportName + "' could not be evaluated: "
                + decision.detail, null);
        }
        if (!decision.shouldRun()) {
            // NOT_YET_ELIGIBLE / EXPIRED / NON_BUSINESS_DAY — an expected skip, not a failure:
            // no RptRefer row, exit 0, re-evaluated on the next execution.
            LOG.info("Report '{}' not run today: {}", reportName, decision);
            return;
        }
        RunDates dates = decision.dates;
        LOG.info("Report '{}' eligible: {}", reportName, decision);

        // ── 1c. Already COMPLETED for this period? (BAU: item + period Completed → skip) ──
        if (isAlreadyCompleted(options, decision, reportName)) {
            return;
        }

        execute(options, config, dates, resolveDataPlan(options, config, dates));
    }

    /**
     * Works out, per data source, which periods the report must find {@code COMPLETED} and which
     * of them it reads — a {@link ReportDataPlan}. Needs each data source's {@code run_details_json}
     * schedule (and, for a DAILY source, its calendar), so it runs in the <b>driver JVM</b> and the
     * result is passed on to {@link #execute(FrameworkOptions, ReportConfig, RunDates, ReportDataPlan)}
     * / carried to the worker — {@code BigQuerySourceConfigRepository} must never run in a DoFn
     * (CLAUDE.md §12).
     * <ul>
     *   <li>Data source with a lookback ({@code lookback_from}/{@code lookback_to}): validate the
     *       whole lookback range ({@link RunDateCalculator#lookbackPeriodIds}). Same frequency as
     *       the report: read only the report's own period. <b>Finer</b> data source (D7): the range
     *       is stepped in the data source's own frequency from the last period inside the report
     *       period, and every period inside the report period is validated and read too.</li>
     *   <li>Otherwise, from the data source's own frequency
     *       ({@link RunDateCalculator#sourcePeriodIdsWithin}, owner decision D7): equal to the
     *       report's → just the report's period; <b>finer</b> (daily sources, monthly report) →
     *       every data source period inside the report period, all validated and all read;
     *       <b>coarser</b> → the report fails.</li>
     *   <li>A data source whose config can't be loaded or has no schedule keeps the report's own
     *       period (the pre-existing behaviour) — unless it has a lookback, which needs both.</li>
     * </ul>
     *
     * @throws ReportProcessingException ({@code DATASOURCE_UNAVAILABLE}) if a data source is
     *         coarser than the report, has a lookback but no schedule, or its calendar can't be
     *         loaded — never computed on a guess
     */
    public static ReportDataPlan resolveDataPlan(FrameworkOptions options, ReportConfig config, RunDates dates) {
        ReportDataPlan plan = ReportDataPlan.empty();
        BigQuerySourceConfigRepository sourceRepo = null;
        BusinessCalendarProvider calendars = null;
        for (ReportDatasourceRef ref : config.datasources) {
            RunScheduleConfig dsSchedule = null;
            try {
                if (sourceRepo == null) {
                    sourceRepo = new BigQuerySourceConfigRepository(options);
                    calendars  = new BigQueryBusinessCalendarProvider(options);
                }
                List<SourceConfig> found = sourceRepo.fetchSourceConfigs(options.getParentId(),
                    ref.datasourceName, ref.datasourceSubprocess, dates.periodId);
                if (found.isEmpty()) {
                    throw new IllegalArgumentException("no source config found");
                }
                dsSchedule = found.get(0).runScheduleConfig;
            } catch (Exception e) {
                if (ref.hasLookback()) {
                    throw planFailure(config, dates, ref, e);
                }
                LOG.warn("Cannot read the schedule of data source '{}' ({}) — the report will use its "
                         + "own period {} for it", ref.datasourceName, e.getMessage(), dates.periodId);
                continue;
            }
            boolean scheduled = dsSchedule != null && dsSchedule.hasSchedule();
            if (!scheduled && !ref.hasLookback()) {
                continue;   // no schedule to compare with: the report's own period, as before
            }
            try {
                if (!scheduled) {
                    throw new IllegalArgumentException("it has no run_details_json schedule");
                }
                BusinessCalendar calendar = RunScheduleConfig.DAILY.equals(dsSchedule.frequency)
                    ? calendars.forKey(dsSchedule.calendarKey) : null;
                List<Integer> within = RunDateCalculator.sourcePeriodIdsWithin(
                    dsSchedule, config.runScheduleConfig, dates, calendar);
                if (!ref.hasLookback()) {
                    plan.put(ref.datasourceName, within, within);
                } else {
                    List<Integer> validate = new ArrayList<>(RunDateCalculator.lookbackPeriodIds(
                        dsSchedule, config.runScheduleConfig, dates,
                        ref.lookbackFrom, ref.lookbackTo, calendar));
                    boolean sameFrequency = dsSchedule.frequency.equals(config.runScheduleConfig.frequency);
                    if (sameFrequency) {
                        // report period itself is the data source period; a required data source needs
                        // it even if the lookback range starts below offset 0
                        if (ref.required && !validate.contains(dates.periodId)) {
                            validate.add(dates.periodId);
                        }
                        plan.put(ref.datasourceName, validate, List.of(dates.periodId));
                    } else {
                        // finer data source (D7): every period inside the report period is also required
                        // and read, on top of the lookback range
                        for (int period : within) {
                            if (!validate.contains(period)) validate.add(period);
                        }
                        plan.put(ref.datasourceName, validate, within);
                    }
                }
            } catch (Exception e) {
                throw planFailure(config, dates, ref, e);
            }
        }
        return plan;
    }

    private static ReportProcessingException planFailure(ReportConfig config, RunDates dates,
                                                          ReportDatasourceRef ref, Exception cause) {
        return new ReportProcessingException(ReportProcessingException.Reason.DATASOURCE_UNAVAILABLE,
            config.reportName, config.reportSubprocess, dates.periodId,
            "Cannot work out the periods needed from data source '" + ref.datasourceName + "': "
            + cause.getMessage(), cause);
    }

    /**
     * BAU "does it still need to run?" check for a report: if this report is already
     * {@code COMPLETED} in RptRefer for the calculated period, skip it. Bypassed by
     * {@code --manualOverrun} — which is only about re-running/overwriting; it does not make an
     * ineligible report eligible (that was decided by the schedule before this is called).
     *
     * <p>Every report reaching this check has a run schedule (an unscheduled report is not
     * processed at all — owner decision D5). Shared with {@code PipelineSequenceFactory}, which makes the same check at submission.
     *
     * @return true if the report should be skipped (and has been logged as such)
     */
    public static boolean isAlreadyCompleted(FrameworkOptions options,
                                      RunDateCalculator.ScheduleDecision decision, String reportName) {
        if (options.getManualOverrun()) {
            LOG.info("--manualOverrun: report '{}' runs for period {} even if already COMPLETED",
                     reportName, decision.dates.periodId);
            return false;
        }
        boolean completed = new BigQueryReportCheckpointAdapter(options)
            .isCompleted(reportName, decision.dates.periodId);
        if (completed) {
            LOG.info("Report '{}' already COMPLETED for period {} — not run again "
                     + "(--manualOverrun=true to force)", reportName, decision.dates.periodId);
        }
        return completed;
    }

    /**
     * Same as {@link #execute(FrameworkOptions)}, but skips the {@code BigQueryReportRepository}
     * lookup and run-date resolution, using an already-fetched {@link ReportConfig} and
     * already-resolved {@link RunDates} instead.
     *
     * <p>Exists so {@link ReportFinalizeTransform} can call this from inside a worker DoFn — the
     * driver JVM pre-fetches {@code ReportConfig} and resolves {@code RunDates} (both plain
     * {@code Serializable} data objects) and passes them in as DoFn fields, avoiding the
     * {@code BigQueryReportRepository}-inside-a-DoFn violation that calling
     * {@link #execute(FrameworkOptions)} directly on a worker would cause, and guaranteeing the
     * report uses the dates decided at submission time rather than whenever the worker runs.
     *
     * @param plan from {@link #resolveDataPlan} (driver JVM): the data source periods that must be
     *             COMPLETED before the report proceeds, and the ones whose rows it reads
     */
    public void execute(FrameworkOptions options, ReportConfig config, RunDates dates,
                        ReportDataPlan plan) {
        String reportName       = config.reportName;
        String reportSubprocess = config.reportSubprocess;
        int    periodId         = dates.periodId;

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
                runPreprocessing(config, options, dates);
            }

            // ── 4. Datasource availability check ──────────────────────────────
            currentReason = ReportProcessingException.Reason.DATASOURCE_UNAVAILABLE;
            checkDatasourceAvailability(config, periodId, dsAdapter, plan);

            // ── 5. Build alias registry (stage DaRec rows into RptStageDa) ───
            currentReason = ReportProcessingException.Reason.STAGING_FAILURE;
            Map<String, String> aliasRegistry = buildAliasRegistry(config, periodId, rptId, dsAdapter, reportAdapter, plan);

            // ── 6. Transformation chain ───────────────────────────────────────
            currentReason = ReportProcessingException.Reason.TRANSFORM_FAILURE;
            if (config.hasTransforms()) {
                runTransformChain(config, options, dates, aliasRegistry);
            }

            // ── 6b. Write final result to per-report BQ table ─────────────────
            currentReason = ReportProcessingException.Reason.OUTPUT_FAILURE;
            if (config.hasOutputBqTable()) {
                writeOutputBqTable(config, aliasRegistry);
            }

            // ── 7. Route outputs to sinks (GCS / BQ / API) ───────────────────
            List<ReportOutputSinkRouter.OutputResult> outputResults =
                exportOutputs(config, options, dates, aliasRegistry);
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
                    .map(r -> new ExportedFile(r.destination(), r.fileName(), r.contentType(),
                        config.reportName + "_" + dates.periodId))
                    .toList();
                sendEmail(config, dates, attachments);
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

    private void runPreprocessing(ReportConfig config, FrameworkOptions options, RunDates dates) {
        LOG.info("Running {} preprocessing step(s)", config.preprocessingSteps.size());
        for (ReportPreprocessingStep step : config.preprocessingSteps) {
            LOG.info("Preprocessing step {}: type={} name={}",
                     step.stepOrder, step.stepType, step.stepName);
            switch (step.stepType) {
                case ReportPreprocessingStep.BQ_QUERY -> {
                    String sql = QueryParameterResolver.resolve(
                            step.bqQuery, step.queryParams, options, dates,
                            config.dateParams, step.stepName);
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

    private void checkDatasourceAvailability(ReportConfig config, int periodId,
                                              DataSourceCheckpointAdapter dsAdapter,
                                              ReportDataPlan plan) {
        LOG.info("Checking availability of {} datasource(s)", config.datasources.size());
        List<String> missing = new ArrayList<>();
        for (ReportDatasourceRef ref : config.datasources) {
            // Required data sources, and any data source with a lookback (an explicit validation),
            // must be COMPLETED for every period the plan lists: the report's own period; every
            // data source period inside the report period for a finer data source (D7); or the
            // whole lookback range.
            if (!ref.required && !ref.hasLookback()) continue;
            for (int period : plan.validatePeriods(ref.datasourceName, periodId)) {
                if (!dsAdapter.isCompleted(ref.datasourceName, period)) {
                    missing.add(ref.datasourceName + "/" + ref.datasourceSubprocess
                                + " (period=" + period + " not COMPLETED)");
                }
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                "Datasource period(s) not yet COMPLETED for report period="
                + periodId + ": " + missing);
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
    private Map<String, String> buildAliasRegistry(ReportConfig config, int periodId, long rptId,
                                                    DataSourceCheckpointAdapter dsAdapter,
                                                    ReportCheckpointAdapter reportAdapter,
                                                    ReportDataPlan plan) {
        Map<String, String> registry = new LinkedHashMap<>();

        for (ReportDatasourceRef ref : config.datasources) {
            // One staged run per data source period the report reads: just the report's own period
            // normally; every data source period inside the report period for a finer data source.
            List<Long> mapIds = new ArrayList<>();
            for (int period : plan.stagePeriods(ref.datasourceName, periodId)) {
                long daId;
                try {
                    daId = dsAdapter.fetchLatestCompletedDaId(ref.datasourceName, period);
                } catch (IllegalArgumentException e) {
                    if (ref.required) {
                        throw e;  // required datasource must be present for every period
                    }
                    LOG.warn("Optional datasource '{}' has no COMPLETED DaRefer row for period={} "
                             + "— that period is left out of alias '{}'",
                             ref.datasourceName, period, ref.transformAlias);
                    continue;
                }
                long mapId = reportAdapter.addDaMapping(rptId, daId);
                reportAdapter.stageFromDaRec(mapId, daId);
                mapIds.add(mapId);
                LOG.info("Datasource '{}' period {} staged (da_id={} map_id={})",
                         ref.datasourceName, period, daId, mapId);
            }
            if (mapIds.isEmpty()) {
                continue;   // optional datasource with nothing available: alias not registered
            }
            registry.put(ref.transformAlias, reportAdapter.stagedDataSubquery(mapIds));
            LOG.info("Alias '{}' → RptStageDa subquery over {} staged run(s)",
                     ref.transformAlias, mapIds.size());
        }
        return registry;
    }

    private void runTransformChain(ReportConfig config, FrameworkOptions options, RunDates dates,
                                   Map<String, String> aliasRegistry) {
        LOG.info("Running {} transformation step(s)", config.transformSteps.size());
        for (ReportTransformStep step : config.transformSteps) {
            LOG.info("Transform step {}: '{}' → alias '{}'",
                     step.stepOrder, step.stepName, step.outputAlias);

            // Alias tokens first ({trades} → staged subquery or `project.dataset.table`),
            // then standard + custom params ({periodStart}, {exchange}, etc.)
            String sql = resolveAliasTokens(step.queryTemplate, aliasRegistry);
            sql = QueryParameterResolver.resolve(sql, step.queryParams, options, dates,
                                                 config.dateParams, step.stepName);

            bqJobService.runQueryToTable(sql, step.outputBqTable);
            aliasRegistry.put(step.outputAlias, step.outputBqTable);
            LOG.info("Step '{}' result registered as alias '{}' → {}",
                     step.stepName, step.outputAlias, step.outputBqTable);
        }
    }

    private List<ReportOutputSinkRouter.OutputResult> exportOutputs(
            ReportConfig config, FrameworkOptions options, RunDates dates,
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
                bqJobService.runQueryToTable(flattenStaged(sourceTable), tempTable);
                sourceTable = tempTable;
            }

            LOG.info("Output {}: sinkType={} alias='{}' source={}",
                     output.outputOrder, output.sinkType, output.inputAlias, sourceTable);

            ReportOutputSinkRouter.OutputResult outputResult =
                sinkRouter.route(output, sourceTable, config, options, dates);
            result.add(outputResult);

            LOG.info("Output {} done → {}", output.outputOrder, outputResult.destination());
        }
        return result;
    }

    /**
     * SQL that reads a staged data source alias as an ordinary table: one column per top-level
     * JSON key (the CSV header), the key's value in each row — instead of the single
     * {@code stage_ds_json_tx} column holding each record's JSON. See {@link JsonRowFlattener}.
     * An empty source (no rows, so no keys) keeps the plain {@code SELECT *}, which is empty too.
     */
    private String flattenStaged(String stagedSubquery) {
        List<String> keys = bqJobService.queryStrings(JsonRowFlattener.keysQuery(stagedSubquery))
            .stream().filter(k -> k != null && !k.isEmpty()).toList();
        if (keys.isEmpty()) {
            LOG.warn("Staged data has no JSON keys (empty source?) — exporting it unchanged");
            return "SELECT * FROM " + stagedSubquery;
        }
        LOG.info("Flattening staged data into {} column(s): {}", keys.size(), keys);
        return JsonRowFlattener.selectSql(stagedSubquery, keys);
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
        String sql = sourceRef.startsWith("(")
            ? flattenStaged(sourceRef)
            : "SELECT * FROM `" + sourceRef + "`";
        LOG.info("Writing final report: alias='{}' → {}", sourceAlias, config.outputBqTable);
        bqJobService.runQueryToTable(sql, config.outputBqTable);
        LOG.info("Final report written to {}", config.outputBqTable);
    }

    private void sendEmail(ReportConfig config, RunDates dates,
                           List<ExportedFile> exportedFiles) {
        if (emailUtility == null) {
            LOG.warn("No EmailSendUtility available (none injected, none configured in EmailSendUtilities.create()) — "
                     + "skipping report-completion email for report={}", config.reportName);
            return;
        }

        String subject = resolveEmailTokens(config.emailConfig.subjectTemplate, config, dates);
        String body    = resolveEmailTokens(config.emailConfig.bodyTemplate,    config, dates);

        List<EmailAttachment> attachments = new ArrayList<>();
        for (ExportedFile file : exportedFiles) {
            // The stored object's name may carry an extra prefix, so locate it by the stem
            // (reportName_periodId) in the output directory instead of the exact URI.
            String uri = file.gcsUri();
            String dir = uri.substring(0, uri.lastIndexOf('/') + 1);
            String ext = file.fileName().contains(".")
                ? file.fileName().substring(file.fileName().lastIndexOf('.')) : "";
            attachments.add(new EmailAttachment(file.fileName(),
                emailUtility.FetchFileFromGcsByPrefix(dir, file.fileName(), file.stem(), ext),
                file.contentType()));
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
                                              RunDates dates) {
        if (template == null) return "";
        return template
            .replace("{reportName}",       config.reportName)
            .replace("{reportSubprocess}", config.reportSubprocess)
            .replace("{periodId}",         String.valueOf(dates.periodId))
            .replace("{periodStart}",      dates.periodStartIso())
            .replace("{periodEnd}",        dates.periodEndIso())
            .replace("{runDate}",          dates.runDateIso());
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    private record ExportedFile(String gcsUri, String fileName, String contentType, String stem) {}
}
