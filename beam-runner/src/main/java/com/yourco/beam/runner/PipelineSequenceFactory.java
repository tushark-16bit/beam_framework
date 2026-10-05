package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.exception.PipelineException;
import com.yourco.beam.exception.ReportProcessingException;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.RunDateCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Assembles a complete {@code PIPELINE} sequence — every datasource the terminal report itself
 * declares via {@code ReportConfig.datasources[]}, plus the report step, plus the report's
 * completion/failure email — as ONE batched Dataflow job, submits it once, and returns
 * immediately. Nothing in the driver JVM blocks.
 *
 * <p>There is no separate pipeline config — {@code --reportName}/{@code --reportSubprocess} are
 * the same flags {@code REPORT_PROCESSING} already uses. A report's own {@code datasources[]}
 * list (with each entry's {@code is_required}) IS the pipeline: it already declares exactly
 * which datasources feed the report and which of those are mandatory, so nothing else needs to
 * redeclare that as a second, separately-maintained sequence.
 *
 * <h2>Why the wait moved onto the worker</h2>
 * This deployment launches via a Dataflow Flex Template, whose launch contract requires
 * {@code main()} to submit the pipeline and exit promptly — the launch is considered complete
 * once the launcher process exits, not once the job finishes. A driver-JVM poll loop (this
 * class's previous design) or {@code PipelineResult.waitUntilFinish()} both violate that
 * contract and were traced to a real incident (Airflow timing out at the graph level while the
 * Dataflow job was still — or already — running). The fix: {@link #execute} wires the report
 * step onto the SAME pipeline as the datasource branches, via
 * {@link ReportFinalizeTransform#wire}, gated on a {@code Wait.on()} data-dependency barrier
 * instead of a poll loop — then submits once and returns. See {@code Main}'s class javadoc.
 *
 * <h2>Execution</h2>
 * <pre>
 *   PipelineSequenceFactory.execute(options)
 *   ├─ 1. BigQueryReportRepository.fetchReportConfig()             → ReportConfig.datasources[]
 *   ├─ 2. fetch SourceConfig for every declared datasource (by name, via BigQuerySourceConfigRepository)
 *   ├─ 3. DataSourcePipelineFactory.assembleForConfigs()            → DataSourceAssembly: the
 *   │        batched pipeline + one finalize signal PCollection per datasource branch
 *   │        (internally skips any datasource already COMPLETED, same as standalone DATA_SOURCE_DOWNLOAD)
 *   ├─ 4. ReportFinalizeTransform.wire(pipeline, finalizeSignals, reportConfig, options)
 *   │        → adds Create.of(1) → Wait.on(finalizeSignals) → ReportRunDoFn to the SAME pipeline
 *   └─ 5. pipeline.run()                                            → submitted; execute() returns
 * </pre>
 * Everything downstream of submission — waiting for the datasource branches, verifying required
 * ones actually reached {@code COMPLETED}, running the report, sending its completion/failure
 * email — happens on a worker, inside {@link ReportFinalizeTransform}'s {@code ReportRunDoFn}.
 *
 * <p>Composes the existing {@link DataSourcePipelineFactory} and {@link ReportPipelineFactory}
 * rather than reimplementing either.
 *
 * <h2>Exception propagation</h2>
 * {@link DataSourceDownloadException} from the assembly/submit phase (synchronous, in the driver
 * JVM) propagates <b>unchanged</b> — it already carries the specific detail {@code Main} needs.
 * A failure discovered only on the worker (a required datasource that didn't reach
 * {@code COMPLETED}, or a {@link ReportProcessingException} from the report itself) never reaches
 * this method or {@code Main}'s catch block at all — {@code execute()} has already returned by
 * then, so {@link ReportFinalizeTransform} calls {@code FailureNotifier} itself from the worker.
 * Only PIPELINE's own config lookup, or an unrecognized exception type during assembly, gets
 * wrapped in {@link PipelineException} here.
 *
 * <h2>{@code --manualOverrun}</h2>
 * Applies uniformly across the whole sequence, exactly as it does standalone, because the same
 * {@code options} instance — never a copy, never a reset field — is passed straight into
 * {@link DataSourcePipelineFactory#assembleForConfigs}, and the worker reconstructs an equivalent
 * view of the same options via {@code PipelineOptions} injection before calling
 * {@link ReportPipelineFactory#execute}:
 * <ul>
 *   <li>Every declared datasource bypasses its own {@code COMPLETED} skip-guard and re-downloads,
 *       superseding its previous run's {@code DaRec} rows once the new run completes — identical
 *       to standalone {@code DATA_SOURCE_DOWNLOAD} under {@code --manualOverrun}, because it's the
 *       same {@code filterByCheckpoint} check reading the same flag off the same options object.</li>
 *   <li>The terminal {@code REPORT} step needs no special handling at all —
 *       {@code ReportPipelineFactory} has no {@code COMPLETED}-skip guard of its own to begin
 *       with (see its class javadoc); every invocation already inserts a fresh {@code RptRefer}
 *       row and re-runs, {@code --manualOverrun} or not.</li>
 * </ul>
 * This is a real invariant this class relies on, not an accident of implementation.
 */
public final class PipelineSequenceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineSequenceFactory.class);

    public void execute(FrameworkOptions options) {
        validateRequiredParameters(options);

        String reportName       = options.getReportName();
        String reportSubprocess = options.getReportSubprocess();
        int    periodId         = options.getPeriodId();
        LOG.info("PIPELINE | report={} subprocess={} period={}", reportName, reportSubprocess, periodId);
        if (options.getManualOverrun()) {
            LOG.info("--manualOverrun=true: every datasource this report declares will bypass "
                     + "its COMPLETED guard and re-download, superseding its previous run once "
                     + "complete — same as standalone DATA_SOURCE_DOWNLOAD. A scheduled report already "
                     + "COMPLETED for its period also re-runs. Eligibility and dates are NOT affected.");
        }

        ReportConfig reportConfig;
        try {
            BigQueryReportRepository reportRepo = new BigQueryReportRepository(options);
            reportConfig = reportRepo.fetchReportConfig(reportName, reportSubprocess, periodId);
        } catch (Exception e) {
            throw PipelineException.wrap(PipelineException.Reason.CONFIG_NOT_FOUND,
                reportName, reportSubprocess, periodId, e);
        }
        List<ReportDatasourceRef> datasources = reportConfig.datasources;

        // Finance Automation scheduling for the report half of PIPELINE — decided here in the
        // driver JVM at submission and carried to the worker as a DoFn field, so the report uses
        // the dates of the Business Date it was submitted on, not whatever "today" is when the
        // worker gets to it. Each datasource is evaluated separately inside
        // DataSourcePipelineFactory against its OWN schedule.
        //
        // BAU: data sources and reports are scheduled independently. A report that isn't
        // eligible today (or is already COMPLETED for its period) does not stop its data sources
        // from loading, and a skipped data source does not skip the report — the report instead
        // fails its required-datasource check when it runs, and is retried on a later execution.
        RunDates reportDates = decideReportRun(options, reportConfig);

        // A DataSourceDownloadException raised during assembly/submission already carries the
        // right specific detail — pass it through unchanged rather than re-wrapping. Only an
        // exception PIPELINE doesn't recognize gets wrapped here.
        try {
            assembleAndSubmit(options, datasources, reportConfig, reportDates);
        } catch (DataSourceDownloadException | PipelineException e) {
            throw e;
        } catch (Exception e) {
            throw PipelineException.wrap(PipelineException.Reason.UNKNOWN,
                reportName, reportSubprocess, periodId, e);
        }

        LOG.info("PIPELINE submitted: report={} — the report itself, and its completion/failure "
                 + "email, will run on a worker once every datasource branch finishes", reportName);
    }

    // ── Assembly: batched data-source job + report step, one pipeline, one submit ──

    /**
     * @param reportDates the report's dates if it should run in this job, or {@code null} if it
     *                    was skipped by its schedule / COMPLETED check — the data sources are then
     *                    still loaded, with no report step wired after them
     */
    private void assembleAndSubmit(FrameworkOptions options, List<ReportDatasourceRef> datasources,
                                    ReportConfig reportConfig, RunDates reportDates) {
        List<SourceConfig> sourceConfigs = new ArrayList<>();
        if (!datasources.isEmpty()) {
            String names = datasources.stream().map(ref -> ref.datasourceName).distinct()
                .collect(Collectors.joining(","));
            try {
                BigQuerySourceConfigRepository sourceRepo = new BigQuerySourceConfigRepository(options);
                for (ReportDatasourceRef ref : datasources) {
                    sourceConfigs.addAll(sourceRepo.fetchSourceConfigs(
                        options.getParentId(), ref.datasourceName, ref.datasourceSubprocess, options.getPeriodId()));
                }
            } catch (Exception e) {
                throw DataSourceDownloadException.wrap(DataSourceDownloadException.Reason.INVALID_INPUT,
                    names, null, options.getPeriodId(), e);
            }
        } else {
            LOG.info("Report declares no datasources — report step will run with nothing to wait for");
        }

        // Lookback periods (driver JVM — needs each data source's schedule). If they can't be
        // resolved the report step is skipped and reported, like a not-evaluable report schedule;
        // the data sources still load.
        Map<String, List<Integer>> lookbackPeriods = Map.of();
        if (reportDates != null) {
            warnOnPeriodMismatch(options, sourceConfigs, reportDates);
            try {
                lookbackPeriods = ReportPipelineFactory.resolveLookbackPeriods(options, reportConfig, reportDates);
            } catch (Exception e) {
                LOG.error("Report '{}' skipped — lookback periods could not be resolved: {}",
                          reportConfig.reportName, e.getMessage());
                FailureNotifier.notify(options, e);
                reportDates = null;
            }
        }

        // assembleForConfigs() already throws DataSourceDownloadException itself on failure —
        // let it propagate unchanged, it's already the right type. It also applies each data
        // source's own schedule, dropping any that aren't eligible today.
        DataSourcePipelineFactory dsFactory = new DataSourcePipelineFactory();
        DataSourceAssembly assembly = dsFactory.assembleForConfigs(options, sourceConfigs);

        if (reportDates == null && assembly.isEmpty()) {
            LOG.info("PIPELINE: report '{}' not run and no datasource eligible/pending — no job "
                     + "submitted", reportConfig.reportName);
            return;
        }

        if (reportDates != null) {
            // Wire the report step onto the SAME pipeline, gated on every datasource branch's
            // finalize signal via Wait.on() — no driver-JVM poll loop.
            ReportFinalizeTransform.wire(assembly.pipeline, assembly.finalizeSignals, reportConfig,
                reportDates, lookbackPeriods, options);
        }

        LOG.info("Submitting batched PIPELINE job ({} datasource branch(es){}) to runner: {}",
                 assembly.finalizeSignals.size(),
                 reportDates != null ? " + report='" + reportConfig.reportName + "'" : ", no report step",
                 options.getRunner().getSimpleName());
        assembly.pipeline.run();
    }

    /**
     * Evaluates the report's own schedule and COMPLETED status.
     *
     * @return the report's dates if it should run in this job; {@code null} if it is skipped
     *         (not eligible today, already COMPLETED, or its schedule is not evaluable — the last
     *         is also sent through {@link FailureNotifier}, without failing the data sources)
     */
    private static RunDates decideReportRun(FrameworkOptions options, ReportConfig reportConfig) {
        String reportName = reportConfig.reportName;
        RunDateCalculator.ScheduleDecision decision;
        try {
            decision = RunDateCalculator.evaluateReport(reportConfig.runScheduleConfig, options);
        } catch (Exception e) {
            // e.g. an invalid --runDate / --periodStart / --businessTimeZone: nothing can run.
            throw PipelineException.wrap(PipelineException.Reason.CONFIGURATION_ERROR,
                reportName, reportConfig.reportSubprocess, options.getPeriodId(), e);
        }
        if (decision.status == RunDateCalculator.ScheduleDecision.Status.NOT_EVALUABLE) {
            LOG.error("Report '{}' skipped — run schedule could not be evaluated: {}",
                      reportName, decision);
            FailureNotifier.notify(options, new PipelineException(
                PipelineException.Reason.CONFIGURATION_ERROR, reportName,
                reportConfig.reportSubprocess, options.getPeriodId(),
                "Run schedule for report '" + reportName + "' could not be evaluated on business date "
                + decision.businessDate + ": " + decision.detail + " — report skipped, its data "
                + "sources still run; will be re-evaluated on the next run"));
            return null;
        }
        if (!decision.shouldRun()) {
            LOG.info("Report '{}' not run today: {} — its data sources are still evaluated",
                     reportName, decision);
            return null;
        }
        LOG.info("Report '{}' eligible: {}", reportName, decision);
        if (ReportPipelineFactory.isAlreadyCompleted(options, decision, reportName)) {
            return null;
        }
        return decision.dates;
    }

    /**
     * The report finds each datasource's DaRefer row by the report's own {@code periodId}, so a
     * datasource whose schedule resolves to a different one can never be picked up by this
     * report. Warn at submission rather than only discovering it after the download runs.
     * Only data sources eligible today are compared; the others are skipped (and reported) by
     * {@code assembleForConfigs()} right after.
     */
    private static void warnOnPeriodMismatch(FrameworkOptions options, List<SourceConfig> sourceConfigs,
                                             RunDates reportDates) {
        for (SourceConfig config : sourceConfigs) {
            int sourcePeriodId;
            try {
                RunDateCalculator.ScheduleDecision decision =
                    RunDateCalculator.evaluateDataSource(config.runScheduleConfig, options);
                if (!decision.shouldRun()) continue;
                sourcePeriodId = decision.dates.periodId;
            } catch (Exception e) {
                continue;
            }
            if (sourcePeriodId != reportDates.periodId) {
                LOG.warn("Datasource '{}' resolves to periodId={} but report resolves to periodId={} "
                         + "— the report will not find this datasource's run",
                         config.datasourceName, sourcePeriodId, reportDates.periodId);
            }
        }
    }

    // ── Validation ───────────────────────────────────────────────────────────

    private static void validateRequiredParameters(FrameworkOptions options) {
        if (options.getReportName() == null || options.getReportName().isBlank()) {
            throw new PipelineException(PipelineException.Reason.CONFIGURATION_ERROR,
                options.getReportName(), options.getReportSubprocess(), options.getPeriodId(),
                "--reportName is required for PIPELINE");
        }
        // --periodId is NOT required: the report's and every datasource's period id is calculated
        // from the Business Date by RunDateCalculator. An item without a run schedule is reported
        // as not evaluable, with a message asking for --periodId.
    }
}
