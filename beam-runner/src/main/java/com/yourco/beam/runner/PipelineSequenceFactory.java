package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.exception.PipelineException;
import com.yourco.beam.exception.ReportProcessingException;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Runs a complete {@code PIPELINE} sequence in one blocking call: submits every datasource the
 * terminal report itself declares via {@code ReportConfig.datasources[]} as a single batched
 * Dataflow job, blocks until every <em>required</em> one reaches {@code COMPLETED}, then runs the
 * report and returns. One Airflow task, one JVM invocation, all the way through the report's own
 * completion email.
 *
 * <p>There is no separate pipeline config — {@code --reportName}/{@code --reportSubprocess} are
 * the same flags {@code REPORT_PROCESSING} already uses. A report's own {@code datasources[]}
 * list (with each entry's {@code is_required}) IS the pipeline: it already declares exactly
 * which datasources feed the report and which of those are mandatory, so nothing else needs to
 * redeclare that as a second, separately-maintained sequence.
 *
 * <h2>Why the wait is a poll loop, not {@code waitUntilFinish()}</h2>
 * This framework's runner platform cannot reliably block on a submitted job's own
 * {@code PipelineResult} — calling {@code PipelineResult.waitUntilFinish()} specifically does not
 * work here. The process itself, though, can be held open for as long as needed — so instead of
 * calling that method, {@link #execute} blocks in {@link DataSourceStatusChecker#awaitPipeline},
 * a plain {@code Thread.sleep} loop re-reading {@code DaRefer} (the same row
 * {@code PostDownloadFinalizeTransform} writes from the worker) until every required datasource
 * reaches {@code COMPLETED}, a required one hits a terminal failure, or the configured timeout
 * elapses.
 *
 * <h2>Execution</h2>
 * <pre>
 *   PipelineSequenceFactory.execute(options)
 *   ├─ 1. BigQueryReportRepository.fetchReportConfig()             → ReportConfig.datasources[]
 *   ├─ 2. fetch SourceConfig for every declared datasource (by name, via BigQuerySourceConfigRepository)
 *   ├─ 3. DataSourcePipelineFactory.assembleForConfigs()            → ONE batched Dataflow job
 *   │        (internally skips any datasource already COMPLETED, same as standalone DATA_SOURCE_DOWNLOAD)
 *   ├─ 4. pipeline.run()                                            → submitted
 *   ├─ 5. DataSourceStatusChecker.awaitPipeline(options)             → blocks here (poll loop)
 *   │        required datasource(s) all COMPLETED → proceed
 *   │        a required datasource terminal-failed  → throw PipelineException(ABORTED_REQUIRED_DATASOURCE)
 *   │        poll loop exceeds --jobPollTimeoutMinutes → throw PipelineException(TIMEOUT)
 *   └─ 6. ReportPipelineFactory.execute(options)                    → report + its completion email
 * </pre>
 *
 * <p>Composes the existing {@link DataSourcePipelineFactory}, {@link DataSourceStatusChecker}, and
 * {@link ReportPipelineFactory} rather than reimplementing any of them.
 *
 * <h2>Exception propagation</h2>
 * {@link DataSourceDownloadException} from the submit phase and {@link ReportProcessingException}
 * from the report phase propagate <b>unchanged</b> — they already carry the specific detail
 * {@code Main} needs. {@link PipelineException} from the poll ({@code ABORTED_REQUIRED_DATASOURCE},
 * {@code TIMEOUT}) is already the right type too. Only PIPELINE's own config lookup, or an
 * unrecognized exception type, gets wrapped in {@link PipelineException} here.
 *
 * <h2>{@code --manualOverrun}</h2>
 * Applies uniformly across the whole sequence, exactly as it does standalone, because the same
 * {@code options} instance — never a copy, never a reset field — is passed straight into both
 * {@link DataSourcePipelineFactory#assembleForConfigs} and {@link ReportPipelineFactory#execute}:
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
 * This is a real invariant this class relies on, not an accident of implementation — do not
 * introduce a scoped copy of {@code options} for either phase without re-threading this flag.
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
                     + "complete — same as standalone DATA_SOURCE_DOWNLOAD. The terminal REPORT "
                     + "step always re-runs regardless (it has no COMPLETED guard of its own).");
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

        // A DataSourceDownloadException, ReportProcessingException, or PipelineException raised
        // by any composed phase already carries the right specific detail — pass it through
        // unchanged rather than re-wrapping. Only an exception PIPELINE doesn't recognize gets
        // wrapped here.
        try {
            submitDataSourceSteps(options, datasources);
            new DataSourceStatusChecker().awaitPipeline(options);

            // reportName/reportSubprocess were never touched — options is exactly what the
            // operator passed in, same flags REPORT_PROCESSING already reads.
            new ReportPipelineFactory().execute(options);
        } catch (DataSourceDownloadException | ReportProcessingException | PipelineException e) {
            throw e;
        } catch (Exception e) {
            throw PipelineException.wrap(PipelineException.Reason.UNKNOWN,
                reportName, reportSubprocess, periodId, e);
        }

        LOG.info("PIPELINE completed: report={}", reportName);
    }

    // ── Phase 1: batched data-source job ────────────────────────────────────

    private void submitDataSourceSteps(FrameworkOptions options, List<ReportDatasourceRef> datasources) {
        if (datasources.isEmpty()) {
            LOG.info("Report declares no datasources — proceeding straight to the report");
            return;
        }

        String names = datasources.stream().map(ref -> ref.datasourceName).distinct()
            .collect(Collectors.joining(","));

        List<SourceConfig> sourceConfigs = new ArrayList<>();
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

        // assembleForConfigs() already throws DataSourceDownloadException itself on failure —
        // let it propagate unchanged, it's already the right type.
        DataSourcePipelineFactory dsFactory = new DataSourcePipelineFactory();
        Pipeline pipeline = dsFactory.assembleForConfigs(options, sourceConfigs);

        LOG.info("Submitting batched data-source job ({} datasource(s)) to runner: {}",
                 datasources.size(), options.getRunner().getSimpleName());
        pipeline.run();
        LOG.info("Batched data-source job submitted — blocking until required datasource(s) "
                 + "complete (poll loop, not waitUntilFinish(); every {}s, up to {}m)",
                 options.getJobPollIntervalSeconds(), options.getJobPollTimeoutMinutes());
    }

    // ── Validation ───────────────────────────────────────────────────────────

    private static void validateRequiredParameters(FrameworkOptions options) {
        if (options.getReportName() == null || options.getReportName().isBlank()) {
            throw new PipelineException(PipelineException.Reason.CONFIGURATION_ERROR,
                options.getReportName(), options.getReportSubprocess(), options.getPeriodId(),
                "--reportName is required for PIPELINE");
        }
        if (options.getPeriodId() <= 0) {
            throw new PipelineException(PipelineException.Reason.CONFIGURATION_ERROR,
                options.getReportName(), options.getReportSubprocess(), options.getPeriodId(),
                "--periodId is required for PIPELINE");
        }
    }
}
