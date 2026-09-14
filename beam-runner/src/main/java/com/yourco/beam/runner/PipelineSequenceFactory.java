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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

        // A DataSourceDownloadException raised during assembly/submission already carries the
        // right specific detail — pass it through unchanged rather than re-wrapping. Only an
        // exception PIPELINE doesn't recognize gets wrapped here.
        try {
            assembleAndSubmit(options, datasources, reportConfig);
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

    private void assembleAndSubmit(FrameworkOptions options, List<ReportDatasourceRef> datasources,
                                    ReportConfig reportConfig) {
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

        // assembleForConfigs() already throws DataSourceDownloadException itself on failure —
        // let it propagate unchanged, it's already the right type.
        DataSourcePipelineFactory dsFactory = new DataSourcePipelineFactory();
        DataSourceAssembly assembly = dsFactory.assembleForConfigs(options, sourceConfigs);

        // Wire the report step onto the SAME pipeline, gated on every datasource branch's
        // finalize signal via Wait.on() — no driver-JVM poll loop.
        ReportFinalizeTransform.wire(assembly.pipeline, assembly.finalizeSignals, reportConfig, options);

        LOG.info("Submitting batched PIPELINE job ({} datasource(s) + report='{}') to runner: {}",
                 datasources.size(), reportConfig.reportName, options.getRunner().getSimpleName());
        assembly.pipeline.run();
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
