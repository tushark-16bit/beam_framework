package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.exception.PipelineException;
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
 * <b>Submits</b> the {@code PIPELINE} datasource phase: one batched Dataflow job covering every
 * not-yet-{@code COMPLETED} datasource the terminal report itself declares via
 * {@code ReportConfig.datasources[]}. Does not wait for it and does not run the report — see
 * below for why.
 *
 * <p>There is no separate pipeline config — {@code --reportName}/{@code --reportSubprocess} are
 * the same flags {@code REPORT_PROCESSING} already uses. A report's own {@code datasources[]}
 * list (with each entry's {@code is_required}) IS the pipeline: it already declares exactly
 * which datasources feed the report and which of those are mandatory.
 *
 * <h2>Why this only submits (no waitUntilFinish, no report call)</h2>
 * This framework's runner platform forbids {@code PipelineResult.waitUntilFinish()} — the driver
 * JVM must submit and return quickly. That means this class can no longer synchronously learn
 * whether the batched job — let alone each individual datasource branch inside it — has actually
 * finished, so it cannot safely re-check required/optional status or run the report as part of
 * the same call the way it used to. That work moved to two other places, both DB-only and
 * external-poller-driven:
 * <ul>
 *   <li>{@link DataSourceStatusChecker#checkPipeline} — the same required/optional gate this
 *       class used to run inline right after {@code waitUntilFinish()} returned, now invoked via
 *       {@code --processType=STATUS_CHECK} by an external poller (an Airflow sensor) once it
 *       expects the batched job to have finished.</li>
 *   <li>Plain {@code --processType=REPORT_PROCESSING --reportName=...} (unchanged,
 *       {@link ReportPipelineFactory}) — invoked by that same poller once
 *       {@code STATUS_CHECK} reports ready. It already re-verifies required datasources itself
 *       (a cheap belt-and-suspenders DB check), so nothing here needs to duplicate that.</li>
 * </ul>
 *
 * <h2>Execution</h2>
 * <pre>
 *   PipelineSequenceFactory.execute(options)
 *   ├─ BigQueryReportRepository.fetchReportConfig()             → ReportConfig.datasources[]
 *   ├─ fetch SourceConfig for every declared datasource (by name, via BigQuerySourceConfigRepository)
 *   ├─ DataSourcePipelineFactory.assembleForConfigs()            → ONE batched Dataflow job
 *   │     (internally skips any datasource already COMPLETED, same as standalone DATA_SOURCE_DOWNLOAD)
 *   └─ pipeline.run()                                            → submitted, NOT awaited; returns
 * </pre>
 *
 * <p>Composes the existing {@link DataSourcePipelineFactory} rather than reimplementing it — this
 * class only decides which sources to batch together into one job.
 *
 * <h2>Exception propagation</h2>
 * {@link DataSourceDownloadException} from the data-source phase propagates <b>unchanged</b> — it
 * already carries the specific detail {@code Main} needs. Only a failure of some other type (or
 * PIPELINE's own config lookup) is wrapped in {@link PipelineException}.
 *
 * <h2>{@code --manualOverrun}</h2>
 * Applies exactly as it does standalone: the same {@code options} instance — never a copy, never
 * a reset field — is passed straight into {@link DataSourcePipelineFactory#assembleForConfigs}, so
 * every declared datasource bypasses its own {@code COMPLETED} skip-guard and re-downloads,
 * superseding its previous run's {@code DaRec} rows once the new run completes — identical to
 * standalone {@code DATA_SOURCE_DOWNLOAD} under {@code --manualOverrun}, because it's the same
 * {@code filterByCheckpoint} check reading the same flag off the same options object. The
 * subsequent {@code REPORT_PROCESSING} invocation carries {@code --manualOverrun} the same way —
 * {@code ReportPipelineFactory} has no {@code COMPLETED}-skip guard of its own to begin with, so
 * it needs no special handling either.
 */
public final class PipelineSequenceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineSequenceFactory.class);

    public void execute(FrameworkOptions options) {
        validateRequiredParameters(options);

        String reportName       = options.getReportName();
        String reportSubprocess = options.getReportSubprocess();
        int    periodId         = options.getPeriodId();
        LOG.info("PIPELINE (submit datasources) | report={} subprocess={} period={}",
                 reportName, reportSubprocess, periodId);
        if (options.getManualOverrun()) {
            LOG.info("--manualOverrun=true: every datasource this report declares will bypass "
                     + "its COMPLETED guard and re-download, superseding its previous run once "
                     + "complete — same as standalone DATA_SOURCE_DOWNLOAD.");
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

        // A DataSourceDownloadException raised while submitting already carries the right
        // specific detail — pass it through unchanged rather than re-wrapping. Only an exception
        // PIPELINE doesn't recognize gets wrapped here.
        try {
            submitDataSourceSteps(options, datasources);
        } catch (DataSourceDownloadException | PipelineException e) {
            throw e;
        } catch (Exception e) {
            throw PipelineException.wrap(PipelineException.Reason.UNKNOWN,
                reportName, reportSubprocess, periodId, e);
        }

        LOG.info("PIPELINE datasource submission complete for report={}. This call does not wait "
                 + "for completion or run the report — poll readiness via "
                 + "--processType=STATUS_CHECK --reportName={} --reportSubprocess={} "
                 + "--periodId={}, then invoke --processType=REPORT_PROCESSING to run the report.",
                 reportName, reportName, reportSubprocess, periodId);
    }

    // ── Submit the batched data-source job ──────────────────────────────────

    private void submitDataSourceSteps(FrameworkOptions options, List<ReportDatasourceRef> datasources) {
        if (datasources.isEmpty()) {
            LOG.info("Report declares no datasources — nothing to submit");
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
        LOG.info("Batched data-source job submitted — not waiting for completion.");
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
