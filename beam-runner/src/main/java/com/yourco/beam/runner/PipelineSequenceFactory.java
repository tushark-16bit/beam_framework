package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.exception.PipelineException;
import com.yourco.beam.exception.ReportProcessingException;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Assembles and runs a {@code PIPELINE} sequence: every datasource the terminal report itself
 * declares via {@code ReportConfig.datasources[]}, batched into one Dataflow job if not already
 * {@code COMPLETED} for the period, followed by the report.
 *
 * <p>There is no separate pipeline config — {@code --reportName}/{@code --reportSubprocess} are
 * the same flags {@code REPORT_PROCESSING} already uses. A report's own {@code datasources[]}
 * list (with each entry's {@code is_required}) IS the pipeline: it already declares exactly
 * which datasources feed the report and which of those are mandatory, so nothing else needs to
 * redeclare that as a second, separately-maintained sequence. {@code PIPELINE} differs from
 * plain {@code REPORT_PROCESSING} only in what happens when a declared datasource isn't
 * {@code COMPLETED} yet: {@code REPORT_PROCESSING} just fails
 * ({@code checkDatasourceAvailability()}); {@code PIPELINE} runs it first.
 *
 * <h2>Execution</h2>
 * <pre>
 *   PipelineSequenceFactory.execute(options)
 *   ├─ BigQueryReportRepository.fetchReportConfig()             → ReportConfig.datasources[]
 *   ├─ fetch SourceConfig for every declared datasource (by name, via BigQuerySourceConfigRepository)
 *   ├─ DataSourcePipelineFactory.assembleForConfigs()            → ONE batched Dataflow job
 *   │     (internally skips any datasource already COMPLETED, same as standalone DATA_SOURCE_DOWNLOAD)
 *   ├─ pipeline.run().waitUntilFinish()
 *   ├─ re-check each declared datasource's checkpoint status
 *   │     unmet + is_required=true    → abort, throw PipelineException(ABORTED_REQUIRED_DATASOURCE)
 *   │     unmet + is_required=false   → log, continue
 *   └─ ReportPipelineFactory.execute()                           → terminal report, unchanged
 * </pre>
 *
 * <p>Composes the existing {@link DataSourcePipelineFactory} and {@link ReportPipelineFactory}
 * rather than reimplementing either — this class only decides which sources to batch together
 * and whether an incomplete one should stop the sequence before the report runs.
 *
 * <h2>Exception propagation</h2>
 * {@link com.yourco.beam.exception.DataSourceDownloadException} from the data-source phase or
 * {@link ReportProcessingException} from the report phase propagate <b>unchanged</b> — they
 * already carry the specific detail {@code Main} needs. Only a failure of some other type (or
 * PIPELINE's own config lookup / required-datasource gate) is wrapped in
 * {@link com.yourco.beam.exception.PipelineException}.
 *
 * <h2>{@code --manualOverrun}</h2>
 * Applies uniformly across the whole sequence, exactly as it does standalone, because the same
 * {@code options} instance — never a copy, never a reset field — is passed straight into both
 * {@link DataSourcePipelineFactory#assembleForConfigs} and {@link ReportPipelineFactory#execute}:
 * <ul>
 *   <li>Every declared datasource bypasses its own {@code COMPLETED} skip-guard and
 *       re-downloads, superseding its previous run's {@code DaRec} rows once the new run
 *       completes — identical to standalone {@code DATA_SOURCE_DOWNLOAD} under
 *       {@code --manualOverrun}, because it's the same {@code filterByCheckpoint} check reading
 *       the same flag off the same options object.</li>
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
        // by either composed phase already carries the right specific detail — pass it through
        // unchanged rather than re-wrapping. Only an exception PIPELINE doesn't recognize gets
        // wrapped here.
        try {
            runDataSourceSteps(options, datasources);
            checkRequiredDataSources(options, datasources, reportName);

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

    private void runDataSourceSteps(FrameworkOptions options, List<ReportDatasourceRef> datasources) {
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
        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
            LOG.info("Batched data-source job finished with state: {}", result.getState());
        } catch (Exception e) {
            LOG.error("Batched data-source job threw exception: {}", e.getMessage(), e);
            DataSourceDownloadException.Reason reason = DataSourceFailureClassifier.classify(e);
            throw DataSourceDownloadException.wrap(reason, names, null, options.getPeriodId(), e);
        }
    }

    // ── Phase 2: required/optional gate ─────────────────────────────────────

    private void checkRequiredDataSources(FrameworkOptions options, List<ReportDatasourceRef> datasources,
                                          String reportName) {
        if (datasources.isEmpty()) return;

        BigQueryDataSourceCheckpointAdapter checkpointAdapter =
            new BigQueryDataSourceCheckpointAdapter(options);

        List<String> unmetRequired = new ArrayList<>();
        for (ReportDatasourceRef ref : datasources) {
            if (checkpointAdapter.isCompleted(ref.datasourceName, options.getPeriodId())) continue;

            if (ref.required) {
                unmetRequired.add(ref.datasourceName);
            } else {
                LOG.warn("Data source '{}' did not complete — continuing (is_required=false for report '{}')",
                         ref.datasourceName, reportName);
            }
        }

        if (!unmetRequired.isEmpty()) {
            throw new PipelineException(PipelineException.Reason.ABORTED_REQUIRED_DATASOURCE,
                options.getReportName(), options.getReportSubprocess(), options.getPeriodId(),
                "PIPELINE aborted before report '" + reportName + "': required data "
                + "source(s) not COMPLETED: " + unmetRequired);
        }
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
