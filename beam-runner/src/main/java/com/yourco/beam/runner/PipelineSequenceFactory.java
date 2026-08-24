package com.yourco.beam.runner;

import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.config.BigQueryPipelineConfigRepository;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.model.DataSourceStep;
import com.yourco.beam.model.PipelineConfig;
import com.yourco.beam.model.PipelineStepConfig;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportStep;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles and runs a {@code PIPELINE} sequence: every {@code DATA_SOURCE} step not already
 * {@code COMPLETED} for the period, batched into one Dataflow job, followed by the terminal
 * {@code REPORT} step — always the last step, enforced by {@link PipelineConfig}.
 *
 * <h2>Required vs. optional data sources</h2>
 * A {@link DataSourceStep} carries no required/optional flag of its own. After the batched job
 * finishes, each step's completion is re-checked; for any still not {@code COMPLETED}, whether
 * that aborts the whole sequence is resolved by looking that datasource up in the terminal
 * report's own {@code ReportConfig.datasources[]} ({@link com.yourco.beam.model.ReportDatasourceRef#required}) —
 * the same flag {@code ReportPipelineFactory.checkDatasourceAvailability()} already enforces on
 * every report run, pipeline or standalone, so there is exactly one place "is this datasource
 * required" is declared. A datasource the report doesn't list at all is treated as not required
 * — the report never reads it, so it can't be required for that report by definition.
 *
 * <h2>Execution</h2>
 * <pre>
 *   PipelineSequenceFactory.execute(options)
 *   ├─ BigQueryPipelineConfigRepository.fetchPipelineConfig()   → ordered steps
 *   ├─ fetch SourceConfig for every DATA_SOURCE step (by name, via BigQuerySourceConfigRepository)
 *   ├─ DataSourcePipelineFactory.assembleForConfigs()           → ONE batched Dataflow job
 *   │     (internally skips any step already COMPLETED, same as standalone DATA_SOURCE_DOWNLOAD)
 *   ├─ pipeline.run().waitUntilFinish()
 *   ├─ re-check each DATA_SOURCE step's checkpoint status
 *   │     unmet + required (per the report's datasources[])   → abort, throw PipelineAbortedException
 *   │     unmet + not required                                 → log, continue
 *   └─ ReportPipelineFactory.execute()                          → terminal report, unchanged
 * </pre>
 *
 * <p>Composes the existing {@link DataSourcePipelineFactory} and {@link ReportPipelineFactory}
 * rather than reimplementing either — this class only decides which sources to batch together
 * and whether an incomplete one should stop the sequence before the report runs.
 *
 * <h2>{@code --manualOverrun}</h2>
 * Applies uniformly across the whole sequence, exactly as it does standalone, because the same
 * {@code options} instance — never a copy, never a reset field — is passed straight into both
 * {@link DataSourcePipelineFactory#assembleForConfigs} and {@link ReportPipelineFactory#execute}:
 * <ul>
 *   <li>Every {@code DATA_SOURCE} step bypasses its own {@code COMPLETED} skip-guard and
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

        String pipelineName       = options.getPipelineName();
        String pipelineSubprocess = options.getPipelineSubprocess();
        LOG.info("PIPELINE | pipeline={} subprocess={} period={}",
                 pipelineName, pipelineSubprocess, options.getPeriodId());
        if (options.getManualOverrun()) {
            LOG.info("--manualOverrun=true: every DATA_SOURCE step in this sequence will bypass "
                     + "its COMPLETED guard and re-download, superseding its previous run once "
                     + "complete — same as standalone DATA_SOURCE_DOWNLOAD. The terminal REPORT "
                     + "step always re-runs regardless (it has no COMPLETED guard of its own).");
        }

        BigQueryPipelineConfigRepository pipelineRepo = new BigQueryPipelineConfigRepository(options);
        PipelineConfig pipelineConfig = pipelineRepo.fetchPipelineConfig(pipelineName, pipelineSubprocess);

        List<DataSourceStep> dataSourceSteps = new ArrayList<>();
        for (PipelineStepConfig step : pipelineConfig.steps()) {
            if (step instanceof DataSourceStep ds) {
                dataSourceSteps.add(ds);
            }
        }
        // PipelineConfig's compact constructor guarantees the last step is a ReportStep.
        ReportStep reportStep = (ReportStep) pipelineConfig.steps().get(pipelineConfig.steps().size() - 1);

        runDataSourceSteps(options, dataSourceSteps);
        checkRequiredDataSources(options, dataSourceSteps, reportStep);
        runReportStep(options, reportStep);

        LOG.info("PIPELINE completed: pipeline={} → report={}", pipelineName, reportStep.reportName());
    }

    // ── Phase 1: batched data-source job ────────────────────────────────────

    private void runDataSourceSteps(FrameworkOptions options, List<DataSourceStep> dataSourceSteps) {
        if (dataSourceSteps.isEmpty()) {
            LOG.info("No DATA_SOURCE steps in this pipeline — proceeding straight to the report");
            return;
        }

        BigQuerySourceConfigRepository sourceRepo = new BigQuerySourceConfigRepository(options);
        List<SourceConfig> sourceConfigs = new ArrayList<>();
        for (DataSourceStep step : dataSourceSteps) {
            sourceConfigs.addAll(sourceRepo.fetchSourceConfigs(
                options.getParentId(), step.datasourceName(), step.subprocessName(), options.getPeriodId()));
        }

        DataSourcePipelineFactory dsFactory = new DataSourcePipelineFactory();
        Pipeline pipeline = dsFactory.assembleForConfigs(options, sourceConfigs);

        LOG.info("Submitting batched data-source job ({} step(s)) to runner: {}",
                 dataSourceSteps.size(), options.getRunner().getSimpleName());
        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
            LOG.info("Batched data-source job finished with state: {}", result.getState());
        } catch (Exception e) {
            LOG.error("Batched data-source job threw exception: {}", e.getMessage(), e);
            throw new RuntimeException("PIPELINE data-source phase failed", e);
        }
    }

    // ── Phase 2: required/optional gate ─────────────────────────────────────

    private void checkRequiredDataSources(FrameworkOptions options, List<DataSourceStep> dataSourceSteps,
                                          ReportStep reportStep) {
        if (dataSourceSteps.isEmpty()) return;

        BigQueryDataSourceCheckpointAdapter checkpointAdapter =
            new BigQueryDataSourceCheckpointAdapter(options);
        BigQueryReportRepository reportRepo = new BigQueryReportRepository(options);
        ReportConfig reportConfig = reportRepo.fetchReportConfig(
            reportStep.reportName(), reportStep.reportSubprocess(), options.getPeriodId());

        List<String> unmetRequired = new ArrayList<>();
        for (DataSourceStep step : dataSourceSteps) {
            if (checkpointAdapter.isCompleted(step.datasourceName(), options.getPeriodId())) continue;

            boolean required = reportConfig.datasources.stream()
                .filter(ref -> ref.datasourceName.equals(step.datasourceName()))
                .findFirst()
                .map(ref -> ref.required)
                .orElse(false);

            if (required) {
                unmetRequired.add(step.datasourceName());
            } else {
                LOG.warn("Data source '{}' did not complete — continuing (not required by report '{}')",
                         step.datasourceName(), reportStep.reportName());
            }
        }

        if (!unmetRequired.isEmpty()) {
            throw new PipelineAbortedException(
                "PIPELINE aborted before report '" + reportStep.reportName() + "': required data "
                + "source(s) not COMPLETED: " + unmetRequired);
        }
    }

    // ── Phase 3: terminal report ────────────────────────────────────────────

    private void runReportStep(FrameworkOptions options, ReportStep reportStep) {
        options.setReportName(reportStep.reportName());
        options.setReportSubprocess(reportStep.reportSubprocess());
        new ReportPipelineFactory().execute(options);
    }

    // ── Validation ───────────────────────────────────────────────────────────

    private static void validateRequiredParameters(FrameworkOptions options) {
        if (options.getPipelineName() == null || options.getPipelineName().isBlank()) {
            throw new PipelineConfigurationException("--pipelineName is required for PIPELINE");
        }
        if (options.getPeriodId() <= 0) {
            throw new PipelineConfigurationException("--periodId is required for PIPELINE");
        }
    }

    // ── Exception types ──────────────────────────────────────────────────────

    public static final class PipelineConfigurationException extends RuntimeException {
        public PipelineConfigurationException(String message) { super(message); }
    }

    /** Thrown when a required (per the terminal report's own datasources[]) step never completed. */
    public static final class PipelineAbortedException extends RuntimeException {
        public PipelineAbortedException(String message) { super(message); }
    }
}
