package com.yourco.beam.runner;

import com.yourco.beam.exception.PipelineException;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.RunDates;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Wires the REPORT_PROCESSING step of a {@code PIPELINE} run onto the SAME batched Dataflow job
 * as its datasource branches — as a worker-side step gated on {@code Wait.on()}, not a
 * driver-JVM poll loop.
 *
 * <h2>Why this exists</h2>
 * This deployment launches via a Dataflow Flex Template, whose launch contract requires
 * {@code main()} to submit the pipeline and exit promptly — the launch is considered complete
 * once the launcher process exits, not once the job finishes. A driver-JVM poll loop (the
 * previous design) or {@code PipelineResult.waitUntilFinish()} both violate that contract, and
 * were the underlying cause of a real incident: Airflow observed the launch never completing and
 * timed out at the graph level, independent of whether the Dataflow job itself succeeded.
 *
 * <p>The fix moves the wait, the report, and its completion/failure email all onto the worker:
 * {@link #wire} adds one more step to the pipeline that only runs once every batched datasource
 * branch's {@link PostDownloadFinalizeTransform} signal has been produced — a direct Beam
 * data-dependency barrier ({@code Wait.on()}), not a timed poll. {@code main()} calls
 * {@link Pipeline#run} once, after wiring, and returns immediately.
 *
 * <h2>Execution</h2>
 * <pre>
 *   Create.of(1) → Wait.on(finalizeSignals) → ReportRunDoFn
 *                                                ├─ verifyRequiredDatasources()  (one-shot, no
 *                                                │    retry/timeout — Wait.on() already
 *                                                │    guarantees every branch reached a terminal
 *                                                │    state; this just checks it was COMPLETED)
 *                                                ├─ ReportPipelineFactory.execute(options, config, dates)
 *                                                │    (report + its own completion email)
 *                                                └─ on any exception: FailureNotifier.notify()
 *                                                     (caught here, not rethrown — this is the
 *                                                     last step; rethrowing would only trigger
 *                                                     Beam bundle retries with no benefit)
 * </pre>
 */
final class ReportFinalizeTransform {

    private static final Logger LOG = LoggerFactory.getLogger(ReportFinalizeTransform.class);

    private ReportFinalizeTransform() {}

    /**
     * Adds the report-run step to {@code pipeline}. Runs once every element of
     * {@code finalizeSignals} — one {@code PCollection<Long>} per batched datasource branch, from
     * {@link PostDownloadFinalizeTransform} — has been produced, whether that branch succeeded or
     * failed. If {@code finalizeSignals} is empty (the report declares no datasources), the report
     * step runs immediately with nothing to wait for.
     *
     * <p>{@code dates} is the report's {@link RunDates}, already resolved in the driver JVM at
     * submission — the worker never re-resolves them.
     */
    static void wire(Pipeline pipeline, List<PCollection<?>> finalizeSignals,
                     ReportConfig config, RunDates dates, FrameworkOptions options) {
        String project = options.getCheckpointBqProject() != null
                        && !options.getCheckpointBqProject().isBlank()
                        ? options.getCheckpointBqProject() : options.getProject();
        String daReferTableRef = "`" + project + "." + options.getCheckpointBqDataset()
                               + "." + options.getDaReferTable() + "`";

        PCollection<Integer> trigger = pipeline.apply(
            "ReportTrigger-" + config.reportName, Create.of(1));

        PCollection<Integer> gated = finalizeSignals.isEmpty()
            ? trigger
            : trigger.apply("WaitForDatasources-" + config.reportName, Wait.on(finalizeSignals));

        gated.apply("RunReport-" + config.reportName,
            ParDo.of(new ReportRunDoFn(config, dates, daReferTableRef)));
    }

    // ── Named DoFn — required for Beam serialization safety ──────────────────

    private static final class ReportRunDoFn extends DoFn<Integer, Void> {

        private static final long serialVersionUID = 1L;

        private final ReportConfig config;
        private final RunDates     dates;
        private final String       daReferTableRef;

        ReportRunDoFn(ReportConfig config, RunDates dates, String daReferTableRef) {
            this.config          = config;
            this.dates           = dates;
            this.daReferTableRef = daReferTableRef;
        }

        @ProcessElement
        public void processElement(@Element Integer trigger, PipelineOptions pipelineOptions) {
            // Beam injects the worker's own PipelineOptions here — reconstructing the
            // FrameworkOptions view this way avoids needing FrameworkOptions (not Serializable)
            // as a DoFn field, and avoids re-parsing raw CLI args.
            FrameworkOptions options = pipelineOptions.as(FrameworkOptions.class);
            LOG.info("Running report '{}' (subprocess={} period={}) — all batched datasource "
                     + "branch(es) have reached a terminal state",
                     config.reportName, config.reportSubprocess, dates.periodId);
            try {
                verifyRequiredDatasources(options);
                new ReportPipelineFactory().execute(options, config, dates);
                LOG.info("Report '{}' completed", config.reportName);
            } catch (Exception e) {
                LOG.error("Report '{}' failed: {}", config.reportName, e.getMessage(), e);
                FailureNotifier.notify(options, e);
            }
        }

        /**
         * One-shot check — no retry, no timeout, since {@code Wait.on()} already guarantees every
         * batched datasource branch reached a terminal checkpoint state before this DoFn runs.
         * Just confirms each required datasource's terminal state was actually {@code COMPLETED},
         * not one of the {@code FAILED*} states.
         */
        private void verifyRequiredDatasources(FrameworkOptions options) {
            BigQueryDataSourceCheckpointAdapter checkpointAdapter =
                new BigQueryDataSourceCheckpointAdapter(daReferTableRef);
            List<String> failedRequired = new ArrayList<>();
            for (ReportDatasourceRef ref : config.datasources) {
                if (!ref.required) continue;
                if (!checkpointAdapter.isCompleted(ref.datasourceName, dates.periodId)) {
                    failedRequired.add(ref.datasourceName);
                }
            }
            if (!failedRequired.isEmpty()) {
                throw new PipelineException(PipelineException.Reason.ABORTED_REQUIRED_DATASOURCE,
                    config.reportName, config.reportSubprocess, dates.periodId,
                    "PIPELINE required data source(s) did not reach COMPLETED, report '"
                    + config.reportName + "' will not run: " + failedRequired);
            }
        }
    }
}
