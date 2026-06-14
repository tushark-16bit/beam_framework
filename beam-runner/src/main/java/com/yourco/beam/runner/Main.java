package com.yourco.beam.runner;

import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Beam Pipeline Framework.
 *
 * <h2>Routing by process type</h2>
 * <pre>
 *   --processType=DATA_SOURCE_DOWNLOAD  →  DataSourcePipelineFactory
 *   --processType=REPORT_PROCESSING     →  PipelineFactory (general-purpose factory)
 * </pre>
 *
 * <h2>DATA_SOURCE_DOWNLOAD lifecycle</h2>
 * <pre>
 *   1. DataSourcePipelineFactory.assemble()
 *        ├─ Validate params in DB
 *        ├─ Fetch source configs (each carrying outputConfig, transforms, validationConfig)
 *        ├─ Filter by checkpoint (skip already-finished sources)
 *        ├─ Write STARTED checkpoints + PENDING process_status rows
 *        └─ Assemble independent per-source Beam branches (no merge)
 *   2. pipeline.run().waitUntilFinish()
 *   3. DataSourcePipelineFactory.runPostPipelineSteps()
 *        ├─ On success: validate output (row count, BnC) per source
 *        └─ Write FINISHED/FAILED checkpoints + COMPLETED/VALIDATION_FAILED/FAILED status
 * </pre>
 *
 * <h2>REPORT_PROCESSING lifecycle</h2>
 * <pre>
 *   1. PipelineFactory.assemble()     — assembles source → transform chain → sink
 *   2. pipeline.run()
 *   3. waitUntilFinish() for batch; streaming runs indefinitely until cancelled
 * </pre>
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOG.info("Starting Beam Pipeline Framework");

        FrameworkOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(FrameworkOptions.class);

        LOG.info("Process type: {}", options.getProcessType());
        LOG.info("Job run ID:   {}", options.getJobRunId());

        switch (options.getProcessType()) {
            case DATA_SOURCE_DOWNLOAD -> runDataSourceDownload(options);
            case REPORT_PROCESSING    -> runReportProcessing(options);
        }
    }

    // ── DATA_SOURCE_DOWNLOAD ─────────────────────────────────────────────────

    private static void runDataSourceDownload(FrameworkOptions options) {
        LOG.info("DATA_SOURCE_DOWNLOAD | datasource={} | period={} | periodStart={} | periodEnd={}",
                 options.getDatasourceName(), options.getPeriodId(),
                 options.getPeriodStart(), options.getPeriodEnd());

        DataSourcePipelineFactory factory = new DataSourcePipelineFactory();
        Pipeline pipeline = factory.assemble(options);

        LOG.info("Submitting to runner: {}", options.getRunner().getSimpleName());
        PipelineResult result = pipeline.run();

        PipelineResult.State finalState = PipelineResult.State.UNKNOWN;
        Throwable pipelineError = null;

        try {
            result.waitUntilFinish();
            finalState = result.getState();
            LOG.info("Pipeline finished with state: {}", finalState);
        } catch (Exception e) {
            pipelineError = e;
            LOG.error("Pipeline run threw exception: {}", e.getMessage(), e);
            try {
                finalState = result.getState();
            } catch (Exception ignored) {}
        }

        // Post-pipeline: validate output tables, write final checkpoints + status rows.
        // This runs regardless of success/failure — the factory handles each case.
        try {
            factory.runPostPipelineSteps(finalState, pipelineError);
        } catch (Exception e) {
            // Best-effort — don't mask a pipeline failure with a status-write failure
            LOG.error("Post-pipeline steps failed (status rows may be incomplete): {}", e.getMessage(), e);
        }

        if (pipelineError != null) {
            throw new RuntimeException("DATA_SOURCE_DOWNLOAD pipeline failed", pipelineError);
        }
    }

    // ── REPORT_PROCESSING ────────────────────────────────────────────────────

    /**
     * Routes REPORT_PROCESSING to one of two modes:
     * <ul>
     *   <li>When {@code --reportName} is set: uses {@link ReportPipelineFactory} which
     *       reads full report configuration from the parameter DB and orchestrates BQ
     *       jobs + email sending in the driver JVM. No Beam pipeline is submitted.</li>
     *   <li>When {@code --reportName} is blank: falls back to the generic
     *       {@link PipelineFactory} (source → transform chain → sink Beam pipeline).</li>
     * </ul>
     */
    private static void runReportProcessing(FrameworkOptions options) {
        String reportName = options.getReportName();
        if (reportName != null && !reportName.isBlank()) {
            LOG.info("REPORT_PROCESSING (DB-configured) | report={} subprocess={} period={}",
                     reportName, options.getReportSubprocess(), options.getPeriodId());
            new ReportPipelineFactory().execute(options);
            return;
        }

        LOG.info("REPORT_PROCESSING (legacy transform-chain) | source={} | chain={} | sink={}",
                 options.getSourceType(), options.getTransformChain(), options.getSinkType());

        Pipeline pipeline = new PipelineFactory().assemble(options);

        LOG.info("Submitting to runner: {}", options.getRunner().getSimpleName());
        PipelineResult result = pipeline.run();

        if (isBatchSource(options.getSourceType())) {
            result.waitUntilFinish();
            LOG.info("Pipeline finished with state: {}", result.getState());
        } else {
            LOG.info("Streaming pipeline submitted. Job running indefinitely until cancelled.");
        }
    }

    // ── Helper ───────────────────────────────────────────────────────────────

    /** Returns {@code true} for bounded sources; false for streaming. */
    private static boolean isBatchSource(SourceType sourceType) {
        if (sourceType == null) return true;
        return switch (sourceType) {
            case GCS, BQ, API, FILE -> true;
            case PUBSUB              -> false;
        };
    }
}
