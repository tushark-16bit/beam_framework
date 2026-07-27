package com.yourco.beam.runner;

import com.yourco.beam.options.FrameworkOptions;
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
 *        ├─ Resolve MSTR_Per row for --periodId
 *        ├─ Validate params in BQ (parameter_store row present)
 *        ├─ Fetch source configs (transforms, validationConfig)
 *        ├─ Skip sources already COMPLETED in DaRefer (unless --overrideDownload)
 *        ├─ Insert DaRefer row sta_cd=LOADING → returns da_id per source
 *        └─ Assemble per-source Beam branches → rows written to DaRec as row_da_json_tx JSON
 *   2. pipeline.run().waitUntilFinish()
 *   3. DataSourcePipelineFactory.runPostPipelineSteps()
 *        ├─ COUNT(*) FROM DaRec WHERE da_id=X; SUM BnC fields
 *        └─ UPDATE DaRefer sta_cd → COMPLETED / FAILED_BNC / FAILED
 * </pre>
 *
 * <h2>REPORT_PROCESSING lifecycle (DB-configured)</h2>
 * <pre>
 *   ReportPipelineFactory.execute() — runs entirely in the driver JVM (no Beam workers):
 *        ├─ Load ReportConfig from BQ (parameter_store)
 *        ├─ Insert RptRefer row sta_cd=LOADING → returns rpt_id
 *        ├─ Run preprocessing steps              (BQ_QUERY jobs)
 *        ├─ Check all required datasources have DaRefer sta_cd=COMPLETED
 *        ├─ Add RptDaMap rows (rpt_id → da_id per datasource)
 *        ├─ Stage rows into RptStageDa (copied from DaRec per map_id)
 *        ├─ Run transform chain (BQ jobs, each materialised to a BQ table)
 *        ├─ Export outputs to GCS/BQ
 *        ├─ Insert RptOutput row per output; clear RptStageDa rows
 *        ├─ Send email (GCS outputs as attachments, if configured)
 *        └─ UPDATE RptRefer sta_cd → COMPLETED / FAILED
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
            case DATA_SOURCE_DOWNLOAD    -> runDataSourceDownload(options);
            case REPORT_PROCESSING       -> runReportProcessing(options);
            case POST_DOWNLOAD_VALIDATION -> runPostDownloadValidation(options);
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

        // Classic Template creation mode: pipeline.run() only serialised the graph to
        // --templateLocation. No workers ran. waitUntilFinish() would throw
        // UnsupportedOperationException ("The result of template creation cannot be used
        // to monitor the job"). Skip it entirely — post-pipeline steps run via a separate
        // Airflow task using --processType=POST_DOWNLOAD_VALIDATION after job completion.
        if (isTemplateCreationMode(options)) {
            LOG.info("Classic Template created at {}. "
                   + "Airflow DAG must: (1) create LOADING row via pre-setup task, "
                   + "(2) launch template with --daId=<N>, "
                   + "(3) wait for job via DataflowJobStateSensor, "
                   + "(4) run --processType=POST_DOWNLOAD_VALIDATION --daId=<N>.",
                   options.getTemplateLocation());
            return;
        }

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

    // ── POST_DOWNLOAD_VALIDATION ─────────────────────────────────────────────

    private static void runPostDownloadValidation(FrameworkOptions options) {
        LOG.info("POST_DOWNLOAD_VALIDATION | datasource={} | period={} | daId={}",
                 options.getDatasourceName(), options.getPeriodId(), options.getDaId());
        new DataSourcePipelineFactory().runPostPipelineValidation(options);
    }

    // ── Template detection ───────────────────────────────────────────────────

    private static boolean isTemplateCreationMode(FrameworkOptions options) {
        String loc = options.getTemplateLocation();
        return loc != null && !loc.isBlank();
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

        LOG.info("REPORT_PROCESSING (legacy transform-chain)");

        PipelineFactory factory = new PipelineFactory();
        Pipeline pipeline = factory.assemble(options);

        LOG.info("Submitting to runner: {}", options.getRunner().getSimpleName());
        PipelineResult result = pipeline.run();

        if (!factory.isStreamingSource()) {
            result.waitUntilFinish();
            LOG.info("Pipeline finished with state: {}", result.getState());
        } else {
            LOG.info("Streaming pipeline submitted. Job running indefinitely until cancelled.");
        }
    }
}
