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
 *   --processType=DATA_SOURCE_DOWNLOAD  →  DataSourcePipelineFactory (submits, does not wait)
 *   --processType=REPORT_PROCESSING     →  PipelineFactory (general-purpose factory)
 *   --processType=PIPELINE              →  PipelineSequenceFactory (same --reportName/
 *                                            --reportSubprocess as REPORT_PROCESSING; submits one
 *                                            batched job for every not-yet-COMPLETED datasource
 *                                            the report's own datasources[] declares, then
 *                                            returns — does NOT wait and does NOT run the report)
 *   --processType=STATUS_CHECK          →  DataSourceStatusChecker (fast DB-only readiness poll;
 *                                            see below)
 *   --processType=PIPELINE_SYNC         →  PipelineSyncRunner (chains PIPELINE's submit +
 *                                            STATUS_CHECK's poll, looped with sleeps, + a
 *                                            REPORT_PROCESSING run into one BLOCKING call — only
 *                                            for a context that can tolerate a long-running
 *                                            process; see its own class javadoc)
 * </pre>
 *
 * <h2>Why nothing here calls {@code PipelineResult.waitUntilFinish()}</h2>
 * This framework's runner platform forbids it — the driver JVM that calls {@code pipeline.run()}
 * must submit and return quickly, not block for a job's full runtime. That makes job outcome
 * unobservable from this process synchronously, so {@code DATA_SOURCE_DOWNLOAD} and
 * {@code PIPELINE} both just submit and log, and completion/failure is discovered later by an
 * external poller (an Airflow sensor's poke loop) calling {@code --processType=STATUS_CHECK}
 * repeatedly against this same JAR. {@code PIPELINE_SYNC} is the one exception: it blocks
 * in-process instead, using its own sleep loop (never {@code waitUntilFinish()}) around repeated
 * {@code DataSourceStatusChecker.checkPipeline()} calls — appropriate only where the invoking
 * context itself can tolerate that. None of this weakens failure handling: the worker-side
 * {@code PostDownloadFinalizeTransform} still writes {@code DaRefer}'s terminal status
 * (COMPLETED / FAILED_BNC / FAILED_TRANSFORM / FAILED) exactly as before regardless of whether
 * anything is watching, and {@link #runStatusCheck} (like {@code PipelineSyncRunner}'s internal
 * poll) throws the same typed exceptions a synchronous {@code waitUntilFinish()} failure used to
 * — so they still flow through this class's one catch block below into {@link FailureNotifier}.
 * Only the trigger for that catch moves from "blocking call threw" to "a status-check invocation
 * observed a terminal failure row in DaRefer".
 *
 * <h2>DATA_SOURCE_DOWNLOAD lifecycle</h2>
 * <pre>
 *   1. DataSourcePipelineFactory.assemble()
 *        ├─ Validate params in BQ (parameter_store row present)
 *        ├─ Fetch source configs (transforms, validationConfig)
 *        ├─ Skip sources already COMPLETED in DaRefer (unless --overrideDownload)
 *        ├─ Insert DaRefer row sta_cd=LOADING → returns da_id per source
 *        └─ Assemble per-source Beam branches:
 *               source read → transform chain → DataSourceRecordSinkTransform (streaming inserts)
 *                                                         ↓
 *                                             PostDownloadFinalizeTransform
 *                                   (BnC validation + checkpoint update + email — in worker)
 *   2. pipeline.run() — submitted, NOT awaited. This process returns immediately.
 *      Checkpoint (COMPLETED / FAILED_BNC / FAILED_TRANSFORM / FAILED) is written by the worker
 *      as the last step, independent of whether anything is still watching. Poll it via
 *      --processType=STATUS_CHECK.
 * </pre>
 *
 * <h2>REPORT_PROCESSING lifecycle (DB-configured)</h2>
 * <pre>
 *   ReportPipelineFactory.execute() — runs entirely in the driver JVM (no Beam workers, so the
 *   waitUntilFinish() restriction above does not apply here — everything is a synchronous BQ job):
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
 *
 * <h2>Failure handling</h2>
 * {@code main()} wraps the entire process-type dispatch in one catch. Each factory
 * ({@code DataSourcePipelineFactory}, {@code ReportPipelineFactory}, {@code PipelineSequenceFactory},
 * {@code DataSourceStatusChecker}) has already classified its own failure into
 * {@link com.yourco.beam.exception.DataSourceDownloadException},
 * {@link com.yourco.beam.exception.ReportProcessingException}, or
 * {@link com.yourco.beam.exception.PipelineException} by the time it reaches here — see each
 * class's own javadoc for exactly where and how. {@link FailureNotifier} picks a notification
 * template by exception type (a default template covers anything else — e.g. the legacy
 * {@code PipelineFactory} path, or a failure before any factory even ran), always logs it, and —
 * only if {@code --opsFailureEmail} is set and an {@code EmailSendUtility} is available — emails
 * it. The original exception is always rethrown afterward, unchanged.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * Process exit code for {@code --processType=STATUS_CHECK} when the watched work is still
     * in progress (DaRefer sta_cd=LOADING, or no row yet) — distinct from 0 (ready) and the JVM
     * default non-zero (an uncaught exception — terminal failure, already notified). An external
     * poller (e.g. an Airflow sensor's poke()) should treat this code, and only this code, as
     * "not yet — check again later", never as an error.
     */
    static final int STATUS_PENDING_EXIT_CODE = 75;

    public static void main(String[] args) {
        LOG.info("Starting Beam Pipeline Framework");

        FrameworkOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(FrameworkOptions.class);

        LOG.info("Process type: {}", options.getProcessType());
        LOG.info("Job run ID:   {}", options.getJobRunId());

        try {
            switch (options.getProcessType()) {
                case DATA_SOURCE_DOWNLOAD -> runDataSourceDownload(options);
                case REPORT_PROCESSING    -> runReportProcessing(options);
                case PIPELINE             -> runPipelineSequence(options);
                case STATUS_CHECK         -> runStatusCheck(options);
                case PIPELINE_SYNC        -> PipelineSyncRunner.execute(options);
            }
        } catch (Exception e) {
            // Single last-resort catch: DataSourcePipelineFactory/ReportPipelineFactory/
            // PipelineSequenceFactory have already classified their own failures into
            // DataSourceDownloadException/ReportProcessingException/PipelineException by the time
            // they reach here — FailureNotifier picks the matching template by type, plus a
            // default template for anything else (e.g. the legacy PipelineFactory path, or a
            // failure before any of those factories even ran). Rethrown unchanged afterward so
            // the process still exits non-zero exactly as it did before this handler existed.
            FailureNotifier.notify(options, e);
            throw e;
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
        pipeline.run();

        LOG.info("Pipeline submitted — this process does not wait for completion (platform "
                 + "restriction: waitUntilFinish() is not available). Poll readiness via "
                 + "--processType=STATUS_CHECK --datasourceName={} --subprocessName={} "
                 + "--periodId={}, or query DaRefer directly.",
                 options.getDatasourceName(), options.getSubprocessName(), options.getPeriodId());
    }

    // ── STATUS_CHECK ─────────────────────────────────────────────────────────

    /**
     * Fast, synchronous, DB-only readiness poll — see {@link DataSourceStatusChecker} and
     * {@link ProcessType#STATUS_CHECK}. Never blocks or sleeps; one invocation is one check.
     *
     * <p>{@code --reportName} set → checks every datasource a report's own
     * {@code ReportConfig.datasources[]} declares (the {@code PIPELINE} readiness gate).
     * {@code --reportName} blank → checks the single {@code --datasourceName}/
     * {@code --subprocessName}/{@code --periodId} (the standalone {@code DATA_SOURCE_DOWNLOAD}
     * readiness gate).
     *
     * <p>Exit codes: {@code 0} — ready, safe to proceed. {@link #STATUS_PENDING_EXIT_CODE} — not
     * yet finished, poll again later; this is not an error and never triggers
     * {@link FailureNotifier}. Any other (JVM-default) non-zero exit — a terminal failure was
     * observed in DaRefer; {@link DataSourceStatusChecker} already threw the typed exception,
     * which this method lets propagate up to {@code main()}'s catch block so
     * {@link FailureNotifier} fires exactly as it would for a synchronous failure.
     */
    private static void runStatusCheck(FrameworkOptions options) {
        LOG.info("STATUS_CHECK | report={} datasource={} period={}",
                 options.getReportName(), options.getDatasourceName(), options.getPeriodId());

        DataSourceStatusChecker checker = new DataSourceStatusChecker();
        boolean pipelineCheck = options.getReportName() != null && !options.getReportName().isBlank();
        DataSourceStatusChecker.Outcome outcome = pipelineCheck
            ? checker.checkPipeline(options)
            : checker.checkSingle(options);

        if (outcome == DataSourceStatusChecker.Outcome.PENDING) {
            LOG.info("STATUS_CHECK: still pending — exiting {}", STATUS_PENDING_EXIT_CODE);
            System.exit(STATUS_PENDING_EXIT_CODE);
        }
        LOG.info("STATUS_CHECK: ready");
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

    // ── PIPELINE ─────────────────────────────────────────────────────────────

    private static void runPipelineSequence(FrameworkOptions options) {
        LOG.info("PIPELINE | report={} subprocess={} period={}",
                 options.getReportName(), options.getReportSubprocess(), options.getPeriodId());
        new PipelineSequenceFactory().execute(options);
    }

    // ── PIPELINE_SYNC ────────────────────────────────────────────────────────
    // No wrapper method here — PipelineSyncRunner.execute(options) is called directly from the
    // switch above. See its own class javadoc: this is the one BLOCKING call chain in the
    // framework (submit + poll loop + run the report), only appropriate from an invocation
    // context that can tolerate a long-running process.
}
