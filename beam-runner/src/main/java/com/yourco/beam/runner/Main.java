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
 *   --processType=DATA_SOURCE_DOWNLOAD  →  DataSourcePipelineFactory (submits, then returns
 *                                            immediately — see below)
 *   --processType=REPORT_PROCESSING     →  PipelineFactory (general-purpose factory)
 *   --processType=PIPELINE              →  PipelineSequenceFactory (same --reportName/
 *                                            --reportSubprocess as REPORT_PROCESSING; wires the
 *                                            report step onto the SAME batched pipeline as a
 *                                            worker-side step gated on {@code Wait.on()}, submits
 *                                            once, then returns immediately)
 *   --processType=STATUS_CHECK          →  DataSourceStatusChecker (fast DB-only readiness poll;
 *                                            optional diagnostic only — see below)
 * </pre>
 *
 * <h2>Why {@code main()} never blocks on a submitted job — the Flex Template launch contract</h2>
 * This deployment launches via a Dataflow Flex Template: the launcher process (this
 * {@code main()}) is expected to build the pipeline graph, call {@code pipeline.run()}, and exit
 * promptly — the Dataflow <em>launch</em> operation is considered complete once the launcher
 * process exits, not once the submitted job itself finishes. Any blocking call in {@code main()}
 * after {@code pipeline.run()} — whether {@code PipelineResult.waitUntilFinish()} (which
 * specifically does not work on this platform anyway) or a hand-rolled poll loop re-reading
 * {@code DaRefer} — breaks the launch itself: Airflow observes the launch never completing and
 * times out at the graph level, even though the actual Dataflow job may still be running (or may
 * even have finished) behind the scenes.
 *
 * <p>So {@code DATA_SOURCE_DOWNLOAD} and {@code PIPELINE} both submit and return immediately.
 * Everything that used to block the driver JVM — waiting for a datasource to finish, running the
 * report, sending completion/failure email — now happens <em>inside the same Beam pipeline</em>,
 * as worker-side steps: {@link PostDownloadFinalizeTransform} for datasource finalization
 * (unchanged), and {@link ReportFinalizeTransform} for the report step of a {@code PIPELINE} run,
 * gated on the datasource branches' completion via {@code Wait.on()} — a Beam data-dependency
 * barrier, not a sleep loop. {@code REPORT_PROCESSING} (DB-configured, {@code --reportName} set
 * with no {@code PIPELINE}) was never affected either way — it never submits a Beam pipeline, so
 * there was never anything to wait for.
 *
 * <p>{@code STATUS_CHECK} still exists as an optional, non-blocking, single-check diagnostic
 * (useful for an ops dashboard or a manual look) — it was never part of the blocking design and
 * remains a plain one-shot DB read.
 *
 * <h2>DATA_SOURCE_DOWNLOAD lifecycle</h2>
 * <pre>
 *   1. DataSourcePipelineFactory.assemble()
 *        ├─ Validate params in BQ (parameter_store row present)
 *        ├─ Fetch source configs (transforms, validationConfig)
 *        ├─ Skip sources already COMPLETED in DaRefer (unless --manualOverrun)
 *        ├─ Insert DaRefer row sta_cd=LOADING → returns da_id per source
 *        └─ Assemble per-source Beam branches:
 *               source read → transform chain → DataSourceRecordSinkTransform (streaming inserts)
 *                                                         ↓
 *                                             PostDownloadFinalizeTransform
 *                                   (BnC validation + checkpoint update + email — in worker;
 *                                    emits a signal element either way, for Wait.on() gating)
 *   2. pipeline.run() — submitted. main() returns immediately; the rest happens on workers.
 * </pre>
 *
 * <h2>REPORT_PROCESSING lifecycle (DB-configured)</h2>
 * <pre>
 *   ReportPipelineFactory.execute() — runs entirely in the driver JVM (no Beam workers, so none of
 *   the above applies here — everything is a synchronous BQ job):
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
 * it. The original exception is always rethrown afterward, unchanged. This only covers failures
 * that happen synchronously in the driver JVM (config/assembly errors, or a submit-phase
 * failure) — a failure discovered only after {@code pipeline.run()} returns (e.g. a datasource or
 * report failing on a worker) never reaches this catch block at all, since {@code main()} has
 * already returned by then; {@link ReportFinalizeTransform} calls {@link FailureNotifier} itself,
 * from inside the worker, for that case.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * Process exit code for {@code --processType=STATUS_CHECK} when the watched work is still
     * in progress (DaRefer sta_cd=LOADING, or no row yet) — distinct from 0 (ready) and the JVM
     * default non-zero (an uncaught exception — terminal failure, already notified). A caller
     * using {@code STATUS_CHECK} as a one-shot diagnostic should treat this code, and only this
     * code, as "not yet — check again later", never as an error.
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

    /**
     * Submits the job and returns immediately — satisfying the Flex Template launch contract
     * (see class javadoc). Finalization (row/BnC validation, checkpoint update, failure email)
     * happens on the worker, inside {@link PostDownloadFinalizeTransform}, as the last step of
     * each source branch; nothing here waits for it.
     */
    private static void runDataSourceDownload(FrameworkOptions options) {
        LOG.info("DATA_SOURCE_DOWNLOAD | datasource={} | period={} | periodStart={} | periodEnd={}",
                 options.getDatasourceName(), options.getPeriodId(),
                 options.getPeriodStart(), options.getPeriodEnd());

        DataSourcePipelineFactory factory = new DataSourcePipelineFactory();
        DataSourceAssembly assembly = factory.assemble(options);
        if (assembly.isEmpty()) {
            // Every source was skipped by its schedule (not yet eligible, expired, non-business
            // day, not evaluable) or is already COMPLETED — nothing to submit.
            LOG.info("DATA_SOURCE_DOWNLOAD: nothing to run for datasource={} — no job submitted",
                     options.getDatasourceName());
            return;
        }

        LOG.info("Submitting to runner: {}", options.getRunner().getSimpleName());
        assembly.pipeline.run();

        LOG.info("DATA_SOURCE_DOWNLOAD job submitted: datasource={} — finalization runs on the "
                 + "worker; main() returns now", options.getDatasourceName());
    }

    // ── STATUS_CHECK ─────────────────────────────────────────────────────────

    /**
     * Fast, synchronous, DB-only readiness poll — see {@link DataSourceStatusChecker} and
     * {@link ProcessType#STATUS_CHECK}. Never blocks or sleeps; one invocation is one check.
     * Optional diagnostic — {@code DATA_SOURCE_DOWNLOAD}/{@code PIPELINE} no longer need an
     * external caller to invoke this, since they poll internally now.
     *
     * <p>{@code --reportName} set → checks every datasource a report's own
     * {@code ReportConfig.datasources[]} declares (the {@code PIPELINE} readiness gate).
     * {@code --reportName} blank → checks the single {@code --datasourceName}/
     * {@code --subprocessName}/{@code --periodId} (the standalone {@code DATA_SOURCE_DOWNLOAD}
     * readiness gate).
     *
     * <p>Exit codes: {@code 0} — ready. {@link #STATUS_PENDING_EXIT_CODE} — not yet finished;
     * this is not an error and never triggers {@link FailureNotifier}. Any other (JVM-default)
     * non-zero exit — a terminal failure was observed in DaRefer; {@link DataSourceStatusChecker}
     * already threw the typed exception, which this method lets propagate up to {@code main()}'s
     * catch block so {@link FailureNotifier} fires exactly as it would for a synchronous failure.
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

    /**
     * Wires the report step onto the same batched-datasource pipeline (gated on {@code Wait.on()}
     * via {@link ReportFinalizeTransform}), submits once, and returns immediately — the report
     * itself, and its completion/failure email, run on a worker after the datasource branches
     * finish. See {@link PipelineSequenceFactory}.
     */
    private static void runPipelineSequence(FrameworkOptions options) {
        LOG.info("PIPELINE | report={} subprocess={} period={}",
                 options.getReportName(), options.getReportSubprocess(), options.getPeriodId());
        new PipelineSequenceFactory().execute(options);
    }
}
