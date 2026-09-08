package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.exception.PipelineException;
import com.yourco.beam.exception.ReportProcessingException;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ProcessType.PIPELINE_SYNC} — the one call chain in this framework that blocks: it
 * composes {@code PIPELINE}'s submit, {@code STATUS_CHECK}'s readiness poll (looped, with sleeps,
 * instead of a single check), and {@code REPORT_PROCESSING}'s report run into a single
 * invocation, the way {@code PipelineSequenceFactory} used to before this platform's
 * {@code waitUntilFinish()} restriction forced it to split into three separate calls.
 *
 * <p><b>Only invoke this from a context that can tolerate a long-running process</b> — a VM, a
 * long-timeout batch job. It is the opposite of {@code PIPELINE}/{@code STATUS_CHECK}, which are
 * designed to submit/check and return in milliseconds. Do not put this behind the same
 * short-lived trigger (e.g. a quick Airflow operator invocation with a tight timeout) that
 * {@code PIPELINE} is designed for — see {@code ProcessType.PIPELINE_SYNC}'s javadoc.
 *
 * <h2>Execution</h2>
 * <pre>
 *   PipelineSyncRunner.execute(options)
 *   ├─ 1. PipelineSequenceFactory.execute(options)     submit the batched datasource job, returns fast
 *   ├─ 2. awaitReady(options)                          loop: DataSourceStatusChecker.checkPipeline()
 *   │        every --pipelineSyncPollIntervalSeconds, until READY or --pipelineSyncTimeoutMinutes
 *   │        elapses (→ throws PipelineException(TIMEOUT))
 *   └─ 3. ReportPipelineFactory.execute(options)       run the report, unchanged
 * </pre>
 *
 * <p>Composes the existing {@link PipelineSequenceFactory}, {@link DataSourceStatusChecker}, and
 * {@link ReportPipelineFactory} rather than reimplementing any of them — this class only adds the
 * sleep loop between steps 1 and 3, and the timeout.
 *
 * <p>Exception propagation follows the same rule the rest of this exception hierarchy uses: a
 * {@link DataSourceDownloadException}, {@link ReportProcessingException}, or
 * {@link PipelineException} raised by any composed step propagates <b>unchanged</b>; anything
 * else becomes {@link PipelineException}({@code UNKNOWN}).
 */
final class PipelineSyncRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineSyncRunner.class);

    private PipelineSyncRunner() {}

    static void execute(FrameworkOptions options) {
        String reportName       = options.getReportName();
        String reportSubprocess = options.getReportSubprocess();
        int    periodId         = options.getPeriodId();
        LOG.info("PIPELINE_SYNC | report={} subprocess={} period={} | pollInterval={}s timeout={}m",
                 reportName, reportSubprocess, periodId,
                 options.getPipelineSyncPollIntervalSeconds(), options.getPipelineSyncTimeoutMinutes());

        try {
            new PipelineSequenceFactory().execute(options);
            awaitReady(options);
            new ReportPipelineFactory().execute(options);
        } catch (DataSourceDownloadException | ReportProcessingException | PipelineException e) {
            throw e;
        } catch (Exception e) {
            throw PipelineException.wrap(PipelineException.Reason.UNKNOWN,
                reportName, reportSubprocess, periodId, e);
        }

        LOG.info("PIPELINE_SYNC completed: report={}", reportName);
    }

    private static void awaitReady(FrameworkOptions options) {
        DataSourceStatusChecker checker = new DataSourceStatusChecker();
        long intervalMillis = options.getPipelineSyncPollIntervalSeconds() * 1000L;
        long deadlineMillis = System.currentTimeMillis()
            + options.getPipelineSyncTimeoutMinutes() * 60_000L;

        while (true) {
            // A terminal failure on a required datasource is thrown here as PipelineException
            // (ABORTED_REQUIRED_DATASOURCE) — let it propagate straight out of execute()'s catch.
            if (checker.checkPipeline(options) == DataSourceStatusChecker.Outcome.READY) {
                return;
            }
            if (System.currentTimeMillis() >= deadlineMillis) {
                throw new PipelineException(PipelineException.Reason.TIMEOUT,
                    options.getReportName(), options.getReportSubprocess(), options.getPeriodId(),
                    "PIPELINE_SYNC timed out after " + options.getPipelineSyncTimeoutMinutes()
                    + " minute(s) waiting for required datasource(s) to reach COMPLETED");
            }
            sleep(intervalMillis);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("PIPELINE_SYNC poll loop interrupted", e);
        }
    }
}
