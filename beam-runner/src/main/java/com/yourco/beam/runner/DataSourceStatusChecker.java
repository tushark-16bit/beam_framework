package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.exception.PipelineException;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.model.DataSourceCheckpoint;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * DB-only readiness check against {@code DaRefer} — the replacement for
 * {@code PipelineResult.waitUntilFinish()} on a platform where blocking on a Beam
 * {@code PipelineResult} specifically does not work, even though the driver JVM itself is free to
 * block for as long as needed.
 *
 * <p>Reads {@code DaRefer} via {@link BigQueryDataSourceCheckpointAdapter#getLatest} — the exact
 * same row {@code PostDownloadFinalizeTransform} writes from inside the Beam worker as the last
 * step of each source branch — instead of observing a synchronous exception from
 * {@code PipelineResult}.
 *
 * <h2>Two layers</h2>
 * <ul>
 *   <li>{@link #checkSingle}/{@link #checkPipeline} — one BQ read (or a handful), return
 *       immediately, never sleep. This is {@code ProcessType.STATUS_CHECK}'s implementation — an
 *       optional diagnostic a caller can invoke standalone.</li>
 *   <li>{@link #awaitSingle}/{@link #awaitPipeline} — thin blocking wrappers: loop calling the
 *       check above, {@code Thread.sleep}ing {@code --jobPollIntervalSeconds} between attempts,
 *       until {@link Outcome#READY} or {@code --jobPollTimeoutMinutes} elapses. This is what
 *       {@code Main.runDataSourceDownload()} and {@code PipelineSequenceFactory.execute()} call —
 *       never {@code waitUntilFinish()}, just a plain sleep loop re-reading the DB.</li>
 * </ul>
 *
 * <h2>Outcome contract</h2>
 * <ul>
 *   <li>{@link Outcome#READY} — every relevant datasource reached {@code COMPLETED}.</li>
 *   <li>{@link Outcome#PENDING} — still {@code LOADING} (or no row yet). Never thrown as an
 *       exception from the check methods — a still-running job is not a failure, just not
 *       finished yet; the await methods are what turn a too-long PENDING streak into a
 *       {@code TIMEOUT}.</li>
 *   <li>A terminal failure ({@code FAILED} / {@code FAILED_BNC} / {@code FAILED_TRANSFORM} on a
 *       required datasource) is <b>thrown</b>, not returned — as
 *       {@link DataSourceDownloadException} ({@link #checkSingle}) or {@link PipelineException}
 *       ({@link #checkPipeline}) — so it flows through {@code Main.main()}'s existing catch →
 *       {@code FailureNotifier} → {@code EmailSendUtility} path exactly like a synchronous
 *       failure always has.</li>
 * </ul>
 */
final class DataSourceStatusChecker {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceStatusChecker.class);

    enum Outcome { READY, PENDING }

    /**
     * Standalone {@code DATA_SOURCE_DOWNLOAD} readiness check for a single
     * {@code --datasourceName}/{@code --subprocessName}/{@code --periodId}. One BQ read, returns
     * immediately.
     */
    Outcome checkSingle(FrameworkOptions options) {
        BigQueryDataSourceCheckpointAdapter checkpointAdapter =
            new BigQueryDataSourceCheckpointAdapter(options);
        Optional<DataSourceCheckpoint> latest =
            checkpointAdapter.getLatest(options.getDatasourceName(), options.getPeriodId());

        if (latest.isEmpty() || DataSourceCheckpoint.STA_LOADING.equals(latest.get().staCd)) {
            LOG.info("'{}' (period={}): still pending",
                     options.getDatasourceName(), options.getPeriodId());
            return Outcome.PENDING;
        }

        DataSourceCheckpoint checkpoint = latest.get();
        if (DataSourceCheckpoint.STA_COMPLETED.equals(checkpoint.staCd)) {
            LOG.info("'{}' (period={}): COMPLETED (da_id={})",
                     options.getDatasourceName(), options.getPeriodId(), checkpoint.daId);
            return Outcome.READY;
        }

        throw new DataSourceDownloadException(DataSourceDownloadException.Reason.JOB_FAILURE,
            options.getDatasourceName(), options.getSubprocessName(), options.getPeriodId(),
            "DATA_SOURCE_DOWNLOAD failed: da_id=" + checkpoint.daId + " sta_cd=" + checkpoint.staCd
            + (checkpoint.balAndCntlSmryTx != null ? " balAndCntlSmryTx=" + checkpoint.balAndCntlSmryTx : ""),
            null);
    }

    /**
     * Blocks until the single datasource reaches {@code COMPLETED}, or throws — a terminal
     * failure ({@link #checkSingle} throwing), or {@link DataSourceDownloadException.Reason#TIMEOUT}
     * once {@code --jobPollTimeoutMinutes} elapses. Sleeps {@code --jobPollIntervalSeconds}
     * between checks. Never calls {@code waitUntilFinish()} — this is a plain sleep loop around
     * {@link #checkSingle}'s DB read.
     */
    void awaitSingle(FrameworkOptions options) {
        long intervalMillis = options.getJobPollIntervalSeconds() * 1000L;
        long deadlineMillis = System.currentTimeMillis() + options.getJobPollTimeoutMinutes() * 60_000L;

        while (true) {
            if (checkSingle(options) == Outcome.READY) {
                return;
            }
            if (System.currentTimeMillis() >= deadlineMillis) {
                throw new DataSourceDownloadException(DataSourceDownloadException.Reason.TIMEOUT,
                    options.getDatasourceName(), options.getSubprocessName(), options.getPeriodId(),
                    "DATA_SOURCE_DOWNLOAD timed out after " + options.getJobPollTimeoutMinutes()
                    + " minute(s) waiting for da_id to reach COMPLETED", null);
            }
            sleep(intervalMillis);
        }
    }

    /**
     * {@code PIPELINE} readiness check: every datasource the report's own
     * {@code ReportConfig.datasources[]} declares. One BQ read per datasource, returns
     * immediately.
     */
    Outcome checkPipeline(FrameworkOptions options) {
        String reportName       = options.getReportName();
        String reportSubprocess = options.getReportSubprocess();
        int    periodId         = options.getPeriodId();

        ReportConfig reportConfig;
        try {
            BigQueryReportRepository reportRepo = new BigQueryReportRepository(options);
            reportConfig = reportRepo.fetchReportConfig(reportName, reportSubprocess, periodId);
        } catch (Exception e) {
            throw PipelineException.wrap(PipelineException.Reason.CONFIG_NOT_FOUND,
                reportName, reportSubprocess, periodId, e);
        }

        List<ReportDatasourceRef> datasources = reportConfig.datasources;
        if (datasources.isEmpty()) {
            LOG.info("report='{}': declares no datasources — ready", reportName);
            return Outcome.READY;
        }

        BigQueryDataSourceCheckpointAdapter checkpointAdapter =
            new BigQueryDataSourceCheckpointAdapter(options);
        List<String> failedRequired = new ArrayList<>();
        boolean anyRequiredPending  = false;

        for (ReportDatasourceRef ref : datasources) {
            Optional<DataSourceCheckpoint> latest =
                checkpointAdapter.getLatest(ref.datasourceName, periodId);
            String staCd = latest.map(c -> c.staCd).orElse(DataSourceCheckpoint.STA_LOADING);

            if (DataSourceCheckpoint.STA_COMPLETED.equals(staCd)) {
                continue;
            }
            boolean pending = latest.isEmpty() || DataSourceCheckpoint.STA_LOADING.equals(staCd);
            if (!ref.required) {
                LOG.warn("report='{}': optional datasource '{}' is {} — not blocking",
                         reportName, ref.datasourceName, staCd);
                continue;
            }
            if (pending) {
                anyRequiredPending = true;
            } else {
                failedRequired.add(ref.datasourceName + " (" + staCd + ")");
            }
        }

        if (!failedRequired.isEmpty()) {
            throw new PipelineException(PipelineException.Reason.ABORTED_REQUIRED_DATASOURCE,
                reportName, reportSubprocess, periodId,
                "PIPELINE required data source(s) failed, report '" + reportName
                + "' will not run: " + failedRequired);
        }
        if (anyRequiredPending) {
            LOG.info("report='{}': required datasource(s) still pending", reportName);
            return Outcome.PENDING;
        }

        LOG.info("report='{}': all required datasource(s) COMPLETED", reportName);
        return Outcome.READY;
    }

    /**
     * Blocks until every required datasource reaches {@code COMPLETED}, or throws — a required
     * datasource's terminal failure ({@link #checkPipeline} throwing
     * {@link PipelineException.Reason#ABORTED_REQUIRED_DATASOURCE}), or
     * {@link PipelineException.Reason#TIMEOUT} once {@code --jobPollTimeoutMinutes} elapses.
     * Sleeps {@code --jobPollIntervalSeconds} between checks. Never calls
     * {@code waitUntilFinish()} — this is a plain sleep loop around {@link #checkPipeline}'s DB
     * reads.
     */
    void awaitPipeline(FrameworkOptions options) {
        long intervalMillis = options.getJobPollIntervalSeconds() * 1000L;
        long deadlineMillis = System.currentTimeMillis() + options.getJobPollTimeoutMinutes() * 60_000L;

        while (true) {
            if (checkPipeline(options) == Outcome.READY) {
                return;
            }
            if (System.currentTimeMillis() >= deadlineMillis) {
                throw new PipelineException(PipelineException.Reason.TIMEOUT,
                    options.getReportName(), options.getReportSubprocess(), options.getPeriodId(),
                    "PIPELINE timed out after " + options.getJobPollTimeoutMinutes()
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
            throw new IllegalStateException("Poll loop interrupted", e);
        }
    }
}
