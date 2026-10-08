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
 * <h2>One layer — non-blocking only</h2>
 * {@link #checkSingle}/{@link #checkPipeline} do one BQ read (or a handful) and return
 * immediately, never sleeping. This is {@code ProcessType.STATUS_CHECK}'s implementation — an
 * optional, non-blocking diagnostic a caller (an ops dashboard, a manual look) can invoke
 * standalone.
 *
 * <p>Nothing in this class blocks the driver JVM. {@code DATA_SOURCE_DOWNLOAD} and
 * {@code PIPELINE} no longer poll here at all — see {@link ReportFinalizeTransform} for how a
 * {@code PIPELINE} run now waits for its batched datasources to finish, using a worker-side
 * {@code Wait.on()} data-dependency barrier instead of a driver-JVM poll loop. That change was
 * required by the Flex Template launch contract: the launcher process (running {@code main()})
 * is expected to build the pipeline, submit it, and exit promptly — the Dataflow launch operation
 * is considered complete once the launcher exits, not once the job itself finishes, so any
 * blocking call in {@code main()} (a poll loop or {@code waitUntilFinish()} alike) breaks the
 * launch itself.
 *
 * <h2>Outcome contract</h2>
 * <ul>
 *   <li>{@link Outcome#READY} — every relevant datasource reached {@code COMPLETED}.</li>
 *   <li>{@link Outcome#PENDING} — still {@code LOADING} (or no row yet). Never thrown as an
 *       exception — a still-running job is not a failure, just not finished yet.</li>
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

}
