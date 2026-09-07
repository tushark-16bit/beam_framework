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
 * Fast, synchronous, DB-only readiness check — {@code ProcessType.STATUS_CHECK}'s implementation,
 * and the replacement for {@code PipelineResult.waitUntilFinish()} on a platform where the driver
 * JVM must submit and return quickly, never block for a job's full runtime.
 *
 * <p>Reads {@code DaRefer} via {@link BigQueryDataSourceCheckpointAdapter#getLatest} — the exact
 * same row {@code PostDownloadFinalizeTransform} writes from inside the Beam worker as the last
 * step of each source branch — instead of observing a synchronous exception from
 * {@code PipelineResult}. Never sleeps or loops internally: one BQ read per datasource, then
 * returns immediately. The caller (an external poller — an Airflow sensor's poke loop) is
 * expected to invoke this repeatedly, on its own schedule, until it returns {@link Outcome#READY}
 * or throws.
 *
 * <h2>Outcome contract</h2>
 * <ul>
 *   <li>{@link Outcome#READY} — every relevant datasource reached {@code COMPLETED}; safe to
 *       proceed (invoke {@code --processType=REPORT_PROCESSING}, or just record done).</li>
 *   <li>{@link Outcome#PENDING} — still {@code LOADING} (or no row yet). Never thrown as an
 *       exception — a still-running job is not a failure, just not finished yet.</li>
 *   <li>A terminal failure ({@code FAILED} / {@code FAILED_BNC} / {@code FAILED_TRANSFORM} on a
 *       required datasource) is <b>thrown</b>, not returned — as
 *       {@link DataSourceDownloadException} ({@link #checkSingle}) or {@link PipelineException}
 *       ({@link #checkPipeline}) — so it flows through {@code Main.main()}'s existing catch →
 *       {@code FailureNotifier} → {@code EmailSendUtility} path exactly like a synchronous
 *       failure always has. Only how the failure is first observed changes (DB read on a status
 *       check invocation, instead of an exception from a blocking wait); the notification path
 *       downstream of that is unchanged.</li>
 * </ul>
 */
final class DataSourceStatusChecker {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceStatusChecker.class);

    enum Outcome { READY, PENDING }

    /**
     * Standalone {@code DATA_SOURCE_DOWNLOAD} readiness check for a single
     * {@code --datasourceName}/{@code --subprocessName}/{@code --periodId}.
     */
    Outcome checkSingle(FrameworkOptions options) {
        BigQueryDataSourceCheckpointAdapter checkpointAdapter =
            new BigQueryDataSourceCheckpointAdapter(options);
        Optional<DataSourceCheckpoint> latest =
            checkpointAdapter.getLatest(options.getDatasourceName(), options.getPeriodId());

        if (latest.isEmpty() || DataSourceCheckpoint.STA_LOADING.equals(latest.get().staCd)) {
            LOG.info("STATUS_CHECK '{}' (period={}): still pending",
                     options.getDatasourceName(), options.getPeriodId());
            return Outcome.PENDING;
        }

        DataSourceCheckpoint checkpoint = latest.get();
        if (DataSourceCheckpoint.STA_COMPLETED.equals(checkpoint.staCd)) {
            LOG.info("STATUS_CHECK '{}' (period={}): COMPLETED (da_id={})",
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
     * {@code ReportConfig.datasources[]} declares, applying the same required/optional gate
     * {@code PipelineSequenceFactory.checkRequiredDataSources()} used to apply inline before this
     * class existed.
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
            LOG.info("STATUS_CHECK report='{}': declares no datasources — ready", reportName);
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
                LOG.warn("STATUS_CHECK report='{}': optional datasource '{}' is {} — not blocking",
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
            LOG.info("STATUS_CHECK report='{}': required datasource(s) still pending", reportName);
            return Outcome.PENDING;
        }

        LOG.info("STATUS_CHECK report='{}': all required datasource(s) COMPLETED", reportName);
        return Outcome.READY;
    }
}
