package com.yourco.beam.exception;

/**
 * Thrown for a PIPELINE (composed DATA_SOURCE_DOWNLOAD submission + poll-to-ready +
 * REPORT_PROCESSING, all within one blocking call) failure that isn't already a
 * {@link DataSourceDownloadException} or {@link ReportProcessingException}.
 *
 * <p>{@code PipelineSequenceFactory.execute()} follows one rule: a
 * {@link DataSourceDownloadException} raised while submitting propagates <b>unchanged</b>; it
 * already carries the right specific detail. Anything else (PIPELINE's own config lookup, or any
 * exception type it doesn't recognize) gets wrapped here instead.
 *
 * <p>{@link Reason#ABORTED_REQUIRED_DATASOURCE} and {@link Reason#TIMEOUT} are thrown by
 * {@code DataSourceStatusChecker.awaitPipeline()} — the poll loop
 * {@code PipelineSequenceFactory.execute()} calls, in-process, after submitting and before running
 * the report. This platform forbids {@code PipelineResult.waitUntilFinish()}, so that loop is a
 * plain {@code Thread.sleep} re-reading {@code DaRefer} (via
 * {@code DataSourceStatusChecker.checkPipeline()}) rather than a blocking call on the Beam
 * {@code PipelineResult} itself — see {@code Main}'s class javadoc.
 */
public final class PipelineException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public enum Reason {
        /** {@code --reportName}/{@code --periodId} missing, or similar CLI-level misconfiguration. */
        CONFIGURATION_ERROR,
        /** PIPELINE's own report-config lookup (for {@code datasources[]}) failed. */
        CONFIG_NOT_FOUND,
        /** A required datasource (per {@code ReportDatasourceRef.required}) never reached COMPLETED. */
        ABORTED_REQUIRED_DATASOURCE,
        /** The batched data-source phase failed with something other than DataSourceDownloadException. */
        DATASOURCE_PHASE_FAILURE,
        /** The terminal report phase failed with something other than ReportProcessingException. */
        REPORT_PHASE_FAILURE,
        /** {@code DataSourceStatusChecker.awaitPipeline()}'s poll loop ran past
         *  {@code --jobPollTimeoutMinutes} without every required datasource reaching COMPLETED. */
        TIMEOUT,
        /** Doesn't match any of the above. */
        UNKNOWN
    }

    public final Reason reason;
    public final String reportName;
    public final String reportSubprocess;
    public final int    periodId;

    public PipelineException(Reason reason, String reportName, String reportSubprocess,
                             int periodId, String message, Throwable cause) {
        super(message, cause);
        this.reason           = reason;
        this.reportName       = reportName;
        this.reportSubprocess = reportSubprocess;
        this.periodId         = periodId;
    }

    public PipelineException(Reason reason, String reportName, String reportSubprocess,
                             int periodId, String message) {
        this(reason, reportName, reportSubprocess, periodId, message, null);
    }

    /** Builds a message from {@code cause} and wraps it with the given {@link Reason}. */
    public static PipelineException wrap(Reason reason, String reportName, String reportSubprocess,
                                         int periodId, Throwable cause) {
        return new PipelineException(reason, reportName, reportSubprocess, periodId,
            "PIPELINE failed (" + reason + ") for report=" + reportName
            + " subprocess=" + reportSubprocess + " period=" + periodId
            + ": " + cause.getMessage(), cause);
    }
}
