package com.yourco.beam.exception;

/**
 * Thrown for a PIPELINE (composed DATA_SOURCE_DOWNLOAD submission + STATUS_CHECK readiness gate +
 * REPORT_PROCESSING) failure that isn't already a {@link DataSourceDownloadException} or
 * {@link ReportProcessingException}.
 *
 * <p>{@code PipelineSequenceFactory.execute()} — which only submits the batched data-source job,
 * see its class javadoc for why it can no longer also wait or run the report — follows one rule:
 * a {@link DataSourceDownloadException} raised while submitting propagates <b>unchanged</b>; it
 * already carries the right specific detail. Anything else (PIPELINE's own config lookup, or any
 * exception type it doesn't recognize) gets wrapped here instead.
 *
 * <p>{@link Reason#ABORTED_REQUIRED_DATASOURCE} is no longer thrown from
 * {@code PipelineSequenceFactory} itself — that required/optional gate moved to
 * {@code DataSourceStatusChecker.checkPipeline()}, invoked later via
 * {@code --processType=STATUS_CHECK} once the batched job is expected to have finished (see
 * {@code Main}'s class javadoc for why the wait moved out-of-process).
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
