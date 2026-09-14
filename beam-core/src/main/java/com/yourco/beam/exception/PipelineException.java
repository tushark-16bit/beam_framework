package com.yourco.beam.exception;

/**
 * Thrown for a PIPELINE (batched DATA_SOURCE_DOWNLOAD + REPORT_PROCESSING wired onto one Beam
 * pipeline via {@code Wait.on()}) failure that isn't already a {@link DataSourceDownloadException}
 * or {@link ReportProcessingException}.
 *
 * <p>{@code PipelineSequenceFactory.execute()} follows one rule: a
 * {@link DataSourceDownloadException} raised while assembling/submitting propagates
 * <b>unchanged</b>; it already carries the right specific detail. Anything else (PIPELINE's own
 * config lookup, or any exception type it doesn't recognize) gets wrapped here instead.
 *
 * <p>{@link Reason#ABORTED_REQUIRED_DATASOURCE} is thrown from inside
 * {@code ReportFinalizeTransform}'s worker-side DoFn — a one-shot check (no retry, no timeout) of
 * every required datasource's terminal {@code DaRefer} status, run after the {@code Wait.on()}
 * barrier confirms every batched datasource branch has finished. There is no poll-loop timeout
 * anymore: {@code Wait.on()} is a direct Beam data-dependency signal, not a sleep loop with a
 * deadline, so a {@code TIMEOUT} reason no longer applies — see {@code Main}'s class javadoc for
 * why blocking in the driver JVM was removed entirely (the Flex Template launch contract).
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
