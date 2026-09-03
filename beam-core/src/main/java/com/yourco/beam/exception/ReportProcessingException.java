package com.yourco.beam.exception;

/**
 * Thrown for a REPORT_PROCESSING failure, carrying enough detail for {@code Main} to log and
 * notify on without re-deriving it from a raw stack trace.
 *
 * <p>Thrown from {@code ReportPipelineFactory.execute()}, which tracks which of its own phases
 * (config load, preprocessing, datasource availability, staging, transform chain, output
 * routing, email) was running when a failure occurred and wraps with the matching {@link Reason}
 * — this is the "specific details" the exception carries, not a generic catch-all.
 */
public final class ReportProcessingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public enum Reason {
        /** {@code fetchReportConfig()} failed — no matching parameter_store row, or bad JSON. */
        CONFIG_NOT_FOUND,
        /** A preprocessing step (BQ_QUERY/API_ENRICHMENT) failed. */
        PREPROCESSING_FAILURE,
        /** A required datasource has no COMPLETED DaRefer row for this period. */
        DATASOURCE_UNAVAILABLE,
        /** Staging DaRec rows into RptStageDa failed. */
        STAGING_FAILURE,
        /** A transform step's query failed, or an alias it referenced wasn't registered. */
        TRANSFORM_FAILURE,
        /** Exporting to GCS/BQ/API, or writing the final output_bq_table, failed. */
        OUTPUT_FAILURE,
        /** Building or sending the report-completion email failed. */
        EMAIL_FAILURE,
        /** Doesn't match any of the above. */
        UNKNOWN
    }

    public final Reason reason;
    public final String reportName;
    public final String reportSubprocess;
    public final int    periodId;

    public ReportProcessingException(Reason reason, String reportName, String reportSubprocess,
                                     int periodId, String message, Throwable cause) {
        super(message, cause);
        this.reason           = reason;
        this.reportName       = reportName;
        this.reportSubprocess = reportSubprocess;
        this.periodId         = periodId;
    }

    /** Builds a message from {@code cause} and wraps it with the given {@link Reason}. */
    public static ReportProcessingException wrap(Reason reason, String reportName, String reportSubprocess,
                                                  int periodId, Throwable cause) {
        return new ReportProcessingException(reason, reportName, reportSubprocess, periodId,
            "REPORT_PROCESSING failed (" + reason + ") for report=" + reportName
            + " subprocess=" + reportSubprocess + " period=" + periodId
            + ": " + cause.getMessage(), cause);
    }
}
