package com.yourco.beam.exception;

/**
 * Thrown for a DATA_SOURCE_DOWNLOAD failure, carrying enough detail for {@code Main} to log and
 * notify on without re-deriving it from a raw stack trace.
 *
 * <p>Thrown from {@code DataSourcePipelineFactory} (config/graph-assembly failures, and
 * submission failures in {@code Main.runDataSourceDownload()} /
 * {@code PipelineSequenceFactory.submitDataSourceSteps()}), and from
 * {@code DataSourceStatusChecker.checkSingle()} when a {@code STATUS_CHECK} invocation observes a
 * terminal non-COMPLETED row in {@code DaRefer}. That last case is the common one: this
 * framework's runner platform forbids {@code PipelineResult.waitUntilFinish()}, so job outcome is
 * discovered later, by an external poller, rather than as a synchronous exception from the
 * submitting call — see {@code Main}'s class javadoc.
 */
public final class DataSourceDownloadException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public enum Reason {
        /** A referenced source file doesn't exist (e.g. a FILE source's GCS object). */
        FILE_NOT_FOUND,
        /** Missing/malformed CLI params, source config, or declared schema. */
        INVALID_INPUT,
        /** Could not reach GCS/BigQuery/the API source. */
        CONNECTIVITY_FAILURE,
        /** The Beam job itself failed or was cancelled, cause not further classified. */
        JOB_FAILURE,
        /** Doesn't match any of the above. */
        UNKNOWN
    }

    public final Reason reason;
    public final String datasourceName;
    public final String subprocessName;
    public final int    periodId;

    public DataSourceDownloadException(Reason reason, String datasourceName, String subprocessName,
                                       int periodId, String message, Throwable cause) {
        super(message, cause);
        this.reason         = reason;
        this.datasourceName = datasourceName;
        this.subprocessName = subprocessName;
        this.periodId       = periodId;
    }

    /** Builds a message from {@code cause} and wraps it with the given {@link Reason}. */
    public static DataSourceDownloadException wrap(Reason reason, String datasourceName,
                                                    String subprocessName, int periodId,
                                                    Throwable cause) {
        return new DataSourceDownloadException(reason, datasourceName, subprocessName, periodId,
            "DATA_SOURCE_DOWNLOAD failed (" + reason + ") for datasource=" + datasourceName
            + " subprocess=" + subprocessName + " period=" + periodId
            + ": " + cause.getMessage(), cause);
    }
}
