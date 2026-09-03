package com.yourco.beam.exception;

/**
 * Thrown for a DATA_SOURCE_DOWNLOAD failure, carrying enough detail for {@code Main} to log and
 * notify on without re-deriving it from a raw stack trace.
 *
 * <p>Thrown from {@code DataSourcePipelineFactory} (config/graph-assembly failures) and from
 * wherever the Beam job itself is submitted and awaited — {@code Main.runDataSourceDownload()}
 * for a standalone run, {@code PipelineSequenceFactory.runDataSourceSteps()} for the
 * data-source phase of a PIPELINE run. Both call sites classify the failure (e.g. detecting a
 * {@code FileSourceAdapter.FileSourceException} in the cause chain for {@link Reason#FILE_NOT_FOUND})
 * before wrapping, since that classification needs {@code beam-io} types this class — living in
 * {@code beam-core} — cannot import.
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
