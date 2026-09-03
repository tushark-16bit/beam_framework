package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.io.source.FileSourceAdapter;

/**
 * Classifies a raw exception from {@code pipeline.run().waitUntilFinish()} into a
 * {@link DataSourceDownloadException.Reason}, by walking the cause chain for a recognizable type.
 *
 * <p>This logic lives in {@code beam-runner}, not on {@link DataSourceDownloadException} itself
 * (which is in {@code beam-core}), because it needs to {@code instanceof}-check
 * {@link FileSourceAdapter.FileSourceException} — a {@code beam-io} type {@code beam-core} must
 * never import (see {@code CLAUDE.md} §5). Shared by {@code Main.runDataSourceDownload()} and
 * {@code PipelineSequenceFactory.runDataSourceSteps()} so both classify the same way.
 *
 * <p>Beam wraps a worker-thrown exception in its own runner-specific exception type before it
 * reaches the driver JVM (e.g. {@code Pipeline.PipelineExecutionException}), so the original
 * cause — {@code FileSourceAdapter.FileSourceException} for a missing GCS file, an
 * {@link IllegalArgumentException} for bad input elsewhere — is a few levels down
 * {@link Throwable#getCause()}, not the exception caught directly.
 */
final class DataSourceFailureClassifier {

    private DataSourceFailureClassifier() {}

    static DataSourceDownloadException.Reason classify(Throwable thrown) {
        Throwable t = thrown;
        while (t != null) {
            if (t instanceof FileSourceAdapter.FileSourceException) {
                return DataSourceDownloadException.Reason.FILE_NOT_FOUND;
            }
            if (t instanceof IllegalArgumentException) {
                return DataSourceDownloadException.Reason.INVALID_INPUT;
            }
            t = t.getCause();
        }
        return DataSourceDownloadException.Reason.JOB_FAILURE;
    }
}
