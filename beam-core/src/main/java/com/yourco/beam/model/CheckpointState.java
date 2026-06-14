package com.yourco.beam.model;

/**
 * Lifecycle states for a single data-source download attempt.
 *
 * <p>The state machine is linear:
 * <pre>
 *   STARTED_ACCESSING ──► FINISHED_ACCESSING
 *                    └──► FAILED_DOWNLOADING
 * </pre>
 *
 * States are written to the BigQuery checkpoint table by
 * {@link com.yourco.beam.io.checkpoint.BigQueryCheckpointAdapter} in the driver JVM,
 * not inside DoFns.
 */
public enum CheckpointState {
    /** Written before {@code pipeline.run()} — the source download has begun. */
    STARTED_ACCESSING,
    /** Written after {@code waitUntilFinish()} returns successfully. */
    FINISHED_ACCESSING,
    /** Written when {@code pipeline.run()} throws or the pipeline finishes with FAILED state. */
    FAILED_DOWNLOADING
}
