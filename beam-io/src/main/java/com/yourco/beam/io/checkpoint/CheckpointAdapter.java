package com.yourco.beam.io.checkpoint;

import com.yourco.beam.model.CheckpointRecord;

import java.util.Optional;

/**
 * Adapter interface for pipeline checkpoint storage.
 *
 * <p>Every checkpoint write/read in this framework goes through this interface.
 * The concrete implementation ({@link BigQueryCheckpointAdapter}) can be swapped
 * without touching the orchestration code in
 * {@link com.yourco.beam.runner.DataSourcePipelineFactory}.
 *
 * <h2>Checkpoint lifecycle in DATA_SOURCE_DOWNLOAD</h2>
 * <pre>
 *   Driver JVM, before pipeline.run():
 *     → writeCheckpoint(CheckpointRecord.started(...))   for each source
 *
 *   Driver JVM, after waitUntilFinish() returns OK:
 *     → writeCheckpoint(CheckpointRecord.finished(...))  for each source
 *
 *   Driver JVM, in catch block if pipeline fails:
 *     → writeCheckpoint(CheckpointRecord.failed(...))    for each source
 * </pre>
 *
 * <p>The next pipeline run reads the latest checkpoint per source to skip already-finished
 * sources (unless {@code --overrideDownload=true}).
 */
public interface CheckpointAdapter {

    /**
     * Persists a checkpoint record. If a record with the same
     * (jobRunId, datasourceName, periodId, subprocessName, state) already exists,
     * the implementation may either upsert or append — callers should not assume idempotency
     * unless the implementation documents it.
     */
    void writeCheckpoint(CheckpointRecord record);

    /**
     * Returns the most recent checkpoint for a given source key, regardless of state.
     * Returns {@link Optional#empty()} if no checkpoint exists.
     */
    Optional<CheckpointRecord> getLatestCheckpoint(String datasourceName, String periodId, String subprocessName);

    /**
     * Convenience method: returns true if the most recent checkpoint for this source
     * has state {@code FINISHED_ACCESSING}.
     */
    boolean isDownloadComplete(String datasourceName, String periodId, String subprocessName);
}
