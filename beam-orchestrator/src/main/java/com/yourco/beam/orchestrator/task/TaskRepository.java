package com.yourco.beam.orchestrator.task;

import com.yourco.beam.orchestrator.model.TaskItem;

import java.util.List;

/**
 * Persists task rows to a durable store.
 *
 * <p>The default implementation is {@link BigQueryTaskRepository}, which inserts rows
 * into {@code orchestrator_tasks} via the BigQuery streaming insert API.
 *
 * <p>Swap this interface to write to a different database, message queue, or file system.
 * The orchestrator calls {@link #save} exactly once per run, after all specs are resolved.
 */
public interface TaskRepository {

    /**
     * Persists all task items for one orchestrator run.
     *
     * <p>Implementations should be idempotent where possible (e.g. use {@code task_id}
     * as a deduplication key). The orchestrator does not retry on failure — if saving
     * fails, the exception propagates and the run is marked failed.
     *
     * @param tasks non-empty list of PENDING task items for this run
     */
    void save(List<TaskItem> tasks);
}
