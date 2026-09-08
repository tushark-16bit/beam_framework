package com.yourco.beam.orchestrator.model;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * A persisted task record — one row in the orchestrator task table and one entry
 * in the GCS manifest JSON.
 *
 * <p>All tasks start with {@code status = PENDING}. The downstream system (Airflow,
 * another process, or the pipeline JAR itself) is responsible for updating the status
 * to RUNNING / COMPLETED / FAILED / SKIPPED. The orchestrator only writes PENDING rows.
 *
 * <h2>metadata</h2>
 * A freeform map for any operational context that doesn't belong in {@link RunSpec#extraParams}
 * (e.g. which orchestrator version created this row, the Airflow DAG run ID, etc.).
 */
public final class TaskItem {

    public static final String STATUS_PENDING   = "PENDING";
    public static final String STATUS_RUNNING   = "RUNNING";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_FAILED    = "FAILED";
    public static final String STATUS_SKIPPED   = "SKIPPED";

    public final String    taskId;
    public final String    runId;
    public final RunSpec   spec;
    public final String    status;
    public final Instant   createdAt;
    public final Map<String, String> metadata;

    public TaskItem(String taskId, String runId, RunSpec spec,
                    String status, Instant createdAt, Map<String, String> metadata) {
        this.taskId    = taskId;
        this.runId     = runId;
        this.spec      = spec;
        this.status    = status;
        this.createdAt = createdAt;
        this.metadata  = metadata != null
                         ? Collections.unmodifiableMap(metadata)
                         : Collections.emptyMap();
    }
}
