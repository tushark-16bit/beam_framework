package com.yourco.beam.orchestrator.manifest;

import com.yourco.beam.orchestrator.model.TaskItem;

import java.util.List;

/**
 * Writes a run manifest so that the triggering DAG can read which tasks were created
 * and fan them out into individual pipeline invocations.
 *
 * <p>The default implementation is {@link GcsManifestWriter}, which writes a JSON file
 * to GCS at a configurable path.
 *
 * <p>Swap this interface to write to a different medium (S3, Pub/Sub, HTTP callback, etc.).
 * If no manifest output is needed (e.g. the DAG reads directly from the task table),
 * use a no-op implementation.
 *
 * <h2>Manifest format (GCS default)</h2>
 * <pre>{@code
 * {
 *   "runId":       "TRADING-MONTHLY-2024-01-31-abc123",
 *   "generatedAt": "2024-01-31T06:00:00Z",
 *   "parentId":    "TRADING",
 *   "frequency":   "MONTHLY",
 *   "runDate":     "2024-01-31",
 *   "period":      { "periodId": 202401, "periodStart": "2024-01-01", "periodEnd": "2024-01-31" },
 *   "tasks":       [ { ...TaskItem fields... }, ... ]
 * }
 * }</pre>
 */
@FunctionalInterface
public interface ManifestWriter {

    /**
     * Writes the manifest and returns the location where it was written
     * (e.g. a GCS URI, a file path, or a logical identifier).
     *
     * @param runId     the unique identifier for this orchestrator run
     * @param parentId  the business group this run was for
     * @param frequency the frequency (DAILY / MONTHLY / WEEKLY)
     * @param runDate   the run date string (yyyy-MM-dd)
     * @param tasks     the task items created for this run
     * @return location string (returned to the caller and logged)
     */
    String write(String runId, String parentId, String frequency,
                 String runDate, List<TaskItem> tasks);
}
