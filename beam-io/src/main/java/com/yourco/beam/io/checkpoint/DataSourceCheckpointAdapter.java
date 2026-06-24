package com.yourco.beam.io.checkpoint;

import com.yourco.beam.model.DataSourceCheckpoint;

import java.util.Optional;

/**
 * Manages checkpoint rows for pipeline runs.
 *
 * <p>Used by both DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING.
 * The checkpoint table tracks the lifecycle of each run:
 * LOADING → COMPLETED / FAILED_BNC / FAILED.
 *
 * <h2>Typical call sequence</h2>
 * <pre>
 *   Driver JVM, before pipeline.run():
 *     long dsId = adapter.createCheckpoint(srcName, perId, dsNm)   // inserts LOADING row
 *
 *   Driver JVM, after pipeline completes:
 *     adapter.updateStatus(dsId, COMPLETED, bncSummaryJson)
 *     — or —
 *     adapter.updateStatus(dsId, FAILED, null)
 * </pre>
 */
public interface DataSourceCheckpointAdapter {

    /**
     * Inserts a LOADING checkpoint row and returns the generated {@code dataSourceId}.
     *
     * <p>The {@code dataSourceId} is a BQ-sequence integer ({@code MAX(dataSourceId)+1}).
     * The version number ({@code vsnNo}) is also auto-incremented per (srcName, perId).
     *
     * @param srcName same as {@code ParameterName} in the parameter store
     * @param perId   period identifier
     * @param dsNm    datasource name — BQ table ref, file path, or API endpoint
     * @return the new {@code dataSourceId} to pass to all record rows and the final update
     */
    long createCheckpoint(String srcName, String perId, String dsNm);

    /**
     * Updates the status of an existing checkpoint row.
     *
     * @param dataSourceId      id returned by {@link #createCheckpoint}
     * @param staCd             {@code DataSourceCheckpoint.STA_*} constant
     * @param balAndCntlSmryTx  BnC summary JSON, or null if not applicable
     */
    void updateStatus(long dataSourceId, String staCd, String balAndCntlSmryTx);

    /**
     * Returns true if the most recent checkpoint for (srcName, perId) has
     * {@code StaCd = COMPLETED} — used to skip already-finished sources.
     */
    boolean isCompleted(String srcName, String perId);

    /**
     * Returns the most recent checkpoint for (srcName, perId), if any.
     */
    Optional<DataSourceCheckpoint> getLatest(String srcName, String perId);
}
