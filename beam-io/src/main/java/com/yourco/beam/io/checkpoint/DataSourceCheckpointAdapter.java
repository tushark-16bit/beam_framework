package com.yourco.beam.io.checkpoint;

import com.yourco.beam.model.DataSourceCheckpoint;

import java.util.Optional;

/**
 * Manages rows in the {@code DaRefer} reference table.
 *
 * <p>Used by both DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING.
 * Tracks the lifecycle of each run: LOADING → COMPLETED / FAILED_BNC / FAILED.
 *
 * <h2>Typical call sequence</h2>
 * <pre>
 *   Driver JVM, before pipeline.run():
 *     long daId = adapter.createCheckpoint(srceNm, perId, flNm)   // inserts LOADING row
 *
 *   Driver JVM, after pipeline completes:
 *     adapter.updateStatus(daId, COMPLETED, bncSummaryJson)
 *     — or —
 *     adapter.updateStatus(daId, FAILED, null)
 * </pre>
 */
public interface DataSourceCheckpointAdapter {

    /**
     * Inserts a LOADING row into {@code DaRefer} and returns the generated {@code DaId}.
     *
     * <p>{@code DaId} is {@code MAX(DaId)+1} across the table.
     * {@code VsnNo} is auto-incremented per (SrceNm, PerId).
     *
     * @param SrceNm data source or report name
     * @param PerId  period identifier (from MSTR_Per)
     * @param FlNm   source location — BQ table ref, file path, or API endpoint
     * @return the new {@code DaId} to pass to all DaRec rows and the final status update
     */
    long createCheckpoint(String SrceNm, String PerId, String FlNm);

    /**
     * Updates the StaCd (and optionally BalAndCntlSmryTx) of an existing DaRefer row.
     *
     * @param DaId              id returned by {@link #createCheckpoint}
     * @param StaCd             {@code DataSourceCheckpoint.STA_*} constant
     * @param balAndCntlSmryTx  BnC summary JSON, or null if not applicable
     */
    void updateStatus(long DaId, String StaCd, String balAndCntlSmryTx);

    /**
     * Returns true if DaRefer has a COMPLETED row for (SrceNm, PerId) —
     * used to skip already-finished sources.
     */
    boolean isCompleted(String SrceNm, String PerId);

    /**
     * Returns the most recent DaRefer row for (SrceNm, PerId), if any.
     */
    Optional<DataSourceCheckpoint> getLatest(String SrceNm, String PerId);
}
