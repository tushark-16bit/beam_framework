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
     * Inserts a LOADING row into {@code DaRefer} and returns the generated {@code da_id}.
     *
     * <p>{@code da_id} is {@code MAX(da_id)+1} across the table.
     * {@code vsn_no} is auto-incremented per (srce_nm, per_id).
     *
     * @param srceNm data source or report name
     * @param perId  period identifier (from MSTR_Per)
     * @param flNm   source location — BQ table ref, file path, or API endpoint
     * @return the new {@code da_id} to pass to all DaRec rows and the final status update
     */
    long createCheckpoint(String srceNm, String perId, String flNm);

    /**
     * Updates the sta_cd (and optionally bal_and_cntl_smry_tx) of an existing DaRefer row.
     *
     * @param daId              id returned by {@link #createCheckpoint}
     * @param staCd             {@code DataSourceCheckpoint.STA_*} constant
     * @param balAndCntlSmryTx  BnC summary JSON, or null if not applicable
     */
    void updateStatus(long daId, String staCd, String balAndCntlSmryTx);

    /**
     * Returns true if DaRefer has a COMPLETED row for (srce_nm, per_id) —
     * used to skip already-finished sources.
     */
    boolean isCompleted(String srceNm, String perId);

    /**
     * Returns the most recent DaRefer row for (srce_nm, per_id), if any.
     */
    Optional<DataSourceCheckpoint> getLatest(String srceNm, String perId);

    /**
     * Returns the {@code da_id} of the most recent COMPLETED DaRefer row for a datasource.
     *
     * <p>Used by REPORT_PROCESSING to build the DaRec subquery:
     * {@code SELECT row_da_json_tx FROM DaRec WHERE da_id = X}.
     *
     * @throws IllegalArgumentException if no COMPLETED row exists for (srce_nm, per_id)
     */
    long fetchLatestCompletedDaId(String srceNm, String perId);
}
