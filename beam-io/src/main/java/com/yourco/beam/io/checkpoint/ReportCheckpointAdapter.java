package com.yourco.beam.io.checkpoint;

/**
 * Manages the four BigQuery tables that track a {@code REPORT_PROCESSING} run:
 * <ul>
 *   <li>{@code RptRefer}   — lifecycle checkpoint (LOADING → COMPLETED / FAILED)</li>
 *   <li>{@code RptDaMap}   — maps each report run to the data-source {@code da_id}s it consumed</li>
 *   <li>{@code RptStageDa} — transient staging area; rows copied from {@code DaRec} before transforms,
 *                            deleted after transforms complete</li>
 *   <li>{@code RptOutput}  — one row per output step produced by the report run</li>
 * </ul>
 *
 * <p>All methods run in the driver JVM — never inside Beam workers.
 *
 * <h2>Typical call sequence</h2>
 * <pre>
 *   long rptId = adapter.createCheckpoint(rptNm, perId, rptDs)   // RptRefer LOADING
 *
 *   for each required datasource:
 *       long mapId = adapter.addDaMapping(rptId, daId)            // RptDaMap
 *       adapter.stageFromDaRec(mapId, daId)                        // RptStageDa INSERT
 *       alias = adapter.stagedDataSubquery(mapId)                  // used in transform SQL
 *
 *   run transform chain (SQL reads staged data via subquery)
 *
 *   for each output:
 *       adapter.writeOutput(rptId, outptCd, ...)                  // RptOutput
 *
 *   adapter.clearStagedData(rptId)                                 // RptStageDa DELETE
 *   adapter.updateStatus(rptId, ReportCheckpoint.STA_COMPLETED)   // RptRefer
 * </pre>
 */
public interface ReportCheckpointAdapter {

    // ── RptRefer ─────────────────────────────────────────────────────────────

    /**
     * Inserts a LOADING row into {@code RptRefer} and returns the generated {@code rpt_id}.
     *
     * @param rptNm  report name (matches {@code --reportName})
     * @param perId  period identifier (integer)
     * @param rptDs  report description (from parameter_store); may be null
     */
    long createCheckpoint(String rptNm, int perId, String rptDs);

    /**
     * Updates the {@code sta_cd} of an existing {@code RptRefer} row.
     *
     * @param rptId  id returned by {@link #createCheckpoint}
     * @param staCd  {@code ReportCheckpoint.STA_*} constant
     */
    void updateStatus(long rptId, String staCd);

    /**
     * Returns true if a COMPLETED {@code RptRefer} row exists for (rpt_nm, per_id).
     * Used to support override-key logic.
     */
    boolean isCompleted(String rptNm, int perId);

    // ── RptDaMap ─────────────────────────────────────────────────────────────

    /**
     * Inserts a row into {@code RptDaMap} linking this report run to a data-source run.
     *
     * @param rptId FK to {@code RptRefer.rpt_id}
     * @param daId  FK to {@code DaRefer.da_id} (the COMPLETED datasource run consumed)
     * @return the new {@code map_id}
     */
    long addDaMapping(long rptId, long daId);

    // ── RptStageDa ───────────────────────────────────────────────────────────

    /**
     * Copies all rows for {@code daId} from {@code DaRec} into {@code RptStageDa}
     * under the given {@code mapId}. Runs as a BQ DML INSERT…SELECT job.
     *
     * @param mapId FK to {@code RptDaMap.map_id}
     * @param daId  the {@code DaRefer.da_id} whose {@code DaRec} rows to stage
     */
    void stageFromDaRec(long mapId, long daId);

    /**
     * Returns the BQ subquery that exposes staged data for {@code mapId}.
     * The result is used as the alias value in the transform chain SQL:
     * {@code (SELECT stage_ds_json_tx FROM RptStageDa WHERE map_id = X)}.
     */
    String stagedDataSubquery(long mapId);

    /**
     * Deletes all {@code RptStageDa} rows associated with this report run
     * (joined via {@code RptDaMap.rpt_id}). Called after all outputs are produced.
     *
     * @param rptId FK to {@code RptRefer.rpt_id}
     */
    void clearStagedData(long rptId);

    // ── RptOutput ────────────────────────────────────────────────────────────

    /**
     * Writes one row to {@code RptOutput} for a completed output step.
     *
     * @param rptId       FK to {@code RptRefer.rpt_id}
     * @param outptCd     unique output code within this report run (alias + order)
     * @param outputDs    human-readable output description (input alias)
     * @param lineReferCd line reference: GCS URI, BQ table ref, or API endpoint
     * @param schedTx     schedule / job-run identifier ({@code --jobRunId})
     * @param balAm       balance amount for financial outputs; 0 otherwise
     * @param rptTypeCd   report type code: GCS | BQ | API
     */
    void writeOutput(long rptId, String outptCd, String outputDs,
                     String lineReferCd, String schedTx, double balAm, String rptTypeCd);
}
