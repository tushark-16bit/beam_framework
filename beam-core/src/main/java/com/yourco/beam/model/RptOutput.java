package com.yourco.beam.model;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Represents one row in the {@code RptOutput} output-tracking table.
 *
 * <p>One row is written per output step after each report run completes.
 * Replaces the use of {@code COM_CmnRptDtl} for report-specific output tracking.
 *
 * <h2>BQ table schema (RptOutput)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.RptOutput (
 *   outpt_cd      STRING   NOT NULL,   -- output code (alias + order, unique per rpt_id)
 *   rpt_dt        DATETIME NOT NULL,   -- report execution datetime
 *   vsn_no        INT64    NOT NULL,   -- rerun counter per (rpt_id, outpt_cd): 1, 2, 3 …
 *   output_ds     STRING,              -- human-readable output description
 *   line_refer_cd STRING,              -- GCS URI, BQ table ref, or API endpoint
 *   sched_tx      STRING,              -- schedule / job-run identifier
 *   bal_am        FLOAT64,             -- balance amount (financial outputs); 0 otherwise
 *   rpt_type_cd   STRING,              -- GCS | BQ | API
 *   rpt_id        INT64    NOT NULL,   -- FK → RptRefer.rpt_id
 *   lst_updt_ts   DATETIME NOT NULL
 * );
 * }</pre>
 */
public final class RptOutput implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String        outptCd;
    public final LocalDateTime rptDt;
    public final int           vsnNo;
    public final String        outputDs;
    public final String        lineReferCd;
    public final String        schedTx;
    public final double        balAm;
    public final String        rptTypeCd;
    public final long          rptId;
    public final LocalDateTime lstUpdtTs;

    public RptOutput(String outptCd, LocalDateTime rptDt, int vsnNo,
                     String outputDs, String lineReferCd, String schedTx,
                     double balAm, String rptTypeCd, long rptId, LocalDateTime lstUpdtTs) {
        this.outptCd     = outptCd;
        this.rptDt       = rptDt;
        this.vsnNo       = vsnNo;
        this.outputDs    = outputDs;
        this.lineReferCd = lineReferCd;
        this.schedTx     = schedTx;
        this.balAm       = balAm;
        this.rptTypeCd   = rptTypeCd;
        this.rptId       = rptId;
        this.lstUpdtTs   = lstUpdtTs;
    }
}
