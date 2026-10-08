package com.yourco.beam.model;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Represents one row in the {@code RptRefer} reference table.
 *
 * <p>A row is created ({@code sta_cd = LOADING}) before the report executes and updated
 * ({@code COMPLETED} / {@code FAILED}) when it finishes.
 * Only {@code REPORT_PROCESSING} uses this table — {@code DATA_SOURCE_DOWNLOAD} uses {@code DaRefer}.
 *
 * <h2>BQ table schema (RptRefer)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.RptRefer (
 *   rpt_id      INT64     NOT NULL,   -- surrogate PK: MAX(rpt_id)+1 per run
 *   rpt_nm      STRING    NOT NULL,   -- report name (matches --reportName)
 *   per_id      INT64     NOT NULL,   -- period identifier (from MSTR_Per)
 *   rpt_ds      STRING,               -- report description (from parameter_store)
 *   sta_cd      STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED
 *   creat_ts    DATETIME  NOT NULL,
 *   lst_updt_ts DATETIME  NOT NULL
 * );
 * }</pre>
 */
public final class ReportCheckpoint implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String STA_LOADING   = "LOADING";
    public static final String STA_COMPLETED = "COMPLETED";
    public static final String STA_FAILED    = "FAILED";

    public final long          rptId;
    public final String        rptNm;
    public final int           perId;
    public final String        rptDs;
    public final String        staCd;
    public final LocalDateTime creatTs;
    public final LocalDateTime lstUpdtTs;

    public ReportCheckpoint(long rptId, String rptNm, int perId, String rptDs,
                             String staCd, LocalDateTime creatTs, LocalDateTime lstUpdtTs) {
        this.rptId     = rptId;
        this.rptNm     = rptNm;
        this.perId     = perId;
        this.rptDs     = rptDs;
        this.staCd     = staCd;
        this.creatTs   = creatTs;
        this.lstUpdtTs = lstUpdtTs;
    }

    @Override
    public String toString() {
        return "ReportCheckpoint{rptId=" + rptId + ", rptNm=" + rptNm
               + ", perId=" + perId + ", staCd=" + staCd + "}";
    }
}
