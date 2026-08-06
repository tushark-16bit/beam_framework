package com.yourco.beam.model;

import java.time.LocalDateTime;

/**
 * Represents one row in the {@code DaRefer} reference table.
 *
 * <p>A row is created (sta_cd = LOADING) before the pipeline starts and updated
 * (COMPLETED / FAILED_BNC / FAILED) after it finishes. Only DATA_SOURCE_DOWNLOAD
 * uses this table — REPORT_PROCESSING uses {@code RptRefer} via {@code ReportCheckpointAdapter}.
 *
 * <h2>BQ table schema (DaRefer)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.DaRefer (
 *   da_id                INT64    NOT NULL,   -- surrogate PK: MAX(da_id)+1 per run
 *   srce_nm              STRING   NOT NULL,   -- data source name
 *   vsn_no               INT64    NOT NULL,   -- rerun counter per (srce_nm, per_id): 1, 2, 3 …
 *   per_id               INT64,               -- period identifier (from MSTR_Per)
 *   fl_nm                STRING,              -- BQ table ref, file path, or API endpoint
 *   bal_and_cntl_smry_tx STRING,              -- JSON: BnC summary {status, srcCount, dstCount, …}
 *   sta_cd               STRING   NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED_TRANSFORM | FAILED
 *   created_ts           DATETIME NOT NULL,
 *   lst_updt_ts          DATETIME NOT NULL
 * );
 * }</pre>
 */
public final class DataSourceCheckpoint {

    // ── Status codes ──────────────────────────────────────────────────────────
    public static final String STA_LOADING          = "LOADING";
    public static final String STA_COMPLETED        = "COMPLETED";
    public static final String STA_FAILED_BNC       = "FAILED_BNC";
    /** The optional {@code data_transform_query} errored, or its output failed row-count bounds. */
    public static final String STA_FAILED_TRANSFORM = "FAILED_TRANSFORM";
    public static final String STA_FAILED           = "FAILED";

    public final long          daId;
    public final String        srceNm;
    public final long          vsnNo;
    public final int           perId;
    public final String        flNm;
    public final String        balAndCntlSmryTx;
    public final String        staCd;
    public final LocalDateTime createdTs;
    public final LocalDateTime lstUpdtTs;

    public DataSourceCheckpoint(long daId, String srceNm, long vsnNo, int perId,
                                String flNm, String balAndCntlSmryTx, String staCd,
                                LocalDateTime createdTs, LocalDateTime lstUpdtTs) {
        this.daId             = daId;
        this.srceNm           = srceNm;
        this.vsnNo            = vsnNo;
        this.perId            = perId;
        this.flNm             = flNm;
        this.balAndCntlSmryTx = balAndCntlSmryTx;
        this.staCd            = staCd;
        this.createdTs        = createdTs;
        this.lstUpdtTs        = lstUpdtTs;
    }

    /** Creates a LOADING row with generated daId and vsnNo. */
    public static DataSourceCheckpoint loading(long daId, long vsnNo,
                                               String srceNm, int perId, String flNm) {
        LocalDateTime now = LocalDateTime.now();
        return new DataSourceCheckpoint(
            daId, srceNm, vsnNo, perId, flNm, null, STA_LOADING, now, now);
    }

    @Override
    public String toString() {
        return "DataSourceCheckpoint{daId=" + daId
            + ", srceNm=" + srceNm + ", vsnNo=" + vsnNo
            + ", perId=" + perId + ", staCd=" + staCd + "}";
    }
}
