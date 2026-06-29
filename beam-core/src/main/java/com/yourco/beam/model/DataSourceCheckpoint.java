package com.yourco.beam.model;

import java.time.Instant;

/**
 * Represents one row in the {@code DaRefer} reference table.
 *
 * <p>A row is created (sta_cd = LOADING) before the pipeline starts and updated
 * (COMPLETED / FAILED_BNC / FAILED) after it finishes. Both DATA_SOURCE_DOWNLOAD
 * and REPORT_PROCESSING use the same table.
 *
 * <h2>BQ table schema (DaRefer)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.DaRefer (
 *   da_id                INT64     NOT NULL,   -- surrogate PK: MAX(da_id)+1 per run
 *   srce_nm              STRING    NOT NULL,   -- data source or report name
 *   vsn_no               INT64     NOT NULL,   -- rerun counter per (srce_nm, per_id): 1, 2, 3 …
 *   per_id               STRING,               -- period identifier (from MSTR_Per)
 *   fl_nm                STRING,               -- BQ table ref, file path, or API endpoint
 *   bal_and_cntl_smry_tx STRING,               -- JSON: BnC summary {status, srcCount, dstCount, …}
 *   sta_cd               STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED
 *   created_ts           TIMESTAMP NOT NULL,
 *   lst_updt_ts          TIMESTAMP NOT NULL
 * );
 * }</pre>
 */
public final class DataSourceCheckpoint {

    // ── Status codes ──────────────────────────────────────────────────────────
    public static final String STA_LOADING    = "LOADING";
    public static final String STA_COMPLETED  = "COMPLETED";
    public static final String STA_FAILED_BNC = "FAILED_BNC";
    public static final String STA_FAILED     = "FAILED";

    public final long    daId;
    public final String  srceNm;
    public final long    vsnNo;
    public final String  perId;
    public final String  flNm;
    public final String  balAndCntlSmryTx;
    public final String  staCd;
    public final Instant createdTs;
    public final Instant lstUpdtTs;

    public DataSourceCheckpoint(long daId, String srceNm, long vsnNo, String perId,
                                String flNm, String balAndCntlSmryTx, String staCd,
                                Instant createdTs, Instant lstUpdtTs) {
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
                                               String srceNm, String perId, String flNm) {
        Instant now = Instant.now();
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
