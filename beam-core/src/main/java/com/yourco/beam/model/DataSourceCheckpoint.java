package com.yourco.beam.model;

import java.time.Instant;

/**
 * Represents one row in the {@code DaRefer} reference table.
 *
 * <p>A row is created (StaCd = LOADING) before the pipeline starts and updated
 * (COMPLETED / FAILED_BNC / FAILED) after it finishes. Both DATA_SOURCE_DOWNLOAD
 * and REPORT_PROCESSING use the same table.
 *
 * <h2>BQ table schema (DaRefer)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.DaRefer (
 *   DaId               INT64     NOT NULL,   -- surrogate PK: MAX(DaId)+1 per run
 *   SrceNm             STRING    NOT NULL,   -- data source or report name
 *   VsnNo              INT64     NOT NULL,   -- rerun counter per (SrceNm, PerId): 1, 2, 3 …
 *   PerId              STRING,               -- period identifier (from MSTR_Per)
 *   FlNm               STRING,               -- BQ table ref, file path, or API endpoint
 *   BalAndCntlSmryTx   STRING,               -- JSON: BnC summary {status, srcCount, dstCount, …}
 *   StaCd              STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED
 *   CreatedTs          TIMESTAMP NOT NULL,
 *   LstUpdtTs          TIMESTAMP NOT NULL
 * );
 * }</pre>
 */
public final class DataSourceCheckpoint {

    // ── Status codes ──────────────────────────────────────────────────────────
    public static final String STA_LOADING    = "LOADING";
    public static final String STA_COMPLETED  = "COMPLETED";
    public static final String STA_FAILED_BNC = "FAILED_BNC";
    public static final String STA_FAILED     = "FAILED";

    public final long    DaId;
    public final String  SrceNm;
    public final long    VsnNo;
    public final String  PerId;
    public final String  FlNm;
    public final String  balAndCntlSmryTx;
    public final String  StaCd;
    public final Instant CreatedTs;
    public final Instant LstUpdtTs;

    public DataSourceCheckpoint(long DaId, String SrceNm, long VsnNo, String PerId,
                                String FlNm, String balAndCntlSmryTx, String StaCd,
                                Instant CreatedTs, Instant LstUpdtTs) {
        this.DaId             = DaId;
        this.SrceNm           = SrceNm;
        this.VsnNo            = VsnNo;
        this.PerId            = PerId;
        this.FlNm             = FlNm;
        this.balAndCntlSmryTx = balAndCntlSmryTx;
        this.StaCd            = StaCd;
        this.CreatedTs        = CreatedTs;
        this.LstUpdtTs        = LstUpdtTs;
    }

    /** Creates a LOADING row with generated DaId and VsnNo. */
    public static DataSourceCheckpoint loading(long DaId, long VsnNo,
                                               String SrceNm, String PerId, String FlNm) {
        Instant now = Instant.now();
        return new DataSourceCheckpoint(
            DaId, SrceNm, VsnNo, PerId, FlNm, null, STA_LOADING, now, now);
    }

    @Override
    public String toString() {
        return "DataSourceCheckpoint{DaId=" + DaId
            + ", SrceNm=" + SrceNm + ", VsnNo=" + VsnNo
            + ", PerId=" + PerId + ", StaCd=" + StaCd + "}";
    }
}
