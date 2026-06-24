package com.yourco.beam.model;

import java.time.Instant;

/**
 * Represents one row in the checkpoint table.
 *
 * <p>A checkpoint is created (with {@code StaCd = LOADING}) before the pipeline starts,
 * and updated (COMPLETED / FAILED_BNC / FAILED) after it finishes. Both DATA_SOURCE_DOWNLOAD
 * and REPORT_PROCESSING use the same table.
 *
 * <h2>BQ table schema</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.data_source_checkpoints (
 *   dataSourceId       INT64     NOT NULL,   -- BQ sequence: MAX(dataSourceId)+1 per run
 *   srcName            STRING    NOT NULL,   -- same as ParameterName in parameter_store
 *   vsnNo              INT64     NOT NULL,   -- increments on rerun of same (srcName, PerId)
 *   PerId              STRING,               -- period identifier
 *   DSNm               STRING,               -- BQ table ref, file path, or API endpoint
 *   BalAndCntlSmryTx   STRING,               -- JSON: BnC summary (status, counts, amounts)
 *   StaCd              STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED
 *   CreatedTs          TIMESTAMP NOT NULL,
 *   LstUpdtTs          TIMESTAMP NOT NULL
 * );
 * }</pre>
 *
 * <h2>BalAndCntlSmryTx format</h2>
 * <pre>{@code
 * {
 *   "status":    "Matched",
 *   "srcCount":  1000,
 *   "srcAmount": 5000000.00,
 *   "dstCount":  1000,
 *   "dstAmount": 5000000.00
 * }
 * }</pre>
 */
public final class DataSourceCheckpoint {

    // ── Status codes ──────────────────────────────────────────────────────────
    public static final String STA_LOADING     = "LOADING";
    public static final String STA_COMPLETED   = "COMPLETED";
    public static final String STA_FAILED_BNC  = "FAILED_BNC";
    public static final String STA_FAILED      = "FAILED";

    public final long    dataSourceId;
    public final String  srcName;
    public final long    vsnNo;
    public final String  perId;
    public final String  dsNm;
    public final String  balAndCntlSmryTx;
    public final String  staCd;
    public final Instant createdTs;
    public final Instant lstUpdtTs;

    public DataSourceCheckpoint(long dataSourceId, String srcName, long vsnNo, String perId,
                                String dsNm, String balAndCntlSmryTx, String staCd,
                                Instant createdTs, Instant lstUpdtTs) {
        this.dataSourceId      = dataSourceId;
        this.srcName           = srcName;
        this.vsnNo             = vsnNo;
        this.perId             = perId;
        this.dsNm              = dsNm;
        this.balAndCntlSmryTx  = balAndCntlSmryTx;
        this.staCd             = staCd;
        this.createdTs         = createdTs;
        this.lstUpdtTs         = lstUpdtTs;
    }

    /** Creates a LOADING checkpoint with generated dataSourceId and vsnNo. */
    public static DataSourceCheckpoint loading(long dataSourceId, long vsnNo,
                                               String srcName, String perId, String dsNm) {
        Instant now = Instant.now();
        return new DataSourceCheckpoint(
            dataSourceId, srcName, vsnNo, perId, dsNm, null, STA_LOADING, now, now);
    }

    @Override
    public String toString() {
        return "DataSourceCheckpoint{id=" + dataSourceId
            + ", src=" + srcName + ", vsn=" + vsnNo
            + ", period=" + perId + ", sta=" + staCd + "}";
    }
}
