package com.yourco.beam.model;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Represents one row in the {@code DaRec} record table.
 *
 * <p>Every row loaded from every source is stored here as a JSON blob.
 * All rows from the same pipeline run share the same {@link #daId} (FK → {@code DaRefer.da_id}).
 *
 * <h2>BQ table schema (DaRec)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.DaRec (
 *   rec_id         STRING    NOT NULL,   -- UUID generated per row
 *   da_id          INT64     NOT NULL,   -- FK → DaRefer.da_id
 *   row_da_json_tx STRING,               -- source row serialised as JSON after transforms
 *   load_dt        DATE      NOT NULL,   -- partition column; set once per run
 *   lst_updt_ts    TIMESTAMP NOT NULL
 * ) PARTITION BY load_dt;
 * }</pre>
 */
public final class DataSourceRecord {

    public final String    recId;
    public final long      daId;
    public final String    rowDaJsonTx;
    public final LocalDate loadDt;
    public final Instant   lstUpdtTs;

    public DataSourceRecord(String recId, long daId, String rowDaJsonTx,
                            LocalDate loadDt, Instant lstUpdtTs) {
        this.recId       = recId;
        this.daId        = daId;
        this.rowDaJsonTx = rowDaJsonTx;
        this.loadDt      = loadDt;
        this.lstUpdtTs   = lstUpdtTs;
    }
}
