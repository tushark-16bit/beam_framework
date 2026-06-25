package com.yourco.beam.model;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Represents one row in the {@code DaRec} record table.
 *
 * <p>Every row loaded from every source is stored here as a JSON blob.
 * All rows from the same pipeline run share the same {@link #DaId} (FK → {@code DaRefer.DaId}).
 *
 * <h2>BQ table schema (DaRec)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.DaRec (
 *   RecId        STRING    NOT NULL,   -- UUID generated per row
 *   DaId         INT64     NOT NULL,   -- FK → DaRefer.DaId
 *   RowDaJsonTx  STRING,               -- source row serialised as JSON after transforms
 *   LoadDt       DATE      NOT NULL,   -- partition column; set once per run
 *   LstUpdtTs    TIMESTAMP NOT NULL
 * ) PARTITION BY LoadDt;
 * }</pre>
 */
public final class DataSourceRecord {

    public final String    RecId;
    public final long      DaId;
    public final String    RowDaJsonTx;
    public final LocalDate LoadDt;
    public final Instant   LstUpdtTs;

    public DataSourceRecord(String RecId, long DaId, String RowDaJsonTx,
                            LocalDate LoadDt, Instant LstUpdtTs) {
        this.RecId       = RecId;
        this.DaId        = DaId;
        this.RowDaJsonTx = RowDaJsonTx;
        this.LoadDt      = LoadDt;
        this.LstUpdtTs   = LstUpdtTs;
    }
}
