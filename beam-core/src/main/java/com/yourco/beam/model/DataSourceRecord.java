package com.yourco.beam.model;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Represents one row in the record table.
 *
 * <p>Every row loaded from every source is stored here as a JSON blob.
 * Replaces per-source output BQ tables — all sources write to the same record table.
 * Rows from the same pipeline run share the same {@link #dataSourceId}.
 *
 * <p>When a source is loaded in shards or Beam bundles, all resulting rows
 * share the same {@code dataSourceId} (set in the driver JVM before {@code pipeline.run()}).
 * Consumers join on {@code dataSourceId} to retrieve all rows for a run.
 *
 * <p>For REPORT_PROCESSING, output rows of a generated report are stored here
 * using the report's {@code dataSourceId} from the checkpoint table.
 *
 * <h2>BQ table schema</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.data_source_records (
 *   RecId          STRING    NOT NULL,   -- UUID generated per row
 *   dataSourceId   INT64     NOT NULL,   -- FK → data_source_checkpoints.dataSourceId
 *   RowDSJsonTx    STRING,               -- source row serialised as JSON
 *   LoadDt         DATE      NOT NULL,
 *   LstUpdtTs      TIMESTAMP NOT NULL
 * );
 * }</pre>
 */
public final class DataSourceRecord {

    public final String    recId;
    public final long      dataSourceId;
    public final String    rowDsJsonTx;
    public final LocalDate loadDt;
    public final Instant   lstUpdtTs;

    public DataSourceRecord(String recId, long dataSourceId, String rowDsJsonTx,
                            LocalDate loadDt, Instant lstUpdtTs) {
        this.recId        = recId;
        this.dataSourceId = dataSourceId;
        this.rowDsJsonTx  = rowDsJsonTx;
        this.loadDt       = loadDt;
        this.lstUpdtTs    = lstUpdtTs;
    }
}
