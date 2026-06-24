package com.yourco.beam.io.sink;

import com.google.api.services.bigquery.model.TableRow;
import com.yourco.beam.io.util.JsonUtils;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Writes a {@code PCollection<Row>} to the unified data-source record table.
 *
 * <p>Every row is stored as a JSON blob in {@code RowDSJsonTx}. This replaces
 * per-source output BQ tables — all sources (BQ, API, file) and all reports write
 * to the same record table.
 *
 * <p>All rows from one run share the same {@code dataSourceId} (generated in
 * the driver JVM by {@link com.yourco.beam.io.checkpoint.DataSourceCheckpointAdapter#createCheckpoint}).
 * Consumers join on {@code dataSourceId} to retrieve all rows for a run.
 *
 * <p>The record table must already exist ({@code CREATE_NEVER}):
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.data_source_records (
 *   RecId          STRING    NOT NULL,
 *   dataSourceId   INT64     NOT NULL,
 *   RowDSJsonTx    STRING,
 *   LoadDt         DATE      NOT NULL,
 *   LstUpdtTs      TIMESTAMP NOT NULL
 * );
 * }</pre>
 */
public final class DataSourceRecordSinkTransform extends PTransform<PCollection<Row>, PDone> {

    private static final long serialVersionUID = 1L;

    private final String recordTableRef; // project:dataset.table — BigQueryIO format
    private final long   dataSourceId;

    public DataSourceRecordSinkTransform(FrameworkOptions options, long dataSourceId) {
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        this.recordTableRef = project + ":" + options.getCheckpointBqDataset()
                            + "." + options.getRecordBqTable();
        this.dataSourceId = dataSourceId;
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        input
            .apply("Row-to-RecordTableRow", MapElements
                .into(TypeDescriptor.of(TableRow.class))
                .via(new RowToRecordTableRowFn(dataSourceId)))
            .apply("WriteTo-RecordTable", BigQueryIO.writeTableRows()
                .to(recordTableRef)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
        return PDone.in(input.getPipeline());
    }

    /** Serializable — safe for Beam worker serialization. */
    private static final class RowToRecordTableRowFn
            implements SerializableFunction<Row, TableRow> {

        private static final long serialVersionUID = 1L;

        private final long dataSourceId;

        RowToRecordTableRowFn(long dataSourceId) {
            this.dataSourceId = dataSourceId;
        }

        @Override
        public TableRow apply(Row row) {
            String now = Instant.now().toString();
            return new TableRow()
                .set("RecId",        UUID.randomUUID().toString())
                .set("dataSourceId", dataSourceId)
                .set("RowDSJsonTx",  JsonUtils.rowToJson(row))
                .set("LoadDt",       LocalDate.now(ZoneOffset.UTC).toString())
                .set("LstUpdtTs",    now);
        }
    }
}
