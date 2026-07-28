package com.yourco.beam.io.sink;

import com.google.api.services.bigquery.model.TableRow;
import com.yourco.beam.io.util.JsonUtils;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Writes a {@code PCollection<Row>} to the {@code DaRec} record table using streaming inserts.
 *
 * <p>Every row is serialised as a JSON blob in {@code row_da_json_tx}.
 * All rows from one run share the same {@code da_id} (FK → {@code DaRefer.da_id}).
 *
 * <p>Returns a {@code PCollection<Long>} with exactly one element — the count of rows
 * successfully inserted into DaRec. This element is only emitted after ALL streaming inserts
 * (both successful and failed) have been processed. Downstream transforms use this as a
 * {@link Wait} signal to ensure they run after the writes are committed.
 *
 * <p>Streaming inserts are used (rather than FILE_LOADS) so that written rows are immediately
 * queryable by the post-write finalization step ({@code PostDownloadFinalizeTransform}).
 * FILE_LOADS commits asynchronously in a BQ load job; streaming inserts are committed
 * per-row and visible in SELECT queries immediately.
 *
 * <p>The DaRec table must already exist ({@code CREATE_NEVER}):
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.DaRec (
 *   rec_id         STRING    NOT NULL,
 *   da_id          INT64     NOT NULL,
 *   row_da_json_tx STRING,
 *   load_dt        DATE      NOT NULL,
 *   lst_updt_ts    TIMESTAMP NOT NULL
 * ) PARTITION BY load_dt;
 * }</pre>
 */
public final class DataSourceRecordSinkTransform extends PTransform<PCollection<Row>, PCollection<Long>> {

    private static final long serialVersionUID = 1L;

    private final String              recordTableRef; // project:dataset.table — BigQueryIO format
    private final ValueProvider<Long> daId;           // resolved at runtime for Classic Template support
    private final String              loadDt;         // captured once — all rows in this run share the same date
    private final String              lstUpdtTs;      // captured once — avoids per-element clock calls

    /**
     * Standard constructor: {@code daId} is known at assembly time.
     */
    public DataSourceRecordSinkTransform(FrameworkOptions options, long daId) {
        this(options, ValueProvider.StaticValueProvider.of(daId));
    }

    public DataSourceRecordSinkTransform(FrameworkOptions options, ValueProvider<Long> daId) {
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        this.recordTableRef = project + ":" + options.getCheckpointBqDataset()
                            + "." + options.getDaRecTable();
        this.daId      = daId;
        this.loadDt    = LocalDate.now(ZoneOffset.UTC).toString();
        this.lstUpdtTs = Instant.now().toString();
    }

    @Override
    public PCollection<Long> expand(PCollection<Row> input) {
        PCollection<TableRow> tableRows = input
            .apply("Row-to-DaRecRow", MapElements
                .into(TypeDescriptor.of(TableRow.class))
                .via(new RowToDaRecFn(daId, loadDt, lstUpdtTs)));

        WriteResult writeResult = tableRows
            .apply("WriteTo-DaRec", BigQueryIO.writeTableRows()
                .to(recordTableRef)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withExtendedErrorInfo()
                .withSuccessfulInsertsPropagation(true));

        // Count successful inserts, then wait for failed inserts to drain before emitting.
        // The single Long element produced here is the trigger that PostDownloadFinalizeTransform
        // waits on — it is only emitted after every streaming insert (success or failure) is done.
        return writeResult.getSuccessfulInserts()
            .apply("Count-DaRec-Inserts", Count.globally())
            .apply("WaitForFailedInserts", Wait.on(writeResult.getFailedInsertsWithErr()));
    }

    /** Serializable — safe for Beam worker serialization. ValueProvider resolved at worker startup. */
    private static final class RowToDaRecFn implements SerializableFunction<Row, TableRow> {

        private static final long serialVersionUID = 1L;

        private final ValueProvider<Long> daId;
        private final String loadDt;
        private final String lstUpdtTs;

        RowToDaRecFn(ValueProvider<Long> daId, String loadDt, String lstUpdtTs) {
            this.daId      = daId;
            this.loadDt    = loadDt;
            this.lstUpdtTs = lstUpdtTs;
        }

        @Override
        public TableRow apply(Row row) {
            return new TableRow()
                .set("rec_id",         UUID.randomUUID().toString())
                .set("da_id",          daId.get())
                .set("row_da_json_tx", JsonUtils.rowToJson(row))
                .set("load_dt",        loadDt)
                .set("lst_updt_ts",    lstUpdtTs);
        }
    }
}
