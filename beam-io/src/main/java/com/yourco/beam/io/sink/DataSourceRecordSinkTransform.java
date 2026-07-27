package com.yourco.beam.io.sink;

import com.google.api.services.bigquery.model.TableRow;
import com.yourco.beam.io.util.JsonUtils;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.ValueProvider;
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
 * Writes a {@code PCollection<Row>} to the {@code DaRec} record table.
 *
 * <p>Every row is serialised as a JSON blob in {@code row_da_json_tx}.
 * All rows from one run share the same {@code da_id} (FK → {@code DaRefer.da_id}).
 *
 * <p>{@code load_dt} and {@code lst_updt_ts} are captured once in the constructor so that
 * retried Beam bundles and runs spanning midnight all land in the same partition.
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
public final class DataSourceRecordSinkTransform extends PTransform<PCollection<Row>, PDone> {

    private static final long serialVersionUID = 1L;

    private final String             recordTableRef; // project:dataset.table — BigQueryIO format
    private final ValueProvider<Long> daId;          // resolved at runtime for Classic Template support
    private final String             loadDt;         // captured once — all rows in this run share the same date
    private final String             lstUpdtTs;      // captured once — avoids per-element clock calls

    /**
     * Flex Template / DirectRunner constructor: checkpoint has already been created and
     * {@code daId} is known. Wraps it in a StaticValueProvider so the graph stays generic.
     */
    public DataSourceRecordSinkTransform(FrameworkOptions options, long daId) {
        this(options, ValueProvider.StaticValueProvider.of(daId));
    }

    /**
     * Classic Template constructor: {@code daId} is a runtime ValueProvider resolved when
     * the template is launched with {@code --daId=<N>}. The LOADING checkpoint row must
     * have been created by the Airflow pre-setup task before launching the template.
     */
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
    public PDone expand(PCollection<Row> input) {
        input
            .apply("Row-to-DaRecRow", MapElements
                .into(TypeDescriptor.of(TableRow.class))
                .via(new RowToDaRecFn(daId, loadDt, lstUpdtTs)))  // daId resolved at worker startup
            .apply("WriteTo-DaRec", BigQueryIO.writeTableRows()
                .to(recordTableRef)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
        return PDone.in(input.getPipeline());
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
