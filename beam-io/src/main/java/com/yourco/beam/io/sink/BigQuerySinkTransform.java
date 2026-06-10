package com.yourco.beam.io.sink;

import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.WriteDispositionType;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Objects;

/**
 * Writes a {@code PCollection<Row>} to a BigQuery table.
 *
 * <h2>Idempotency (I2 fix)</h2>
 * The default write disposition is now {@link WriteDispositionType#TRUNCATE}
 * (full-refresh semantics, safe to re-run). Use {@code --writeDisposition=APPEND}
 * only when appending is intentional — note that APPEND is not idempotent and
 * re-running the pipeline will duplicate rows.
 *
 * <p>The table must already exist ({@code CREATE_NEVER}). For auto-creation,
 * switch to {@code CREATE_IF_NEEDED} and supply a {@code TableSchema}.
 */
public final class BigQuerySinkTransform extends PTransform<PCollection<Row>, PDone> {

    private static final long serialVersionUID = 1L;

    private final String destinationTable;
    private final BigQueryIO.Write.WriteDisposition writeDisposition;

    public BigQuerySinkTransform(FrameworkOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        this.destinationTable = Objects.requireNonNull(
                options.getBqSinkTable(),
                "sinkType=BQ requires --bqSinkTable, e.g. project:dataset.table");
        this.writeDisposition = toBeamDisposition(options.getWriteDisposition());
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return input
                .apply("Row-to-TableRow", MapElements
                        .into(TypeDescriptor.of(TableRow.class))
                        .via(new RowToTableRowFn()))
                .apply("WriteTo-BQ", BigQueryIO.writeTableRows()
                        .to(destinationTable)
                        .withWriteDisposition(writeDisposition)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
    }

    private static BigQueryIO.Write.WriteDisposition toBeamDisposition(WriteDispositionType type) {
        return switch (type) {
            case APPEND   -> BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
            case TRUNCATE -> BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
        };
    }

    /** Named static SerializableFunction — safe for Beam worker serialization. */
    private static final class RowToTableRowFn
            implements SerializableFunction<Row, TableRow> {

        private static final long serialVersionUID = 1L;

        @Override
        public TableRow apply(Row row) {
            TableRow tableRow = new TableRow();
            row.getSchema().getFields()
               .forEach(f -> tableRow.set(f.getName(), row.getValue(f.getName())));
            return tableRow;
        }
    }
}
