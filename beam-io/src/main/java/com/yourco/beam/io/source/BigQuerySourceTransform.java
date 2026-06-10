package com.yourco.beam.io.source;

import com.yourco.beam.options.FrameworkOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Objects;

/**
 * Reads from a BigQuery table or SQL query and produces a {@code PCollection<Row>}.
 *
 * <h2>Schema handling</h2>
 * BigQuery table columns are mapped to a flat string-value Beam schema.
 * Each {@link TableRow} field becomes a nullable {@code STRING} column in the
 * output schema. For typed schemas (NUMERIC, TIMESTAMP, etc.), replace
 * {@link #buildSchema(TableRow)} with schema inference from the BQ table
 * metadata using {@code BigQueryUtils.fromTableSchema(tableSchema)}.
 *
 * <h2>Configuration</h2>
 * <ul>
 *   <li><b>Table</b>: {@code --bqSourceTable=project:dataset.table}</li>
 *   <li><b>Query</b>: {@code --bqSourceQuery=SELECT ...} (takes precedence)</li>
 * </ul>
 *
 * <h2>I6 fix</h2>
 * Extends {@link PTransform} so Beam's graph-building hooks are invoked and
 * the source appears as a labelled node in the Dataflow UI.
 */
public final class BigQuerySourceTransform extends PTransform<PBegin, PCollection<Row>> {

    private static final long serialVersionUID = 1L;

    private final String bqTable;
    private final String bqQuery;

    public BigQuerySourceTransform(FrameworkOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        this.bqTable = options.getBqSourceTable();
        this.bqQuery = options.getBqSourceQuery();
        validateOptions();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        PCollection<TableRow> tableRows = input.apply("ReadFrom-BQ", buildRead());

        // Convert each TableRow to a Beam Row using all-string schema derived from row fields.
        // This is a generic approach that works without knowing the schema at pipeline-build time.
        // For production use with typed schemas, replace with BigQueryUtils.toBeamRow(schema, tableRow).
        return tableRows
                .apply("TableRow-to-Row", MapElements
                        .into(TypeDescriptor.of(Row.class))
                        .via(new TableRowToRowFn()))
                // Schema is set per-element inside the DoFn; set a sentinel schema here so
                // Beam's schema-aware code paths are activated. In production, derive the real
                // schema from BigQuery table metadata and set it here instead.
                .setRowSchema(buildGenericSchema());
    }

    private BigQueryIO.TypedRead<TableRow> buildRead() {
        if (bqQuery != null && !bqQuery.isBlank()) {
            return BigQueryIO.readTableRows()
                    .fromQuery(bqQuery)
                    .usingStandardSql();
        }
        return BigQueryIO.readTableRows().from(bqTable);
    }

    /**
     * Generic fallback schema: a single BYTES field holding the full row as a JSON string.
     * Replace with a real schema derived from BQ table metadata for column-level transforms.
     */
    private static Schema buildGenericSchema() {
        return Schema.builder()
                .addNullableStringField("_row_json")
                .build();
    }

    /** Converts a {@link TableRow} to a {@link Row} by serializing the map to a JSON string. */
    private static Schema buildSchema(TableRow tableRow) {
        Schema.Builder builder = Schema.builder();
        tableRow.keySet().forEach(key -> builder.addNullableStringField(key));
        return builder.build();
    }

    private void validateOptions() {
        boolean hasTable = bqTable != null && !bqTable.isBlank();
        boolean hasQuery = bqQuery != null && !bqQuery.isBlank();
        if (!hasTable && !hasQuery) {
            throw new IllegalArgumentException(
                "sourceType=BQ requires either --bqSourceTable or --bqSourceQuery");
        }
    }

    // ── Named SerializableFunction — safe for Beam serialization ─────────────

    /** Maps a {@link TableRow} to a single-field {@link Row} holding each column as a string. */
    private static final class TableRowToRowFn
            implements SerializableFunction<TableRow, Row> {

        private static final long serialVersionUID = 1L;

        @Override
        public Row apply(TableRow tableRow) {
            // Build a per-row schema from the actual fields present
            Schema schema = buildSchema(tableRow);
            Row.Builder builder = Row.withSchema(schema);
            tableRow.keySet().forEach(key -> {
                Object value = tableRow.get(key);
                builder.addValue(value != null ? value.toString() : null);
            });
            return builder.build();
        }
    }
}
