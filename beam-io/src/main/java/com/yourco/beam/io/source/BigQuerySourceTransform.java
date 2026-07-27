package com.yourco.beam.io.source;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Reads from a BigQuery table or SQL query and produces a {@code PCollection<Row>}.
 *
 * <h2>Schema handling</h2>
 * Two modes:
 * <ul>
 *   <li><b>Typed</b> (preferred): pass a pre-fetched {@link Schema} from
 *       {@code BigQuerySchemaUtils.fetchBeamSchema()} (called in the driver JVM by the
 *       caller in beam-runner). Each {@link TableRow} is converted with
 *       {@link BigQueryUtils#toBeamRow(Schema, TableRow)}, preserving native types
 *       (INT64, DOUBLE, BOOLEAN, DATETIME, etc.).</li>
 *   <li><b>Generic fallback</b>: pass {@code null} for schema (or use the two-arg
 *       constructor). Each field is coerced to a nullable STRING; the column name is
 *       the key from the {@link TableRow} map. Used for query-only sources where
 *       no table exists to inspect, or when schema fetch fails.</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <ul>
 *   <li><b>Table</b>: {@code --bqSourceTable=project:dataset.table}</li>
 *   <li><b>Query</b>: {@code --bqSourceQuery=SELECT ...} (takes precedence)</li>
 * </ul>
 */
public final class BigQuerySourceTransform extends PTransform<PBegin, PCollection<Row>> {

    private static final long serialVersionUID = 1L;

    private final String bqTable;
    private final String bqQuery;
    // Nullable: when non-null, typed conversion is used; when null, generic fallback.
    // Schema is Serializable so it can be baked into a Classic Template graph.
    private final Schema schema;

    /** Generic fallback constructor — schema derived per-element from TableRow keys. */
    public BigQuerySourceTransform(String bqTable, String bqQuery) {
        this(bqTable, bqQuery, null);
    }

    /**
     * Typed constructor — schema pre-fetched at driver-JVM time via
     * {@code BigQuerySchemaUtils.fetchBeamSchema()} in beam-runner.
     *
     * @param schema pre-fetched Beam Schema, or {@code null} to use the generic fallback
     */
    public BigQuerySourceTransform(String bqTable, String bqQuery, Schema schema) {
        this.bqTable = bqTable;
        this.bqQuery = bqQuery;
        this.schema  = schema;
        validateOptions();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        PCollection<TableRow> tableRows = input.apply("ReadFrom-BQ", buildRead());

        if (schema != null) {
            // Typed path: use BigQueryUtils for native type mapping (INT64, DOUBLE, BOOLEAN, etc.)
            return tableRows
                    .apply("TableRow-to-Row", MapElements
                            .into(TypeDescriptor.of(Row.class))
                            .via(new TypedTableRowToRowFn(schema)))
                    .setRowSchema(schema);
        }

        // Generic fallback: all fields as nullable STRING, schema built from first row's keys.
        return tableRows
                .apply("TableRow-to-Row", MapElements
                        .into(TypeDescriptor.of(Row.class))
                        .via(new GenericTableRowToRowFn()))
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
     * Sentinel schema used by the generic fallback so Beam's schema-aware code paths are
     * activated. Downstream transforms should use the per-row schema built by
     * {@link GenericTableRowToRowFn} instead.
     */
    private static Schema buildGenericSchema() {
        return Schema.builder()
                .addNullableStringField("_row_json")
                .build();
    }

    private void validateOptions() {
        boolean hasTable = bqTable != null && !bqTable.isBlank();
        boolean hasQuery = bqQuery != null && !bqQuery.isBlank();
        if (!hasTable && !hasQuery) {
            throw new IllegalArgumentException(
                "sourceType=BQ requires either --bqSourceTable or --bqSourceQuery");
        }
    }

    // ── Named SerializableFunction implementations — safe for Beam serialization ──

    /**
     * Typed conversion: uses {@link BigQueryUtils#toBeamRow(Schema, TableRow)} to map each
     * BQ field to its native Beam type (INT64, DOUBLE, BOOLEAN, DATETIME, etc.).
     * Schema was pre-fetched at driver-JVM time and is stable for all rows in this PCollection.
     */
    private static final class TypedTableRowToRowFn
            implements SerializableFunction<TableRow, Row> {

        private static final long serialVersionUID = 1L;

        private final Schema schema;

        TypedTableRowToRowFn(Schema schema) {
            this.schema = schema;
        }

        @Override
        public Row apply(TableRow tableRow) {
            return BigQueryUtils.toBeamRow(schema, tableRow);
        }
    }

    /**
     * Generic fallback: each field is coerced to a nullable STRING.
     * The schema is built per-row from the TableRow key set.
     * Used when no pre-fetched schema is available (query-only sources, or schema fetch failed).
     */
    private static final class GenericTableRowToRowFn
            implements SerializableFunction<TableRow, Row> {

        private static final long serialVersionUID = 1L;

        @Override
        public Row apply(TableRow tableRow) {
            Schema.Builder schemaBuilder = Schema.builder();
            tableRow.keySet().forEach(key -> schemaBuilder.addNullableStringField(key));
            Schema rowSchema = schemaBuilder.build();
            Row.Builder builder = Row.withSchema(rowSchema);
            tableRow.keySet().forEach(key -> {
                Object value = tableRow.get(key);
                builder.addValue(value != null ? value.toString() : null);
            });
            return builder.build();
        }
    }
}
