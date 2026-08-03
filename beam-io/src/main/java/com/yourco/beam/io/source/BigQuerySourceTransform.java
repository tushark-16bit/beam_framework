package com.yourco.beam.io.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.common.io.BaseEncoding;
import com.yourco.beam.model.Schemas;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads from a BigQuery table or SQL query and produces a {@code PCollection<Row>}.
 *
 * <h2>Schema handling</h2>
 * Two modes:
 * <ul>
 *   <li><b>Typed</b> (preferred): pass a pre-fetched {@link Schema} from
 *       {@code BigQuerySchemaUtils.fetchBeamSchema()} (called in the driver JVM by the
 *       caller in beam-runner). Each {@link TableRow} is converted field-by-field using
 *       native types: INT64, DOUBLE, BOOLEAN as their Java equivalents; TIMESTAMP/DATE/
 *       DATETIME/TIME and STRING kept as {@code String} (ISO format from JSON encoding).
 *       Does NOT use {@code BigQueryUtils.toBeamRow()} — that API assumes Avro encoding
 *       and throws {@link NumberFormatException} on ISO temporal strings.</li>
 *   <li><b>Generic fallback</b>: pass {@code null} for schema (or use the two-arg
 *       constructor). Used for query-only sources, or when the caller's
 *       {@code BigQuerySchemaUtils.fetchBeamSchema()} call fails (e.g. no
 *       {@code bigquery.tables.get} permission on the table). At {@code expand()} time
 *       (driver JVM, pipeline-assembly), this runs a lightweight
 *       {@code SELECT * FROM (...) LIMIT 1} preview query to learn the real column
 *       <em>names</em> — this only needs query-execution rights, not table-metadata
 *       access — and builds one nullable-STRING field per column, applied consistently
 *       to both {@code .setRowSchema()} and every emitted {@link Row}. If even the
 *       preview query fails, falls back further to {@link Schemas#RAW_JSON} — a single
 *       {@code raw_json} blob field containing the whole row as JSON text, matching the
 *       convention used by {@code GcsSourceTransform} / {@code PubSubSourceTransform}.</li>
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
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceTransform.class);

    private final String bqTable;
    private final String bqQuery;
    // Nullable: when non-null, typed conversion is used; when null, generic fallback.
    // Schema is Serializable so it can be baked into a Classic Template graph.
    private final Schema schema;

    /** Generic fallback constructor — column names resolved via preview query in {@code expand()}. */
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

        // Generic fallback: resolve real column names via a preview query (driver JVM,
        // here in expand()), then apply that SAME schema to every Row so the PCollection's
        // declared schema always matches what is actually emitted.
        Schema genericSchema = resolveGenericSchema();
        boolean blobFallback = genericSchema.equals(Schemas.RAW_JSON);
        return tableRows
                .apply("TableRow-to-Row", MapElements
                        .into(TypeDescriptor.of(Row.class))
                        .via(new GenericTableRowToRowFn(genericSchema, blobFallback)))
                .setRowSchema(genericSchema);
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
     * Learns real column names for the generic fallback by running a lightweight
     * {@code SELECT * ... LIMIT 1} preview query in the driver JVM at pipeline-assembly time
     * ({@code expand()} runs once here, not per-worker).
     *
     * <p>Unlike {@code BigQuerySchemaUtils.fetchBeamSchema()}, this never calls
     * {@code Tables.get()} — it only needs query-execution permission, so it still works when
     * table-metadata read access ({@code bigquery.tables.get}) is unavailable for a table.
     * Every column becomes a nullable STRING field, matching the "generic" contract: no type
     * guessing, just real column names with string-coerced values.
     *
     * <p>If the preview query itself fails (no read access at all, or a transient error),
     * falls back to {@link Schemas#RAW_JSON} — a single blob field with the whole row as
     * JSON text — so the pipeline can still run.
     */
    private Schema resolveGenericSchema() {
        String previewSql = (bqQuery != null && !bqQuery.isBlank())
                ? "SELECT * FROM (" + bqQuery + ") LIMIT 1"
                : "SELECT * FROM `" + bqTable.replace(':', '.') + "` LIMIT 1";
        try {
            BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
            TableResult result = bq.query(QueryJobConfiguration.newBuilder(previewSql)
                    .setUseLegacySql(false)
                    .build());

            Schema.Builder builder = Schema.builder();
            result.getSchema().getFields().forEach(f -> builder.addNullableStringField(f.getName()));
            Schema resolved = builder.build();

            if (resolved.getFieldCount() == 0) {
                LOG.warn("Preview query for generic BQ fallback returned zero columns — "
                         + "falling back to raw_json blob schema. sql={}", previewSql);
                return Schemas.RAW_JSON;
            }
            LOG.info("Resolved {} column name(s) for generic BQ fallback via preview query",
                     resolved.getFieldCount());
            return resolved;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ column-name preview query interrupted", e);
        } catch (Exception e) {
            LOG.warn("Could not resolve column names for generic BQ fallback ({}) — "
                     + "falling back to raw_json blob schema.", e.getMessage());
            return Schemas.RAW_JSON;
        }
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
     * Typed conversion from a JSON-encoded {@link TableRow} (produced by
     * {@link BigQueryIO#readTableRows()}) to a Beam {@link Row}.
     *
     * <p>{@link BigQueryIO#readTableRows()} uses JSON encoding, not Avro, so field values
     * arrive as Java types from the JSON deserializer:
     * <ul>
     *   <li>INTEGER / FLOAT → {@code String} (e.g. {@code "123"}, {@code "3.14"})</li>
     *   <li>BOOLEAN → {@code Boolean} or {@code String} {@code "true"/"false"}</li>
     *   <li>TIMESTAMP / DATE / DATETIME / TIME → {@code String} (ISO format)</li>
     *   <li>BYTES → {@code String} (base64)</li>
     *   <li>STRING → {@code String}</li>
     * </ul>
     * We do NOT use {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils#toBeamRow}
     * here because that method assumes Avro encoding and tries to parse temporal fields as
     * epoch-float numbers, causing {@link NumberFormatException} on ISO strings.
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
            Row.Builder builder = Row.withSchema(schema);
            for (Schema.Field field : schema.getFields()) {
                Object raw = tableRow.get(field.getName());
                builder.addValue(convertValue(field.getType(), raw));
            }
            return builder.build();
        }

        private static Object convertValue(Schema.FieldType fieldType, Object raw) {
            if (raw == null) return null;
            String s = raw.toString();
            switch (fieldType.getTypeName()) {
                case INT64:   return Long.parseLong(s);
                case DOUBLE:  return Double.parseDouble(s);
                case BOOLEAN: return raw instanceof Boolean ? (Boolean) raw : Boolean.parseBoolean(s);
                case BYTES:   return BaseEncoding.base64().decode(s);
                default:      return s; // STRING and all temporal types (already ISO strings)
            }
        }
    }

    /**
     * Generic fallback: builds each {@link Row} against the SAME {@link Schema} resolved once
     * by {@link #resolveGenericSchema()} — never a per-row schema — so every emitted Row matches
     * the PCollection's declared {@code .setRowSchema()} exactly.
     *
     * <p>Two shapes, selected by {@code blobFallback}:
     * <ul>
     *   <li>{@code false} (normal case): one nullable STRING field per resolved column name.
     *       Each value is coerced via {@code toString()}; a column missing from a given
     *       {@link TableRow} (should not normally happen — BigQuery includes every column,
     *       {@code null} or not) becomes {@code null} rather than throwing.</li>
     *   <li>{@code true}: {@code schema} is {@link Schemas#RAW_JSON} — the whole row is
     *       serialised to a single {@code raw_json} field as a last resort.</li>
     * </ul>
     */
    private static final class GenericTableRowToRowFn
            implements SerializableFunction<TableRow, Row> {

        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private final Schema  schema;
        private final boolean blobFallback;

        GenericTableRowToRowFn(Schema schema, boolean blobFallback) {
            this.schema       = schema;
            this.blobFallback = blobFallback;
        }

        @Override
        public Row apply(TableRow tableRow) {
            if (blobFallback) {
                return Row.withSchema(schema).addValue(toJson(tableRow)).build();
            }
            Row.Builder builder = Row.withSchema(schema);
            for (Schema.Field field : schema.getFields()) {
                Object raw = tableRow.get(field.getName());
                builder.addValue(raw != null ? raw.toString() : null);
            }
            return builder.build();
        }

        private static String toJson(TableRow row) {
            try {
                return MAPPER.writeValueAsString(row);
            } catch (Exception e) {
                return String.valueOf(row);
            }
        }
    }
}
