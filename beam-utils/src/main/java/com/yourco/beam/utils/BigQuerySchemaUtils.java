package com.yourco.beam.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utilities for fetching and converting BigQuery table schemas at pipeline-assembly time.
 *
 * <h2>When to use this</h2>
 * Call these methods inside source or transform constructors — in the <em>driver JVM</em>
 * before the pipeline is submitted to Dataflow. Do NOT call them inside a {@code DoFn}
 * because each worker would make a separate BQ API call, causing unnecessary load and latency.
 *
 * <h2>Why the default BQ source doesn't use this yet</h2>
 * {@link com.yourco.beam.io.source.BigQuerySourceTransform} currently emits a generic
 * string schema because it doesn't know the table structure at pipeline-build time.
 * Use {@link #fetchBeamSchema(String)} to get the real schema and pass it to the source
 * so downstream transforms see actual column names like {@code order_id}, {@code email}.
 *
 * <h2>Example usage in a custom source</h2>
 * <pre>{@code
 * Schema schema = BigQuerySchemaUtils.fetchBeamSchema("my-project:my-dataset.orders");
 * PCollection<Row> rows = pipeline.apply(
 *     BigQueryIO.<Row>read(record ->
 *         BigQueryUtils.toBeamRow(schema, record.getRecord()))
 *     .from("my-project:my-dataset.orders")
 *     .withCoder(SchemaCoder.of(schema, TypeDescriptor.of(Row.class),
 *         r -> null, r -> r)));
 * rows.setRowSchema(schema);
 * }</pre>
 */
public final class BigQuerySchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySchemaUtils.class);

    // BQ field type → Beam Schema field type mapping
    private static final Map<String, Schema.FieldType> TYPE_MAP = Map.of(
            "STRING",    Schema.FieldType.STRING,
            "INTEGER",   Schema.FieldType.INT64,
            "INT64",     Schema.FieldType.INT64,
            "FLOAT",     Schema.FieldType.DOUBLE,
            "FLOAT64",   Schema.FieldType.DOUBLE,
            "BOOLEAN",   Schema.FieldType.BOOLEAN,
            "BOOL",      Schema.FieldType.BOOLEAN,
            "BYTES",     Schema.FieldType.BYTES,
            "TIMESTAMP", Schema.FieldType.DATETIME,
            "DATE",      Schema.FieldType.DATETIME
    );

    private BigQuerySchemaUtils() {}

    /**
     * Fetches the Beam {@link Schema} for a BigQuery table using application-default credentials.
     *
     * <p>Call this in the driver JVM at pipeline-assembly time, not inside a DoFn.
     *
     * @param tableRef BigQuery table reference in format {@code project:dataset.table}
     *                 or {@code project.dataset.table}
     * @return Beam {@link Schema} with all top-level columns as nullable fields
     * @throws IllegalArgumentException if the table does not exist or is not accessible
     */
    public static Schema fetchBeamSchema(String tableRef) {
        LOG.info("Fetching BQ schema for table: {}", tableRef);

        BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
        TableId tableId = parseTableRef(tableRef);
        Table table = bq.getTable(tableId);

        if (table == null) {
            throw new IllegalArgumentException(
                "BigQuery table not found or not accessible: " + tableRef);
        }

        com.google.cloud.bigquery.Schema bqSchema =
                table.<StandardTableDefinition>getDefinition().getSchema();

        if (bqSchema == null) {
            throw new IllegalArgumentException(
                "Table has no schema (external or partitioned without schema?): " + tableRef);
        }

        Schema.Builder builder = Schema.builder();
        for (Field field : bqSchema.getFields()) {
            Schema.FieldType beamType = TYPE_MAP.getOrDefault(
                    field.getType().name(), Schema.FieldType.STRING);
            builder.addNullableField(field.getName(), beamType);
        }

        Schema schema = builder.build();
        LOG.info("Fetched {} field(s) from {}", schema.getFieldCount(), tableRef);
        return schema;
    }

    /**
     * Returns the number of rows in the given table. Useful for pre-flight validation
     * in report pipelines (e.g., check source table is not empty before running).
     *
     * @param tableRef BigQuery table reference
     * @return approximate row count, or -1 if unavailable
     */
    public static long fetchRowCount(String tableRef) {
        BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
        Table table = bq.getTable(parseTableRef(tableRef));
        if (table == null) return -1L;
        StandardTableDefinition def = table.getDefinition();
        Long rows = def.getNumRows();
        return rows != null ? rows : -1L;
    }

    /**
     * Returns {@code true} if the given table exists and is accessible.
     * Safe to call at pipeline-assembly time for pre-flight checks.
     */
    public static boolean tableExists(String tableRef) {
        try {
            BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
            return bq.getTable(parseTableRef(tableRef)) != null;
        } catch (Exception e) {
            LOG.warn("Could not check table existence for {}: {}", tableRef, e.getMessage());
            return false;
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Parses a BQ table reference in either {@code project:dataset.table}
     * or {@code project.dataset.table} format.
     */
    private static TableId parseTableRef(String tableRef) {
        // Normalise project:dataset.table → project.dataset.table
        String normalised = tableRef.replace(':', '.');
        String[] parts = normalised.split("\\.");
        if (parts.length == 3) {
            return TableId.of(parts[0], parts[1], parts[2]);
        }
        if (parts.length == 2) {
            return TableId.of(parts[0], parts[1]);
        }
        throw new IllegalArgumentException(
            "Invalid BigQuery table reference: '" + tableRef
            + "'. Expected format: project:dataset.table or dataset.table");
    }
}
