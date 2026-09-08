package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a BigQuery data source fetched from the parameter DB.
 *
 * <p>Use either {@code table} (simple table read) or {@code query} (SQL override with
 * optional parameter injection). If both are set, {@code query} takes precedence.
 *
 * <h2>Query parameter injection</h2>
 * The {@code query} field may contain placeholder tokens that are resolved at runtime
 * by {@code QueryParameterResolver} before the query is executed:
 * <pre>{@code
 * SELECT * FROM trades
 * WHERE trade_date BETWEEN '{periodStart}' AND '{periodEnd}'
 *   AND exchange = '{exchange}'
 * }</pre>
 *
 * <p>Standard tokens: {@code {periodStart}}, {@code {periodEnd}}, {@code {periodId}},
 * {@code {runDate}}. Additional custom params are supplied via the {@code source_config}
 * table's {@code query_params_json} column (stored in {@link com.yourco.beam.model.QueryConfig}).
 *
 * <p>Corresponding columns in the {@code source_config} table:
 * <pre>
 *   bq_project_id, bq_dataset, bq_table, bq_query, query_params_json, bq_schema_json
 * </pre>
 */
public final class BqFetchConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String projectId;
    public final String dataset;
    /** Table name. Used when {@code query} is null or blank. */
    public final String table;
    /**
     * SQL template. May contain {@code {periodStart}}, {@code {periodEnd}}, {@code {periodId}},
     * {@code {runDate}}, and any custom param keys from {@link #queryParams}.
     * Null means read from {@code table} directly.
     */
    public final String query;

    /**
     * Named parameters that supplement or override the standard tokens in {@link #query}.
     * Values can themselves reference standard tokens, e.g. {@code {"start": "{periodStart}"}}.
     * Populated from the {@code query_params_json} column in {@code source_config}.
     */
    public final Map<String, String> queryParams;

    /**
     * Explicit column schema declared by the operator in {@code bq_schema_json}, as an
     * alternative to {@code BigQuerySchemaUtils.fetchBeamSchema()} (which needs
     * {@code bigquery.tables.get} permission) or the name-only preview-query fallback in
     * {@code BigQuerySourceTransform}.
     *
     * <p>When present, this is authoritative: {@code DataSourcePipelineFactory.fetchBqSchema()}
     * uses it directly (no BQ metadata call at all) and every fetched row is converted strictly
     * against it — a value that does not match its declared type (e.g. non-numeric text in a
     * declared {@code FLOAT64} column) fails the run rather than being silently coerced or
     * dropped. Empty when no {@code bq_schema_json} was provided, in which case schema
     * resolution falls back to the existing behaviour.
     */
    public final List<SourceSchemaField> schema;

    public BqFetchConfig(String projectId, String dataset, String table, String query,
                         Map<String, String> queryParams, List<SourceSchemaField> schema) {
        this.projectId   = projectId;
        this.dataset     = dataset;
        this.table       = table;
        this.query       = (query != null && !query.isBlank()) ? query : null;
        this.queryParams = (queryParams != null) ? Collections.unmodifiableMap(queryParams)
                                                 : Collections.emptyMap();
        this.schema      = (schema != null) ? Collections.unmodifiableList(schema)
                                            : Collections.emptyList();
    }

    /** Backwards-compatible constructor for sources with no declared schema. */
    public BqFetchConfig(String projectId, String dataset, String table, String query,
                         Map<String, String> queryParams) {
        this(projectId, dataset, table, query, queryParams, Collections.emptyList());
    }

    /** Backwards-compatible constructor for sources with no custom query params or declared schema. */
    public BqFetchConfig(String projectId, String dataset, String table, String query) {
        this(projectId, dataset, table, query, Collections.emptyMap(), Collections.emptyList());
    }

    /** Returns the fully qualified table reference {@code project:dataset.table}. */
    public String tableRef() {
        return projectId + ":" + dataset + "." + table;
    }

    /** True if a custom SQL query should be used instead of a direct table read. */
    public boolean hasQuery() {
        return query != null;
    }

    /** True if an explicit column schema was declared via {@code bq_schema_json}. */
    public boolean hasSchema() {
        return !schema.isEmpty();
    }
}
