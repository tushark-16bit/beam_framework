package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Configuration for a BigQuery data source fetched from the parameter DB.
 *
 * <p>Use either {@code table} (simple table read) or {@code query} (SQL override).
 * If both are set, {@code query} takes precedence.
 *
 * <p>Corresponding columns in the {@code source_config} table:
 * <pre>
 *   bq_project_id, bq_dataset, bq_table, bq_query
 * </pre>
 */
public final class BqFetchConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String projectId;
    public final String dataset;
    /** Table name. Used when {@code query} is null or blank. */
    public final String table;
    /** Optional SQL override. Standard SQL. Null means read from {@code table} directly. */
    public final String query;

    public BqFetchConfig(String projectId, String dataset, String table, String query) {
        this.projectId = projectId;
        this.dataset   = dataset;
        this.table     = table;
        this.query     = (query != null && !query.isBlank()) ? query : null;
    }

    /** Returns the fully qualified table reference {@code project:dataset.table}. */
    public String tableRef() {
        return projectId + ":" + dataset + "." + table;
    }

    /** True if a custom SQL query should be used instead of a direct table read. */
    public boolean hasQuery() {
        return query != null;
    }
}
