package com.yourco.beam.model;

import java.io.Serializable;

/**
 * One declared column in a data source's explicit schema, as entered by an operator in
 * {@code parameter_store}'s {@code bq_schema_json} (see {@link BqFetchConfig#schema}).
 *
 * <p>{@link #bqType} uses real BigQuery SQL type names — {@code STRING}, {@code INT64},
 * {@code FLOAT64}, {@code BOOLEAN}, {@code BYTES}, {@code DATE}, {@code DATETIME},
 * {@code TIME}, {@code TIMESTAMP}, {@code NUMERIC}, {@code BIGNUMERIC} — the same names
 * documented at cloud.google.com/bigquery/docs/reference/standard-sql/data-types, so the
 * person editing the config recognises them directly rather than an internal Beam
 * {@code Schema.FieldType} name.
 */
public final class SourceSchemaField implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String columnName;
    public final String bqType;

    public SourceSchemaField(String columnName, String bqType) {
        this.columnName = columnName;
        this.bqType     = bqType;
    }

    @Override
    public String toString() {
        return columnName + ":" + bqType;
    }
}
