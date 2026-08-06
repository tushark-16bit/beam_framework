package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Optional post-storage SQL transform applied to one data source's freshly downloaded rows,
 * within the same {@code DATA_SOURCE_DOWNLOAD} run.
 *
 * <p>Runs in {@code PostDownloadFinalizeTransform} — after rows are written to {@code DaRec}
 * and after the existing row-count/BnC storage-integrity checks pass, but before the checkpoint
 * is marked {@code COMPLETED}. {@link #query} is real BigQuery Standard SQL, written against a
 * single {@code {data}} token that resolves to a subquery reunifying every paginated {@code DaRec}
 * page for this run into one flat rowset of JSON strings — the same
 * {@code CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx))} pattern already used elsewhere
 * in this framework, so pagination is invisible to the operator's SQL. Each row in {@code {data}}
 * is a JSON object string; extract fields with {@code JSON_VALUE(row_json, '$.field')}.
 *
 * <p>Example:
 * <pre>{@code
 * SELECT JSON_VALUE(row_json,'$.trade_id') AS trade_id,
 *        ROUND(CAST(JSON_VALUE(row_json,'$.amount') AS FLOAT64) * 1.1, 2) AS amount_with_tax
 * FROM {data}
 * WHERE CAST(JSON_VALUE(row_json,'$.amount') AS FLOAT64) > 0
 * }</pre>
 *
 * <p>If the transform's output row count fails {@link #minRowCount}/{@link #maxRowCount}, or the
 * query itself errors, the original stored rows are left untouched and the run fails with
 * {@code FAILED_TRANSFORM} — the transform only replaces {@code DaRec}'s rows for this run once
 * its output has been validated.
 *
 * <p>Stored in the {@code source_config} table (parameters_val_json) as:
 * {@code data_transform_query}, {@code data_transform_min_row_count},
 * {@code data_transform_max_row_count}.
 */
public final class DataTransformConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final long NO_MIN = 0L;
    public static final long NO_MAX = -1L;

    /** SQL against the {@code {data}} token. Null means no transform is configured. */
    public final String query;

    /** Minimum acceptable row count in the transform's output. {@code 0} = no check. */
    public final long minRowCount;

    /** Maximum acceptable row count in the transform's output. {@code -1} = no check. */
    public final long maxRowCount;

    public DataTransformConfig(String query, long minRowCount, long maxRowCount) {
        this.query       = (query != null && !query.isBlank()) ? query : null;
        this.minRowCount = minRowCount;
        this.maxRowCount = maxRowCount;
    }

    public boolean hasQuery()       { return query != null; }
    public boolean hasMinRowCheck() { return minRowCount > 0; }
    public boolean hasMaxRowCheck() { return maxRowCount >= 0; }

    public static DataTransformConfig none() {
        return new DataTransformConfig(null, NO_MIN, NO_MAX);
    }
}
