package com.yourco.beam.io.records;

import java.math.BigDecimal;

/**
 * Queries the {@code DaRec} record table for post-pipeline validation.
 *
 * <p>Used by {@link com.yourco.beam.runner.DataSourcePipelineFactory} after
 * {@code pipeline.run()} to perform row-count and Balance &amp; Control (BnC) checks
 * against rows written to DaRec for a given run.
 *
 * <p>The concrete implementation ({@link BigQueryDataSourceRecordAdapter}) queries
 * {@code row_da_json_tx} using {@code JSON_VALUE} to extract field values from the
 * JSON blobs stored per row.
 */
public interface DataSourceRecordAdapter {

    /**
     * Returns the number of records in DaRec for this run.
     *
     * @param daId the run identifier (FK from DaRefer)
     * @return row count, or -1 if the table cannot be queried
     */
    long countRecords(long daId);

    /**
     * Returns the sum of a numeric field extracted from {@code row_da_json_tx} JSON blobs
     * for all records of this run.
     *
     * <p>Uses {@code JSON_VALUE(row_da_json_tx, '$.field')} — the field must be a numeric
     * value at the root of the JSON blob.
     *
     * @param daId  the run identifier
     * @param field JSON key name (direct child of root)
     * @return the sum, or {@link Double#NaN} if the table cannot be queried
     */
    double sumField(long daId, String field);

    /**
     * Deletes all DaRec rows (every page) for a given {@code da_id}.
     *
     * <p>Best-effort: failures are logged and swallowed rather than thrown, since this is
     * always a cleanup step performed after the run it matters to has already reached a
     * terminal status — a delete failure here should never fail or retry the pipeline.
     *
     * <p>Used for two purposes: (1) replacing a run's own rows with the output of its
     * {@code data_transform_query} once that output has been validated, and (2) under
     * {@code --manualOverrun}, removing the superseded previous run's rows once the new
     * run reaches {@code COMPLETED}.
     *
     * @param daId the run identifier whose rows should be removed
     */
    void deleteRecords(long daId);

    /** Row count and the sum of one column, as read back from DaRec for Balance &amp; Control. */
    record Totals(long count, BigDecimal amount) {}

    /**
     * The BnC destination values for a run: {@code COUNT(*)} of the stored rows and the sum of
     * {@code destColumn} over them. {@code destColumn} is matched case-insensitively — for rows
     * keyed by column name against those names; for a FILE source (rows keyed A, B, …) against the
     * header names first, then the column letter. Values that are not numbers count as 0
     * (legacy rule); an empty run is {@code Totals(0, 0)}.
     *
     * @param destColumn the column to sum, or null/blank to read only the count (amount = 0)
     * @throws IllegalArgumentException when the column cannot be found in the stored rows
     * @throws IllegalStateException    when BigQuery cannot be queried — unlike the other methods
     *         here this one never swallows a failure, because a BnC that cannot read its
     *         destination must report an error, not a zero
     */
    Totals countAndSum(long daId, String destColumn);
}
