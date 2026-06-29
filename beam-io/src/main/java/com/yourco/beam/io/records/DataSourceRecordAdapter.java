package com.yourco.beam.io.records;

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
}
