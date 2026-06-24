package com.yourco.beam.io.records;

/**
 * Queries the record table for post-pipeline validation.
 *
 * <p>Used by {@link com.yourco.beam.runner.DataSourcePipelineFactory} after
 * {@code pipeline.run()} to perform row-count and Balance &amp; Control (BnC) checks
 * against rows written to the record table for a given run.
 *
 * <p>The concrete implementation ({@link BigQueryDataSourceRecordAdapter}) queries
 * {@code RowDSJsonTx} using {@code JSON_VALUE} to extract field values from the
 * JSON blobs stored per row.
 */
public interface DataSourceRecordAdapter {

    /**
     * Returns the number of records written to the record table for this run.
     *
     * @param dataSourceId the run identifier
     * @return row count, or -1 if the table cannot be queried
     */
    long countRecords(long dataSourceId);

    /**
     * Returns the sum of a numeric field extracted from the {@code RowDSJsonTx} JSON blobs
     * for all records of this run.
     *
     * <p>Uses {@code JSON_VALUE(RowDSJsonTx, '$.field')} — the field must be a numeric
     * value stored in the JSON row blob.
     *
     * @param dataSourceId the run identifier
     * @param field        JSON key name (direct child of root)
     * @return the sum, or {@link Double#NaN} if the table cannot be queried
     */
    double sumField(long dataSourceId, String field);
}
