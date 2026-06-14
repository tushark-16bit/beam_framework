package com.yourco.beam.io.status;

import com.yourco.beam.model.ProcessStatusRecord;

/**
 * Interface for writing and querying per-source process status records.
 *
 * <p>One row is written per data source per pipeline run, tracking the progression
 * from PENDING → COMPLETED (or FAILED / VALIDATION_FAILED).
 *
 * <p>The concrete implementation ({@link BigQueryProcessStatusAdapter}) writes to
 * a BigQuery table configured via {@code --processStatusBqDataset} and
 * {@code --processStatusBqTable}.
 *
 * <h2>Typical call sequence in DataSourcePipelineFactory</h2>
 * <ol>
 *   <li>Before pipeline.run(): {@code write(ProcessStatusRecord.pending(...))} per source</li>
 *   <li>After waitUntilFinish() + validation: {@code write(ProcessStatusRecord.completed(...))}
 *       or {@code write(ProcessStatusRecord.failed(...))} per source</li>
 * </ol>
 */
public interface ProcessStatusAdapter {

    /**
     * Writes (inserts) a status record to the process status table.
     * Does not upsert — each call appends a new row. The status table is append-only;
     * the most recent row for a given (jobRunId, datasourceName) is the current state.
     */
    void write(ProcessStatusRecord record);

    /**
     * Returns the latest status for a given source in the current run.
     * Returns null if no record exists yet.
     */
    ProcessStatusRecord getLatest(String jobRunId, String datasourceName, String subprocessName);

    /**
     * Queries the row count in the output BQ table for a given source.
     * Used for post-pipeline validation (row count check).
     *
     * @param bqTableRef fully qualified table reference {@code project:dataset.table}
     * @return row count, or -1 if the table cannot be queried
     */
    long queryRowCount(String bqTableRef);

    /**
     * Queries the sum of a numeric field in the output BQ table.
     * Used for Balance and Control (BnC) validation.
     *
     * @param bqTableRef fully qualified table reference {@code project:dataset.table}
     * @param field      numeric column name to sum
     * @return the sum, or Double.NaN if the table cannot be queried
     */
    double querySum(String bqTableRef, String field);
}
