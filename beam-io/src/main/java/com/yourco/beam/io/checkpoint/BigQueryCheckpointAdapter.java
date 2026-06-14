package com.yourco.beam.io.checkpoint;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.yourco.beam.model.CheckpointRecord;
import com.yourco.beam.model.CheckpointState;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * BigQuery-backed implementation of {@link CheckpointAdapter}.
 *
 * <h2>Why BigQuery for checkpoints?</h2>
 * Checkpoints need to be visible across Airflow runs and potentially across machines.
 * BQ is already a dependency, requires no additional infrastructure, and the checkpoint
 * table is naturally queryable for operational dashboards.
 *
 * <h2>Write path</h2>
 * Uses the streaming-insert API ({@code tabledata.insertAll}) for low-latency writes.
 * Checkpoint records are small and infrequent — one record per source per pipeline run.
 *
 * <h2>Read path</h2>
 * Uses {@code BigQuery.query()} which runs an interactive query job. Suitable for
 * driver-JVM use (called once per pipeline run, not per element).
 *
 * <h2>Pre-requisite</h2>
 * The checkpoint table must exist. Suggested DDL:
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.pipeline_checkpoints (
 *   job_run_id        STRING    NOT NULL,
 *   datasource_name   STRING    NOT NULL,
 *   period_id         STRING    NOT NULL,
 *   subprocess_name   STRING    NOT NULL,
 *   state             STRING    NOT NULL,
 *   source_type       STRING,
 *   created_at        TIMESTAMP NOT NULL,
 *   error_message     STRING,
 *   records_processed INT64
 * );
 * }</pre>
 */
public final class BigQueryCheckpointAdapter implements CheckpointAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCheckpointAdapter.class);

    private final BigQuery bigQuery;
    private final String projectId;
    private final String datasetId;
    private final String tableId;

    public BigQueryCheckpointAdapter(FrameworkOptions options) {
        this.bigQuery  = BigQueryOptions.getDefaultInstance().getService();
        // Fall back to --project if --checkpointBqProject is not set
        String project = options.getCheckpointBqProject();
        this.projectId = (project != null && !project.isBlank()) ? project : options.getProject();
        this.datasetId = options.getCheckpointBqDataset();
        this.tableId   = options.getCheckpointBqTable();
    }

    @Override
    public void writeCheckpoint(CheckpointRecord record) {
        Map<String, Object> row = new HashMap<>();
        row.put("job_run_id",        record.jobRunId);
        row.put("datasource_name",   record.datasourceName);
        row.put("period_id",         record.periodId);
        row.put("subprocess_name",   record.subprocessName);
        row.put("state",             record.state.name());
        row.put("source_type",       record.sourceType != null ? record.sourceType.name() : null);
        row.put("created_at",        record.createdAt.toString());
        row.put("error_message",     record.errorMessage);
        row.put("records_processed", record.recordsProcessed);

        TableId tableRef = TableId.of(projectId, datasetId, tableId);
        InsertAllRequest request = InsertAllRequest.newBuilder(tableRef)
            .addRow(record.jobRunId + "-" + record.datasourceName + "-" + record.state.name(), row)
            .build();

        InsertAllResponse response = bigQuery.insertAll(request);
        if (response.hasErrors()) {
            LOG.error("Checkpoint write had errors for {}: {}", record, response.getInsertErrors());
        } else {
            LOG.info("Checkpoint written: {} -> {}", record.datasourceName, record.state);
        }
    }

    @Override
    public Optional<CheckpointRecord> getLatestCheckpoint(String datasourceName, String periodId, String subprocess) {
        String sql = String.format(
            "SELECT * FROM `%s.%s.%s` "
            + "WHERE datasource_name = '%s' AND period_id = '%s' AND subprocess_name = '%s' "
            + "ORDER BY created_at DESC LIMIT 1",
            projectId, datasetId, tableId,
            escape(datasourceName), escape(periodId), escape(subprocess));

        try {
            TableResult result = bigQuery.query(QueryJobConfiguration.of(sql));
            if (result.getTotalRows() == 0) return Optional.empty();

            com.google.cloud.bigquery.FieldValueList bqRow = result.iterateAll().iterator().next();
            CheckpointRecord record = bqRowToRecord(bqRow);
            return Optional.of(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Checkpoint read interrupted for datasource={}", datasourceName);
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("Could not read checkpoint for datasource={}: {}", datasourceName, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public boolean isDownloadComplete(String datasourceName, String periodId, String subprocess) {
        return getLatestCheckpoint(datasourceName, periodId, subprocess)
            .map(r -> r.state == CheckpointState.FINISHED_ACCESSING)
            .orElse(false);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private static CheckpointRecord bqRowToRecord(com.google.cloud.bigquery.FieldValueList row) {
        CheckpointState checkpointState = CheckpointState.valueOf(row.get("state").getStringValue());
        return CheckpointRecord.builder()
            .jobRunId(row.get("job_run_id").getStringValue())
            .datasourceName(row.get("datasource_name").getStringValue())
            .periodId(row.get("period_id").getStringValue())
            .subprocessName(row.get("subprocess_name").getStringValue())
            .state(checkpointState)
            .sourceType(parseSourceType(row.get("source_type").getStringValue()))
            .errorMessage(row.get("error_message").isNull() ? null : row.get("error_message").getStringValue())
            .recordsProcessed(row.get("records_processed").isNull() ? 0L : row.get("records_processed").getLongValue())
            .build();
    }

    private static SourceType parseSourceType(String s) {
        try {
            return s != null && !s.isBlank() ? SourceType.valueOf(s) : null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** Minimal SQL escape for string literals — avoids injection in the checkpoint query. */
    private static String escape(String s) {
        return s == null ? "" : s.replace("'", "\\'");
    }
}
