package com.yourco.beam.io.status;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.yourco.beam.model.ProcessStatusRecord;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * BigQuery-backed implementation of {@link ProcessStatusAdapter}.
 *
 * <p>Writes one row per source per pipeline run to the {@code process_status} table.
 * Also provides helper methods to query the output table for row count and field sums,
 * which are used for post-pipeline validation (row count and BnC checks).
 *
 * <h2>Pre-requisite: create the table</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.process_status (
 *   job_run_id          STRING    NOT NULL,
 *   process_type        STRING,
 *   datasource_name     STRING    NOT NULL,
 *   subprocess_name     STRING,
 *   period_id           STRING,
 *   period_start        STRING,
 *   period_end          STRING,
 *   status              STRING    NOT NULL,
 *   row_count           INT64,
 *   error_message       STRING,
 *   validation_details  STRING,
 *   started_at          TIMESTAMP,
 *   completed_at        TIMESTAMP
 * )
 * PARTITION BY DATE(started_at);
 * }</pre>
 */
public final class BigQueryProcessStatusAdapter implements ProcessStatusAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryProcessStatusAdapter.class);

    private final BigQuery bigQuery;
    private final String projectId;
    private final String datasetId;
    private final String tableId;

    public BigQueryProcessStatusAdapter(FrameworkOptions options) {
        this.bigQuery  = BigQueryOptions.getDefaultInstance().getService();
        String project = options.getCheckpointBqProject();
        this.projectId = (project != null && !project.isBlank()) ? project : options.getProject();
        this.datasetId = options.getProcessStatusBqDataset();
        this.tableId   = options.getProcessStatusBqTable();
    }

    @Override
    public void write(ProcessStatusRecord record) {
        Map<String, Object> row = new HashMap<>();
        row.put("job_run_id",         record.jobRunId);
        row.put("process_type",       record.processType);
        row.put("datasource_name",    record.datasourceName);
        row.put("subprocess_name",    record.subprocessName);
        row.put("period_id",          record.periodId);
        row.put("period_start",       record.periodStart);
        row.put("period_end",         record.periodEnd);
        row.put("status",             record.status);
        row.put("row_count",          record.rowCount);
        row.put("error_message",      record.errorMessage);
        row.put("validation_details", record.validationDetails);
        row.put("started_at",         record.startedAt != null ? record.startedAt.toString() : null);
        row.put("completed_at",       record.completedAt != null ? record.completedAt.toString() : null);

        String insertId = record.jobRunId + "-" + record.datasourceName + "-"
            + record.subprocessName + "-" + record.status + "-" + Instant.now().toEpochMilli();

        TableId tableRef = TableId.of(projectId, datasetId, tableId);
        InsertAllRequest request = InsertAllRequest.newBuilder(tableRef)
            .addRow(insertId, row)
            .build();

        InsertAllResponse response = bigQuery.insertAll(request);
        if (response.hasErrors()) {
            LOG.error("process_status write had errors for {}/{}: {}",
                      record.datasourceName, record.status, response.getInsertErrors());
        } else {
            LOG.info("process_status written: datasource={}, status={}",
                     record.datasourceName, record.status);
        }
    }

    @Override
    public ProcessStatusRecord getLatest(String jobRunId, String datasourceName, String subprocessName) {
        String sql = String.format(
            "SELECT * FROM `%s.%s.%s` "
            + "WHERE job_run_id = '%s' AND datasource_name = '%s' AND subprocess_name = '%s' "
            + "ORDER BY started_at DESC LIMIT 1",
            projectId, datasetId, tableId,
            escape(jobRunId), escape(datasourceName), escape(subprocessName));

        try {
            TableResult result = bigQuery.query(QueryJobConfiguration.of(sql));
            if (result.getTotalRows() == 0) return null;
            return bqRowToRecord(result.iterateAll().iterator().next());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            LOG.warn("Could not read process_status for {}/{}: {}", datasourceName, subprocessName, e.getMessage());
            return null;
        }
    }

    @Override
    public long queryRowCount(String bqTableRef) {
        String sql = "SELECT COUNT(*) AS row_count FROM `" + bqTableRefToStd(bqTableRef) + "`";
        try {
            TableResult result = bigQuery.query(QueryJobConfiguration.of(sql));
            FieldValueList row = result.iterateAll().iterator().next();
            return row.get("row_count").getLongValue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1L;
        } catch (Exception e) {
            LOG.warn("Could not query row count for {}: {}", bqTableRef, e.getMessage());
            return -1L;
        }
    }

    @Override
    public double querySum(String bqTableRef, String field) {
        String sql = "SELECT SUM(`" + escape(field) + "`) AS field_sum FROM `"
            + bqTableRefToStd(bqTableRef) + "`";
        try {
            TableResult result = bigQuery.query(QueryJobConfiguration.of(sql));
            FieldValueList row = result.iterateAll().iterator().next();
            if (row.get("field_sum").isNull()) return 0.0;
            return row.get("field_sum").getDoubleValue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Double.NaN;
        } catch (Exception e) {
            LOG.warn("Could not query sum({}) for {}: {}", field, bqTableRef, e.getMessage());
            return Double.NaN;
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static ProcessStatusRecord bqRowToRecord(FieldValueList row) {
        return ProcessStatusRecord.builder()
            .jobRunId(strVal(row, "job_run_id"))
            .processType(strVal(row, "process_type"))
            .datasourceName(strVal(row, "datasource_name"))
            .subprocessName(strVal(row, "subprocess_name"))
            .periodId(strVal(row, "period_id"))
            .periodStart(strVal(row, "period_start"))
            .periodEnd(strVal(row, "period_end"))
            .status(strVal(row, "status"))
            .rowCount(row.get("row_count").isNull() ? 0L : row.get("row_count").getLongValue())
            .errorMessage(strVal(row, "error_message"))
            .validationDetails(strVal(row, "validation_details"))
            .build();
    }

    private static String strVal(FieldValueList row, String field) {
        try {
            return row.get(field).isNull() ? null : row.get(field).getStringValue();
        } catch (Exception e) {
            return null;
        }
    }

    /** Converts {@code project:dataset.table} → {@code project.dataset.table} for StandardSQL. */
    private static String bqTableRefToStd(String ref) {
        return ref.replace(":", ".");
    }

    private static String escape(String s) {
        return s == null ? "" : s.replace("'", "\\'").replace("`", "\\`");
    }
}
