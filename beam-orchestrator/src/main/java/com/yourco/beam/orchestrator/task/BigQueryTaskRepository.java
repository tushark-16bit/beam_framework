package com.yourco.beam.orchestrator.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.yourco.beam.orchestrator.model.TaskItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Persists task items to a BigQuery table via the streaming insert API.
 *
 * <h2>Required table schema</h2>
 * See {@code beam-orchestrator/README.md} for the DDL.
 * Key columns: {@code task_id (STRING)}, {@code run_id (STRING)},
 * {@code run_type (STRING)}, {@code parent_id (STRING)}, {@code name (STRING)},
 * {@code subprocess (STRING)}, {@code period_id (INT64)},
 * {@code period_start (DATE)}, {@code period_end (DATE)},
 * {@code run_date (DATE)}, {@code status (STRING)},
 * {@code extra_params_json (STRING)}, {@code created_at (DATETIME)}.
 *
 * <h2>Idempotency</h2>
 * {@code task_id} (a UUID) is passed as the BQ deduplication token so that re-running
 * the orchestrator within the same BQ deduplication window (≈ 1 minute) is safe.
 * For longer-window idempotency, add a MERGE / DELETE+INSERT step in your own impl.
 */
public final class BigQueryTaskRepository implements TaskRepository {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTaskRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final BigQuery bigquery;
    private final TableId  tableId;

    public BigQueryTaskRepository(BigQuery bigquery, String project,
                                  String dataset, String table) {
        this.bigquery = bigquery;
        this.tableId  = TableId.of(project, dataset, table);
    }

    @Override
    public void save(List<TaskItem> tasks) {
        if (tasks.isEmpty()) {
            LOG.info("No tasks to save");
            return;
        }

        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>(tasks.size());
        for (TaskItem task : tasks) {
            rows.add(InsertAllRequest.RowToInsert.of(
                task.taskId,       // deduplication key
                toRow(task)
            ));
        }

        InsertAllRequest request = InsertAllRequest.newBuilder(tableId)
            .setRows(rows)
            .build();

        InsertAllResponse response = bigquery.insertAll(request);
        if (response.hasErrors()) {
            LOG.error("BQ insert errors for task table {}: {}", tableId, response.getInsertErrors());
            throw new RuntimeException(
                "Failed to insert " + response.getInsertErrors().size()
                + " task row(s) into " + tableId + ". See logs for details.");
        }

        LOG.info("Saved {} task row(s) to {}", tasks.size(), tableId);
    }

    private static Map<String, Object> toRow(TaskItem task) {
        Map<String, Object> row = new HashMap<>();
        row.put("task_id",     task.taskId);
        row.put("run_id",      task.runId);
        row.put("run_type",    task.spec.runType);
        row.put("parent_id",   task.spec.parentId);
        row.put("name",        task.spec.name);
        row.put("subprocess",  task.spec.subprocess);
        row.put("period_id",   task.spec.period.periodId);
        row.put("period_start", task.spec.period.periodStart.toString());
        row.put("period_end",   task.spec.period.periodEnd.toString());
        row.put("run_date",     task.spec.period.runDate.toString());
        row.put("run_order",    task.spec.runOrder);
        row.put("status",       task.status);
        row.put("extra_params_json", toJson(task.spec.extraParams));
        row.put("metadata_json",     toJson(task.metadata));
        row.put("created_at",        task.createdAt.toString());
        return row;
    }

    private static String toJson(Object obj) {
        try {
            return JSON.writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }
}
