package com.yourco.beam.io.bq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.yourco.beam.context.RequestContext;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Service for running BigQuery jobs synchronously.
 *
 * <p>Used by {@code ReportPipelineFactory} (driver JVM) to:
 * <ol>
 *   <li>Run transformation queries and materialise results to BQ tables</li>
 *   <li>Export BQ tables to GCS files (CSV or JSON) for email attachment</li>
 * </ol>
 *
 * <p>Also safe to use inside a Beam worker DoFn: the no-arg constructor holds only a plain
 * {@link BigQuery} client, no {@code FrameworkOptions} reference, so it follows the same
 * {@code @Setup}-time instantiation pattern as {@code BigQueryDataSourceRecordAdapter}'s
 * String-tableRef constructor. {@code PostDownloadFinalizeTransform.FinalizeDoFn} uses it this
 * way to run an operator-declared {@code data_transform_query}.
 *
 * <p>All methods block until the BQ job completes. They throw {@link RuntimeException}
 * on failure so the caller can propagate the error and mark the report (or run) as FAILED.
 *
 * <h2>Table reference format</h2>
 * Accepts {@code project.dataset.table} (dot-separated) throughout.
 * Handles 3-part ({@code project.dataset.table}) and 2-part ({@code dataset.table}) forms.
 *
 * <h2>Authentication</h2>
 * Uses Application Default Credentials automatically on GCP and locally after
 * {@code gcloud auth application-default login}.
 */
public final class BigQueryJobService {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryJobService.class);

    private final BigQuery bigquery;

    public BigQueryJobService() {
        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
    }

    BigQueryJobService(BigQuery bigquery) {
        this.bigquery = bigquery;
    }

    // ── Query → table ─────────────────────────────────────────────────────────

    /**
     * Runs {@code sql} as a Standard SQL query and materialises the result to
     * {@code destinationTable} (WRITE_TRUNCATE, CREATE_IF_NEEDED).
     *
     * @param sql              fully resolved BQ Standard SQL (no template tokens remain)
     * @param destinationTable target table reference ({@code project.dataset.table})
     * @throws RuntimeException if the job fails or is interrupted
     */
    public void runQueryToTable(String sql, String destinationTable) {
        LOG.info("Running BQ query → {}", destinationTable);
        TableId dest = parseTableId(destinationTable);
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .setLabels(RequestContext.labels())
            .setUseLegacySql(false)
            .setDestinationTable(dest)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .build();
        awaitJob(bigquery.create(JobInfo.of(config)),
                 "query → " + destinationTable);
        LOG.info("BQ query materialised to {}", destinationTable);
    }

    /**
     * Runs {@code sql} as a Standard SQL query (no destination table).
     * Useful for preprocessing DDL or DML statements that don't produce output rows.
     */
    public void runQuery(String sql) {
        LOG.info("Running BQ query (no destination)");
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .setLabels(RequestContext.labels())
            .setUseLegacySql(false)
            .build();
        awaitJob(bigquery.create(JobInfo.of(config)), "query");
    }

    /**
     * Returns the exact row count of {@code tableRef} via a live {@code SELECT COUNT(*)} query.
     *
     * <p>Unlike a table-metadata row count (which can lag right after a write), this always
     * reflects rows visible at query time — needed immediately after {@link #runQueryToTable}
     * materialises a fresh result.
     *
     * @param tableRef table reference ({@code project.dataset.table})
     * @return exact row count
     * @throws RuntimeException if the count query fails or is interrupted
     */
    public long countRows(String tableRef) {
        String sql = "SELECT COUNT(*) AS cnt FROM `" + tableRef + "`";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .setLabels(RequestContext.labels())
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("cnt").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BQ row count query interrupted for " + tableRef, e);
        }
        return 0L;
    }

    /**
     * Runs {@code sql} and returns the first column of every row as text, in result order
     * (SQL NULL becomes {@code null}). For small result sets such as a list of column names —
     * not for reading data.
     *
     * @throws RuntimeException if the query fails or is interrupted
     */
    public List<String> queryStrings(String sql) {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .setLabels(RequestContext.labels())
            .setUseLegacySql(false)
            .build();
        List<String> values = new ArrayList<>();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                values.add(row.get(0).isNull() ? null : row.get(0).getStringValue());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BQ query interrupted", e);
        }
        return values;
    }

    /** One result row: column names and values as text ({@code null} for SQL NULL). */
    public record ResultRow(List<String> names, List<String> values) {
        /** The value of the column called {@code name} (case-insensitive), or null if absent/NULL. */
        public String get(String name) {
            for (int i = 0; i < names.size(); i++) {
                if (names.get(i).equalsIgnoreCase(name)) return values.get(i);
            }
            return null;
        }
        /** True if the result has a column called {@code name} (case-insensitive). */
        public boolean has(String name) {
            return names.stream().anyMatch(n -> n.equalsIgnoreCase(name));
        }
    }

    /**
     * Runs {@code sql} and returns the <b>last</b> row of the result (the legacy Balance &amp;
     * Control source query "last row wins"), or {@code null} when it returns no rows.
     *
     * @param location BigQuery job location, or null for BigQuery's default
     * @throws RuntimeException if the query fails or is interrupted
     */
    public ResultRow queryLastRow(String sql, String location) {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .setLabels(RequestContext.labels())
            .setUseLegacySql(false)
            .build();
        try {
            TableResult result = (location == null || location.isBlank())
                ? bigquery.query(config)
                : bigquery.query(config, JobId.newBuilder().setLocation(location.trim()).build());
            List<String> names = new ArrayList<>();
            result.getSchema().getFields().forEach(f -> names.add(f.getName()));
            ResultRow last = null;
            for (FieldValueList row : result.iterateAll()) {
                List<String> values = new ArrayList<>();
                for (int i = 0; i < names.size(); i++) {
                    values.add(row.get(i).isNull() ? null : row.get(i).getStringValue());
                }
                last = new ResultRow(names, values);
            }
            return last;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BQ query interrupted", e);
        }
    }

    /**
     * Drops {@code tableRef} if it exists. Best-effort — failures are logged and swallowed,
     * never thrown, since this is always a cleanup step for a temporary/staging table and
     * should never fail an otherwise-successful job.
     *
     * @param tableRef table reference ({@code project.dataset.table})
     */
    public void dropTableIfExists(String tableRef) {
        try {
            runQuery("DROP TABLE IF EXISTS `" + tableRef + "`");
        } catch (Exception e) {
            LOG.warn("Failed to drop temp table {}: {}", tableRef, e.getMessage());
        }
    }

    // ── Table → BQ table ─────────────────────────────────────────────────────

    /**
     * Copies all rows from {@code sourceTable} into {@code destinationTable}
     * (WRITE_TRUNCATE, CREATE_IF_NEEDED) using a {@code SELECT *} query.
     *
     * <p>This is the BQ output sink: shares a result table with a downstream
     * dataset (e.g. analytics project) without needing a GCS intermediate step.
     *
     * @param sourceTable      fully-qualified source: {@code project.dataset.table}
     * @param destinationTable fully-qualified destination: {@code project.dataset.table}
     */
    public void copyTable(String sourceTable, String destinationTable) {
        LOG.info("Copying BQ table {} → {}", sourceTable, destinationTable);
        runQueryToTable("SELECT * FROM `" + sourceTable + "`", destinationTable);
        LOG.info("BQ copy complete: {} → {}", sourceTable, destinationTable);
    }

    // ── Table → GCS ───────────────────────────────────────────────────────────

    /**
     * Exports {@code sourceTable} to {@code gcsUri} as CSV.
     *
     * <p>For tables smaller than ~1 GB the export produces a single file at
     * {@code gcsUri}. For larger tables, append {@code *.csv} to {@code gcsUri}
     * so BQ can shard across multiple files.
     *
     * @param sourceTable   source table reference ({@code project.dataset.table})
     * @param gcsUri        destination GCS URI (e.g. {@code gs://bucket/reports/file.csv})
     * @param includeHeader whether to include a header row in the CSV
     */
    public void exportToCsv(String sourceTable, String gcsUri, boolean includeHeader) {
        LOG.info("Exporting {} → {} (CSV, header={})", sourceTable, gcsUri, includeHeader);
        doExport(sourceTable, gcsUri, "CSV", includeHeader);
    }

    /**
     * Exports {@code sourceTable} to {@code gcsUri} as newline-delimited JSON.
     */
    public void exportToJson(String sourceTable, String gcsUri) {
        LOG.info("Exporting {} → {} (JSON)", sourceTable, gcsUri);
        doExport(sourceTable, gcsUri, "NEWLINE_DELIMITED_JSON", true);
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private void doExport(String sourceTable, String gcsUri, String format, boolean printHeader) {
        TableId tableId = parseTableId(sourceTable);
        ExtractJobConfiguration config = ExtractJobConfiguration.newBuilder(tableId, gcsUri)
            .setLabels(RequestContext.labels())
            .setFormat(format)
            .setPrintHeader(printHeader)
            .build();
        awaitJob(bigquery.create(JobInfo.of(config)),
                 "export " + sourceTable + " → " + gcsUri);
        LOG.info("Export complete: {}", gcsUri);
    }

    private void awaitJob(Job job, String description) {
        try {
            Job completed = job.waitFor();
            if (completed == null) {
                throw new RuntimeException("BQ job for [" + description + "] no longer exists");
            }
            if (completed.getStatus().getError() != null) {
                throw new RuntimeException(
                    "BQ job failed [" + description + "]: "
                    + completed.getStatus().getError().getMessage());
            }
        } catch (BigQueryException e) {
            throw new RuntimeException("BQ exception [" + description + "]: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BQ job interrupted [" + description + "]", e);
        }
    }

    /** Parses {@code project.dataset.table} or {@code dataset.table}. */
    static TableId parseTableId(String tableRef) {
        if (tableRef == null || tableRef.isBlank()) {
            throw new IllegalArgumentException("BQ table reference must not be blank");
        }
        String[] parts = tableRef.trim().split("\\.");
        return switch (parts.length) {
            case 3 -> TableId.of(parts[0], parts[1], parts[2]);
            case 2 -> TableId.of(parts[0], parts[1]);
            default -> throw new IllegalArgumentException(
                "Invalid BQ table reference (expected project.dataset.table): " + tableRef);
        };
    }
}
