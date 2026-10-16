package com.yourco.beam.io.records;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.io.util.FileHeaderLegend;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

/**
 * BigQuery implementation of {@link DataSourceRecordAdapter}.
 *
 * <p>Each DaRec row contains a page of up to 250 source rows in {@code row_da_json_tx} — a flat
 * JSON array for most sources, or {@code {"Data":[...],"DataHeaders":[...]}} for a FILE source
 * with a header row. Queries un-nest the row array with
 * {@link FileHeaderLegend#dataArrayExpr}, which handles both shapes. All queries filter by
 * {@code da_id}.
 */
public final class BigQueryDataSourceRecordAdapter implements DataSourceRecordAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDataSourceRecordAdapter.class);

    private final BigQuery bigquery;
    private final String   table;  // fully-qualified: `project.dataset.table`

    public BigQueryDataSourceRecordAdapter(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    /**
     * In-worker constructor: takes a pre-formatted table reference instead of
     * {@link FrameworkOptions} (which is not serializable and cannot be a DoFn field).
     *
     * @param tableRef backtick-quoted fully-qualified table ref, e.g.
     *                 {@code `project.dataset.DaRec`}
     */
    public BigQueryDataSourceRecordAdapter(String tableRef) {
        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
        this.table    = tableRef;
        LOG.info("DaRec table (worker): {}", tableRef);
    }

    BigQueryDataSourceRecordAdapter(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        this.table = "`" + project + "." + options.getCheckpointBqDataset()
                   + "." + options.getDaRecTable() + "`";
        LOG.info("DaRec table: {}", table);
    }

    @Override
    public long countRecords(long daId) {
        // row_da_json_tx holds a page's rows either as a flat JSON array, or (FILE sources with
        // a header) as {"Data":[...],"DataHeaders":[...]}. FileHeaderLegend.dataArrayExpr()
        // extracts just the row array either way, so COUNT(*) never includes the header legend
        // — it lives in DataHeaders, structurally separate from Data, and is never unnested here.
        String sql = "SELECT COUNT(*) AS cnt"
            + " FROM " + table
            + " CROSS JOIN UNNEST(" + FileHeaderLegend.dataArrayExpr("row_da_json_tx") + ") AS row_json"
            + " WHERE da_id = @daId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("daId", QueryParameterValue.int64(daId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("cnt").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("DaRec count query interrupted for da_id={}", daId);
        } catch (Exception e) {
            LOG.warn("DaRec count query failed for da_id={}: {}", daId, e.getMessage());
        }
        return -1L;
    }

    @Override
    public void deleteRecords(long daId) {
        String sql = "DELETE FROM " + table + " WHERE da_id = @daId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("daId", QueryParameterValue.int64(daId))
            .setUseLegacySql(false)
            .build();
        try {
            bigquery.query(config);
            LOG.info("Deleted DaRec rows for da_id={}", daId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("DaRec delete interrupted for da_id={}", daId);
        } catch (Exception e) {
            LOG.warn("Failed to delete DaRec rows for da_id={}: {}", daId, e.getMessage());
        }
    }

    @Override
    public double sumField(long daId, String field) {
        // Unnest just the Data/row array (see countRecords()), then extract and sum the target
        // field. The header legend lives in a separate DataHeaders array and is never unnested
        // here, so a BnC rule for a lettered field (e.g. "A") never risks CASTing a legend's
        // header-name string instead of a real row's value.
        String sql = "SELECT SUM(CAST(JSON_VALUE(row_json, @jsonPath) AS FLOAT64)) AS total"
            + " FROM " + table
            + " CROSS JOIN UNNEST(" + FileHeaderLegend.dataArrayExpr("row_da_json_tx") + ") AS row_json"
            + " WHERE da_id = @daId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("jsonPath", QueryParameterValue.string("$." + field))
            .addNamedParameter("daId",     QueryParameterValue.int64(daId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                var fv = row.get("total");
                return fv.isNull() ? 0.0 : fv.getDoubleValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("DaRec sum query interrupted for da_id={}, field={}", daId, field);
        } catch (Exception e) {
            LOG.warn("DaRec sum query failed for da_id={}, field={}: {}", daId, field, e.getMessage());
        }
        return Double.NaN;
    }

    // ── BnC destination values ────────────────────────────────────────────────

    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public Totals countAndSum(long daId, String destColumn) {
        try {
            if (destColumn == null || destColumn.isBlank()) {
                return new Totals(queryTotals(daId, null).count(), BigDecimal.ZERO);
            }
            String key = resolveColumnKey(firstPageJson(daId), destColumn);
            return queryTotals(daId, key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRec BnC query interrupted for da_id=" + daId, e);
        }
    }

    private Totals queryTotals(long daId, String key) throws InterruptedException {
        // NUMERIC (exact decimal) rather than FLOAT64 so the inclusive amount boundary of the BnC
        // rule is not at the mercy of binary rounding; SAFE_CAST turns a non-number into NULL,
        // which SUM ignores — the legacy "non-numeric counts as 0".
        String amountExpr = key == null ? "CAST(NULL AS STRING)"
            : "CAST(SUM(SAFE_CAST(JSON_VALUE(row_json, @jsonPath) AS NUMERIC)) AS STRING)";
        String sql = "SELECT COUNT(*) AS cnt, " + amountExpr + " AS total"
            + " FROM " + table
            + " CROSS JOIN UNNEST(" + FileHeaderLegend.dataArrayExpr("row_da_json_tx") + ") AS row_json"
            + " WHERE da_id = @daId";
        QueryJobConfiguration.Builder b = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("daId", QueryParameterValue.int64(daId))
            .setUseLegacySql(false);
        if (key != null) {
            b.addNamedParameter("jsonPath", QueryParameterValue.string(jsonPath(key)));
        }
        try {
            for (FieldValueList row : bigquery.query(b.build()).iterateAll()) {
                long count = row.get("cnt").getLongValue();
                var total = row.get("total");
                return new Totals(count, total.isNull() ? BigDecimal.ZERO : new BigDecimal(total.getStringValue()));
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("DaRec BnC totals query failed for da_id=" + daId
                + ": " + e.getMessage(), e);
        }
        return new Totals(0, BigDecimal.ZERO);
    }

    /** The first page (lowest {@code page_no}) of a run, raw, or null when the run has no rows. */
    private String firstPageJson(long daId) throws InterruptedException {
        String sql = "SELECT row_da_json_tx FROM " + table
            + " WHERE da_id = @daId ORDER BY page_no LIMIT 1";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("daId", QueryParameterValue.int64(daId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                var v = row.get("row_da_json_tx");
                return v.isNull() ? null : v.getStringValue();
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("DaRec first-page query failed for da_id=" + daId
                + ": " + e.getMessage(), e);
        }
        return null;
    }

    /** JSON path for a key, quoted when it is not a plain identifier. */
    static String jsonPath(String key) {
        return key.matches("[A-Za-z0-9_]+") ? "$." + key
            : "$.\"" + key.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * Finds the stored JSON key for the configured destination column, case-insensitively.
     *
     * <ul>
     *   <li>A FILE page {@code {"Data":[…],"DataHeaders":[{"A":"trade_id",…}]}}: the header
     *       <em>name</em> is matched first, then the column <em>letter</em> — the rows are keyed
     *       by letter (legacy rule: header {@code ColumnName}, then {@code DataColName}).</li>
     *   <li>A flat page {@code [{…},…]}: matched against the keys of its first row.</li>
     *   <li>No page at all (an empty run): the name as configured — the totals will simply be 0.</li>
     * </ul>
     *
     * @throws IllegalArgumentException when no stored column matches
     */
    static String resolveColumnKey(String firstPageJson, String destColumn) {
        if (firstPageJson == null || firstPageJson.isBlank()) return destColumn;
        JsonNode root;
        try {
            root = JSON.readTree(firstPageJson);
        } catch (Exception e) {
            throw new IllegalStateException("stored DaRec page is not valid JSON: " + e.getMessage(), e);
        }
        if (root.isObject() && root.has("Data")) {
            JsonNode headers = root.path("DataHeaders");
            JsonNode legend = headers.isArray() && headers.size() > 0 ? headers.get(0) : null;
            if (legend != null && legend.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = legend.fields();
                while (it.hasNext()) {                      // header name first
                    Map.Entry<String, JsonNode> e = it.next();
                    if (e.getValue().isTextual() && e.getValue().asText().equalsIgnoreCase(destColumn)) {
                        return e.getKey();
                    }
                }
            }
            JsonNode data = root.path("Data");
            JsonNode first = data.isArray() && data.size() > 0 ? data.get(0) : null;
            String byLetter = matchKey(first, destColumn);   // then the letter (A, B, …)
            if (byLetter != null) return byLetter;
        } else if (root.isArray() && root.size() > 0) {
            String key = matchKey(root.get(0), destColumn);
            if (key != null) return key;
        } else {
            return destColumn;                              // an empty page: nothing to match against
        }
        throw new IllegalArgumentException("BnC destination column '" + destColumn
            + "' was not found in the stored rows");
    }

    private static String matchKey(JsonNode row, String name) {
        if (row == null || !row.isObject()) return null;
        Iterator<String> keys = row.fieldNames();
        while (keys.hasNext()) {
            String k = keys.next();
            if (k.equalsIgnoreCase(name)) return k;
        }
        return null;
    }
}
