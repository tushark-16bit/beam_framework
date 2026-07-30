package com.yourco.beam.io.records;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQuery implementation of {@link DataSourceRecordAdapter}.
 *
 * <p>Each DaRec row now contains a page of up to 250 source rows serialised as a JSON array
 * in {@code row_da_json_tx}. Queries must un-nest the array with
 * {@code UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx))} to reach individual source records.
 * All queries filter by {@code da_id}.
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
        // row_da_json_tx is a JSON array of source rows per DaRec page.
        // JSON_ARRAY_LENGTH sums element counts across all pages to return total source rows.
        String sql = "SELECT IFNULL(SUM(JSON_ARRAY_LENGTH(row_da_json_tx)), 0) AS cnt"
            + " FROM " + table + " WHERE da_id = @daId";
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
    public double sumField(long daId, String field) {
        // Unnest the JSON array in each page row, then extract and sum the target field.
        String sql = "SELECT SUM(CAST(JSON_VALUE(row_json, @jsonPath) AS FLOAT64)) AS total"
            + " FROM " + table
            + " CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx)) AS row_json"
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
}
