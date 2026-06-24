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
 * <p>Queries the record table using {@code JSON_VALUE} to extract numeric fields
 * from {@code RowDSJsonTx} blobs. All queries filter by {@code dataSourceId}.
 */
public final class BigQueryDataSourceRecordAdapter implements DataSourceRecordAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDataSourceRecordAdapter.class);

    private final BigQuery bigquery;
    private final String   table;  // fully-qualified: `project.dataset.table`

    public BigQueryDataSourceRecordAdapter(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryDataSourceRecordAdapter(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        this.table = "`" + project + "." + options.getCheckpointBqDataset()
                   + "." + options.getRecordBqTable() + "`";
        LOG.info("DataSourceRecordAdapter: {}", table);
    }

    @Override
    public long countRecords(long dataSourceId) {
        String sql = "SELECT COUNT(*) AS cnt FROM " + table
            + " WHERE dataSourceId = @dsId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("dsId", QueryParameterValue.int64(dataSourceId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("cnt").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Record count query interrupted for dataSourceId={}", dataSourceId);
        } catch (Exception e) {
            LOG.warn("Record count query failed for dataSourceId={}: {}", dataSourceId, e.getMessage());
        }
        return -1L;
    }

    @Override
    public double sumField(long dataSourceId, String field) {
        // JSON_VALUE returns STRING; cast to FLOAT64 for aggregation
        String sql = "SELECT SUM(CAST(JSON_VALUE(RowDSJsonTx, @jsonPath) AS FLOAT64)) AS total"
            + " FROM " + table + " WHERE dataSourceId = @dsId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("jsonPath", QueryParameterValue.string("$." + field))
            .addNamedParameter("dsId",     QueryParameterValue.int64(dataSourceId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                var fv = row.get("total");
                return fv.isNull() ? 0.0 : fv.getDoubleValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("BnC sum query interrupted for dataSourceId={}, field={}", dataSourceId, field);
        } catch (Exception e) {
            LOG.warn("BnC sum query failed for dataSourceId={}, field={}: {}",
                     dataSourceId, field, e.getMessage());
        }
        return Double.NaN;
    }
}
