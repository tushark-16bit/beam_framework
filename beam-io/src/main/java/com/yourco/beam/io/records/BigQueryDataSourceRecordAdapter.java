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
 * <p>Queries the {@code DaRec} table using {@code JSON_VALUE} to extract numeric
 * fields from {@code RowDaJsonTx} blobs. All queries filter by {@code DaId}.
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
                   + "." + options.getDaRecTable() + "`";
        LOG.info("DaRec table: {}", table);
    }

    @Override
    public long countRecords(long DaId) {
        String sql = "SELECT COUNT(*) AS cnt FROM " + table + " WHERE DaId = @daId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("daId", QueryParameterValue.int64(DaId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("cnt").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("DaRec count query interrupted for DaId={}", DaId);
        } catch (Exception e) {
            LOG.warn("DaRec count query failed for DaId={}: {}", DaId, e.getMessage());
        }
        return -1L;
    }

    @Override
    public double sumField(long DaId, String field) {
        String sql = "SELECT SUM(CAST(JSON_VALUE(RowDaJsonTx, @jsonPath) AS FLOAT64)) AS total"
            + " FROM " + table + " WHERE DaId = @daId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("jsonPath", QueryParameterValue.string("$." + field))
            .addNamedParameter("daId",     QueryParameterValue.int64(DaId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                var fv = row.get("total");
                return fv.isNull() ? 0.0 : fv.getDoubleValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("DaRec sum query interrupted for DaId={}, field={}", DaId, field);
        } catch (Exception e) {
            LOG.warn("DaRec sum query failed for DaId={}, field={}: {}", DaId, field, e.getMessage());
        }
        return Double.NaN;
    }
}
