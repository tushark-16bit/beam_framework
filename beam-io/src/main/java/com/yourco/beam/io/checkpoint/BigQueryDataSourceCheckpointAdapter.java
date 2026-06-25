package com.yourco.beam.io.checkpoint;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.model.DataSourceCheckpoint;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * BigQuery implementation of {@link DataSourceCheckpointAdapter}.
 *
 * <p>Uses BQ DML for all operations. The checkpoint table is small and
 * accessed only from the driver JVM — never from Beam workers.
 *
 * <h2>dataSourceId generation</h2>
 * Queries {@code SELECT IFNULL(MAX(dataSourceId), 0) + 1} from the checkpoint table
 * to get the next sequential ID. This is not atomic across concurrent pipelines —
 * use a single driver JVM per period to avoid collisions.
 */
public final class BigQueryDataSourceCheckpointAdapter implements DataSourceCheckpointAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDataSourceCheckpointAdapter.class);

    private final BigQuery bigquery;
    private final String   table;   // fully-qualified: `project.dataset.table`

    public BigQueryDataSourceCheckpointAdapter(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryDataSourceCheckpointAdapter(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        this.table = "`" + project + "." + options.getCheckpointBqDataset()
                   + "." + options.getCheckpointBqTable() + "`";
        LOG.info("DataSourceCheckpointAdapter: {}", table);
    }

    @Override
    public long createCheckpoint(String srcName, String perId, String dsNm) {
        long dsId  = nextDataSourceId();
        long vsnNo = nextVsnNo(srcName, perId);
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(Instant.now().toEpochMilli());

        String sql = "INSERT INTO " + table
            + " (dataSourceId, srcName, vsnNo, PerId, DSNm, BalAndCntlSmryTx, StaCd, CreatedTs, LstUpdtTs)"
            + " VALUES (@dsId, @srcName, @vsnNo, @perId, @dsNm, NULL, @staCd, @now, @now)";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("dsId",   QueryParameterValue.int64(dsId))
            .addNamedParameter("srcName",QueryParameterValue.string(srcName))
            .addNamedParameter("vsnNo",  QueryParameterValue.int64(vsnNo))
            .addNamedParameter("perId",  QueryParameterValue.string(perId))
            .addNamedParameter("dsNm",   QueryParameterValue.string(dsNm != null ? dsNm : ""))
            .addNamedParameter("staCd",  QueryParameterValue.string(DataSourceCheckpoint.STA_LOADING))
            .addNamedParameter("now",    QueryParameterValue.timestamp(nowMicros))
            .setUseLegacySql(false)
            .build();

        runDml(config);
        LOG.info("Checkpoint created: dataSourceId={} srcName={} perId={} vsnNo={} sta=LOADING",
                 dsId, srcName, perId, vsnNo);
        return dsId;
    }

    @Override
    public void updateStatus(long dataSourceId, String staCd, String balAndCntlSmryTx) {
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(Instant.now().toEpochMilli());
        String sql = "UPDATE " + table
            + " SET StaCd = @staCd, BalAndCntlSmryTx = @bnc, LstUpdtTs = @now"
            + " WHERE dataSourceId = @dsId";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("staCd", QueryParameterValue.string(staCd))
            .addNamedParameter("bnc",   balAndCntlSmryTx != null
                                        ? QueryParameterValue.string(balAndCntlSmryTx)
                                        : QueryParameterValue.string(""))
            .addNamedParameter("now",   QueryParameterValue.timestamp(nowMicros))
            .addNamedParameter("dsId",  QueryParameterValue.int64(dataSourceId))
            .setUseLegacySql(false)
            .build();

        runDml(config);
        LOG.info("Checkpoint updated: dataSourceId={} staCd={}", dataSourceId, staCd);
    }

    @Override
    public boolean isCompleted(String srcName, String perId) {
        // Filter on StaCd before ordering so a newer LOADING/FAILED row cannot shadow
        // an older COMPLETED row for the same (srcName, PerId) pair.
        String sql = "SELECT dataSourceId FROM " + table
            + " WHERE srcName = @srcName AND PerId = @perId AND StaCd = @completed"
            + " LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srcName",   QueryParameterValue.string(srcName))
            .addNamedParameter("perId",     QueryParameterValue.string(perId))
            .addNamedParameter("completed", QueryParameterValue.string(DataSourceCheckpoint.STA_COMPLETED))
            .setUseLegacySql(false)
            .build();

        try {
            return bigquery.query(config).iterateAll().iterator().hasNext();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ checkpoint query interrupted", e);
        }
    }

    @Override
    public Optional<DataSourceCheckpoint> getLatest(String srcName, String perId) {
        String sql = "SELECT * FROM " + table
            + " WHERE srcName = @srcName AND PerId = @perId"
            + " ORDER BY LstUpdtTs DESC LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srcName", QueryParameterValue.string(srcName))
            .addNamedParameter("perId",   QueryParameterValue.string(perId))
            .setUseLegacySql(false)
            .build();

        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return Optional.of(new DataSourceCheckpoint(
                    row.get("dataSourceId").getLongValue(),
                    row.get("srcName").getStringValue(),
                    row.get("vsnNo").getLongValue(),
                    strOrNull(row, "PerId"),
                    strOrNull(row, "DSNm"),
                    strOrNull(row, "BalAndCntlSmryTx"),
                    row.get("StaCd").getStringValue(),
                    Instant.parse(row.get("CreatedTs").getStringValue()),
                    Instant.parse(row.get("LstUpdtTs").getStringValue())
                ));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ checkpoint query interrupted", e);
        }
        return Optional.empty();
    }

    // ── Sequence helpers ──────────────────────────────────────────────────────

    private long nextDataSourceId() {
        String sql = "SELECT IFNULL(MAX(dataSourceId), 0) + 1 AS next_id FROM " + table;
        return queryLong(sql, "next_id");
    }

    private long nextVsnNo(String srcName, String perId) {
        String sql = "SELECT IFNULL(MAX(vsnNo), 0) + 1 AS next_vsn FROM " + table
            + " WHERE srcName = @srcName AND PerId = @perId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srcName", QueryParameterValue.string(srcName))
            .addNamedParameter("perId",   QueryParameterValue.string(perId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("next_vsn").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ vsnNo query interrupted", e);
        }
        return 1L;
    }

    private long queryLong(String sql, String col) {
        try {
            for (FieldValueList row : bigquery.query(
                    QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build()).iterateAll()) {
                return row.get(col).getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ sequence query interrupted", e);
        }
        return 1L;
    }

    private void runDml(QueryJobConfiguration config) {
        try {
            bigquery.query(config);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ DML interrupted", e);
        }
    }

    private static String strOrNull(FieldValueList row, String col) {
        try {
            var fv = row.get(col);
            return fv.isNull() ? null : fv.getStringValue();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
