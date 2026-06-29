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
 * <p>Reads and writes the {@code DaRefer} table using BQ DML.
 * The table is small and accessed only from the driver JVM — never from Beam workers.
 *
 * <h2>da_id generation</h2>
 * {@code SELECT IFNULL(MAX(da_id), 0) + 1} — not atomic across concurrent pipelines.
 * Use a single driver JVM per period to avoid collisions, or switch da_id to UUID.
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
                   + "." + options.getDaReferTable() + "`";
        LOG.info("DaRefer table: {}", table);
    }

    @Override
    public long createCheckpoint(String srceNm, String perId, String flNm) {
        long daId  = nextDaId();
        long vsnNo = nextVsnNo(srceNm, perId);
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(Instant.now().toEpochMilli());

        String sql = "INSERT INTO " + table
            + " (da_id, srce_nm, vsn_no, per_id, fl_nm, bal_and_cntl_smry_tx, sta_cd, created_ts, lst_updt_ts)"
            + " VALUES (@daId, @srceNm, @vsnNo, @perId, @flNm, NULL, @staCd, @now, @now)";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("daId",   QueryParameterValue.int64(daId))
            .addNamedParameter("srceNm", QueryParameterValue.string(srceNm))
            .addNamedParameter("vsnNo",  QueryParameterValue.int64(vsnNo))
            .addNamedParameter("perId",  QueryParameterValue.string(perId))
            .addNamedParameter("flNm",   QueryParameterValue.string(flNm != null ? flNm : ""))
            .addNamedParameter("staCd",  QueryParameterValue.string(DataSourceCheckpoint.STA_LOADING))
            .addNamedParameter("now",    QueryParameterValue.timestamp(nowMicros))
            .setUseLegacySql(false)
            .build();

        runDml(config);
        LOG.info("DaRefer row created: da_id={} srce_nm={} per_id={} vsn_no={} sta_cd=LOADING",
                 daId, srceNm, perId, vsnNo);
        return daId;
    }

    @Override
    public void updateStatus(long daId, String staCd, String balAndCntlSmryTx) {
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(Instant.now().toEpochMilli());
        String sql = "UPDATE " + table
            + " SET sta_cd = @staCd, bal_and_cntl_smry_tx = @bnc, lst_updt_ts = @now"
            + " WHERE da_id = @daId";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("staCd", QueryParameterValue.string(staCd))
            .addNamedParameter("bnc",   balAndCntlSmryTx != null
                                        ? QueryParameterValue.string(balAndCntlSmryTx)
                                        : QueryParameterValue.string(""))
            .addNamedParameter("now",   QueryParameterValue.timestamp(nowMicros))
            .addNamedParameter("daId",  QueryParameterValue.int64(daId))
            .setUseLegacySql(false)
            .build();

        runDml(config);
        LOG.info("DaRefer updated: da_id={} sta_cd={}", daId, staCd);
    }

    @Override
    public boolean isCompleted(String srceNm, String perId) {
        // Filter on sta_cd first so a newer LOADING/FAILED row cannot shadow an older COMPLETED row.
        String sql = "SELECT da_id FROM " + table
            + " WHERE srce_nm = @srceNm AND per_id = @perId AND sta_cd = @completed"
            + " LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srceNm",    QueryParameterValue.string(srceNm))
            .addNamedParameter("perId",     QueryParameterValue.string(perId))
            .addNamedParameter("completed", QueryParameterValue.string(DataSourceCheckpoint.STA_COMPLETED))
            .setUseLegacySql(false)
            .build();

        try {
            return bigquery.query(config).iterateAll().iterator().hasNext();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRefer isCompleted query interrupted", e);
        }
    }

    @Override
    public Optional<DataSourceCheckpoint> getLatest(String srceNm, String perId) {
        String sql = "SELECT * FROM " + table
            + " WHERE srce_nm = @srceNm AND per_id = @perId"
            + " ORDER BY lst_updt_ts DESC LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srceNm", QueryParameterValue.string(srceNm))
            .addNamedParameter("perId",  QueryParameterValue.string(perId))
            .setUseLegacySql(false)
            .build();

        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return Optional.of(new DataSourceCheckpoint(
                    row.get("da_id").getLongValue(),
                    row.get("srce_nm").getStringValue(),
                    row.get("vsn_no").getLongValue(),
                    strOrNull(row, "per_id"),
                    strOrNull(row, "fl_nm"),
                    strOrNull(row, "bal_and_cntl_smry_tx"),
                    row.get("sta_cd").getStringValue(),
                    Instant.parse(row.get("created_ts").getStringValue()),
                    Instant.parse(row.get("lst_updt_ts").getStringValue())
                ));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRefer getLatest query interrupted", e);
        }
        return Optional.empty();
    }

    @Override
    public long fetchLatestCompletedDaId(String srceNm, String perId) {
        String sql = "SELECT da_id FROM " + table
            + " WHERE srce_nm = @srceNm AND per_id = @perId AND sta_cd = 'COMPLETED'"
            + " ORDER BY lst_updt_ts DESC LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srceNm", QueryParameterValue.string(srceNm))
            .addNamedParameter("perId",  QueryParameterValue.string(perId))
            .setUseLegacySql(false)
            .build();

        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("da_id").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRefer fetchLatestCompletedDaId query interrupted", e);
        }
        throw new IllegalArgumentException(
            "No COMPLETED DaRefer row found for srce_nm=" + srceNm
            + " per_id=" + perId + " — ensure DATA_SOURCE_DOWNLOAD ran successfully first");
    }

    // ── Sequence helpers ──────────────────────────────────────────────────────

    private long nextDaId() {
        String sql = "SELECT IFNULL(MAX(da_id), 0) + 1 AS next_id FROM " + table;
        return queryLong(sql, "next_id");
    }

    private long nextVsnNo(String srceNm, String perId) {
        String sql = "SELECT IFNULL(MAX(vsn_no), 0) + 1 AS next_vsn FROM " + table
            + " WHERE srce_nm = @srceNm AND per_id = @perId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srceNm", QueryParameterValue.string(srceNm))
            .addNamedParameter("perId",  QueryParameterValue.string(perId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("next_vsn").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRefer vsn_no query interrupted", e);
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
            throw new IllegalStateException("DaRefer sequence query interrupted", e);
        }
        return 1L;
    }

    private void runDml(QueryJobConfiguration config) {
        try {
            bigquery.query(config);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRefer DML interrupted", e);
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
