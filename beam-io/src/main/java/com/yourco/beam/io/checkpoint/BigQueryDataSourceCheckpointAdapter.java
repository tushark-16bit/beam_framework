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
 * <h2>DaId generation</h2>
 * {@code SELECT IFNULL(MAX(DaId), 0) + 1} — not atomic across concurrent pipelines.
 * Use a single driver JVM per period to avoid collisions, or switch DaId to UUID.
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
    public long createCheckpoint(String SrceNm, String PerId, String FlNm) {
        long daId  = nextDaId();
        long vsnNo = nextVsnNo(SrceNm, PerId);
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(Instant.now().toEpochMilli());

        String sql = "INSERT INTO " + table
            + " (DaId, SrceNm, VsnNo, PerId, FlNm, BalAndCntlSmryTx, StaCd, CreatedTs, LstUpdtTs)"
            + " VALUES (@daId, @srceNm, @vsnNo, @perId, @flNm, NULL, @staCd, @now, @now)";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("daId",   QueryParameterValue.int64(daId))
            .addNamedParameter("srceNm", QueryParameterValue.string(SrceNm))
            .addNamedParameter("vsnNo",  QueryParameterValue.int64(vsnNo))
            .addNamedParameter("perId",  QueryParameterValue.string(PerId))
            .addNamedParameter("flNm",   QueryParameterValue.string(FlNm != null ? FlNm : ""))
            .addNamedParameter("staCd",  QueryParameterValue.string(DataSourceCheckpoint.STA_LOADING))
            .addNamedParameter("now",    QueryParameterValue.timestamp(nowMicros))
            .setUseLegacySql(false)
            .build();

        runDml(config);
        LOG.info("DaRefer row created: DaId={} SrceNm={} PerId={} VsnNo={} StaCd=LOADING",
                 daId, SrceNm, PerId, vsnNo);
        return daId;
    }

    @Override
    public void updateStatus(long DaId, String StaCd, String balAndCntlSmryTx) {
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(Instant.now().toEpochMilli());
        String sql = "UPDATE " + table
            + " SET StaCd = @staCd, BalAndCntlSmryTx = @bnc, LstUpdtTs = @now"
            + " WHERE DaId = @daId";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("staCd", QueryParameterValue.string(StaCd))
            .addNamedParameter("bnc",   balAndCntlSmryTx != null
                                        ? QueryParameterValue.string(balAndCntlSmryTx)
                                        : QueryParameterValue.string(""))
            .addNamedParameter("now",   QueryParameterValue.timestamp(nowMicros))
            .addNamedParameter("daId",  QueryParameterValue.int64(DaId))
            .setUseLegacySql(false)
            .build();

        runDml(config);
        LOG.info("DaRefer updated: DaId={} StaCd={}", DaId, StaCd);
    }

    @Override
    public boolean isCompleted(String SrceNm, String PerId) {
        // Filter on StaCd first so a newer LOADING/FAILED row cannot shadow an older COMPLETED row.
        String sql = "SELECT DaId FROM " + table
            + " WHERE SrceNm = @srceNm AND PerId = @perId AND StaCd = @completed"
            + " LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srceNm",    QueryParameterValue.string(SrceNm))
            .addNamedParameter("perId",     QueryParameterValue.string(PerId))
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
    public Optional<DataSourceCheckpoint> getLatest(String SrceNm, String PerId) {
        String sql = "SELECT * FROM " + table
            + " WHERE SrceNm = @srceNm AND PerId = @perId"
            + " ORDER BY LstUpdtTs DESC LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srceNm", QueryParameterValue.string(SrceNm))
            .addNamedParameter("perId",  QueryParameterValue.string(PerId))
            .setUseLegacySql(false)
            .build();

        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return Optional.of(new DataSourceCheckpoint(
                    row.get("DaId").getLongValue(),
                    row.get("SrceNm").getStringValue(),
                    row.get("VsnNo").getLongValue(),
                    strOrNull(row, "PerId"),
                    strOrNull(row, "FlNm"),
                    strOrNull(row, "BalAndCntlSmryTx"),
                    row.get("StaCd").getStringValue(),
                    Instant.parse(row.get("CreatedTs").getStringValue()),
                    Instant.parse(row.get("LstUpdtTs").getStringValue())
                ));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRefer getLatest query interrupted", e);
        }
        return Optional.empty();
    }

    // ── Sequence helpers ──────────────────────────────────────────────────────

    private long nextDaId() {
        String sql = "SELECT IFNULL(MAX(DaId), 0) + 1 AS next_id FROM " + table;
        return queryLong(sql, "next_id");
    }

    private long nextVsnNo(String SrceNm, String PerId) {
        String sql = "SELECT IFNULL(MAX(VsnNo), 0) + 1 AS next_vsn FROM " + table
            + " WHERE SrceNm = @srceNm AND PerId = @perId";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("srceNm", QueryParameterValue.string(SrceNm))
            .addNamedParameter("perId",  QueryParameterValue.string(PerId))
            .setUseLegacySql(false)
            .build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row.get("next_vsn").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DaRefer VsnNo query interrupted", e);
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
