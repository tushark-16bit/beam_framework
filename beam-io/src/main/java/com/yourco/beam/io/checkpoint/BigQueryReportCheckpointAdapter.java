package com.yourco.beam.io.checkpoint;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.io.util.FileHeaderLegend;
import com.yourco.beam.model.ReportCheckpoint;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * BigQuery implementation of {@link ReportCheckpointAdapter}.
 *
 * <p>Reads and writes the four report tracking tables using BQ DML.
 * All operations run in the driver JVM — never inside Beam workers.
 *
 * <h2>ID generation</h2>
 * {@code SELECT IFNULL(MAX(id), 0) + 1} — not atomic across concurrent pipelines.
 * One driver JVM per period avoids collisions, or switch to UUID keys.
 */
public final class BigQueryReportCheckpointAdapter implements ReportCheckpointAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryReportCheckpointAdapter.class);
    private static final DateTimeFormatter BQ_DATETIME =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private final BigQuery bigquery;
    private final String   rptReferTable;    // `project.dataset.RptRefer`
    private final String   rptDaMapTable;    // `project.dataset.RptDaMap`
    private final String   rptStageDaTable;  // `project.dataset.RptStageDa`
    private final String   rptOutputTable;   // `project.dataset.RptOutput`
    private final String   daRecTable;       // `project.dataset.DaRec` (source for staging)

    public BigQueryReportCheckpointAdapter(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryReportCheckpointAdapter(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        String ds = options.getCheckpointBqDataset();
        this.rptReferTable   = "`" + project + "." + ds + "." + options.getRptReferTable()   + "`";
        this.rptDaMapTable   = "`" + project + "." + ds + "." + options.getRptDaMapTable()   + "`";
        this.rptStageDaTable = "`" + project + "." + ds + "." + options.getRptStageDaTable() + "`";
        this.rptOutputTable  = "`" + project + "." + ds + "." + options.getRptOutputTable()  + "`";
        this.daRecTable      = "`" + project + "." + ds + "." + options.getDaRecTable()      + "`";
        LOG.info("Report adapter tables: RptRefer={} RptDaMap={} RptStageDa={} RptOutput={}",
                 rptReferTable, rptDaMapTable, rptStageDaTable, rptOutputTable);
    }

    // ── RptRefer ─────────────────────────────────────────────────────────────

    @Override
    public long createCheckpoint(String rptNm, int perId, String rptDs) {
        long rptId = nextRptId();
        String now = LocalDateTime.now().format(BQ_DATETIME);

        String sql = "INSERT INTO " + rptReferTable
            + " (rpt_id, rpt_nm, per_id, rpt_ds, sta_cd, creat_ts, lst_updt_ts)"
            + " VALUES (@rptId, @rptNm, @perId, @rptDs, @staCd, @now, @now)";

        runDml(QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("rptId",  QueryParameterValue.int64(rptId))
            .addNamedParameter("rptNm",  QueryParameterValue.string(rptNm))
            .addNamedParameter("perId",  QueryParameterValue.int64(perId))
            .addNamedParameter("rptDs",  QueryParameterValue.string(rptDs != null ? rptDs : ""))
            .addNamedParameter("staCd",  QueryParameterValue.string(ReportCheckpoint.STA_LOADING))
            .addNamedParameter("now",    QueryParameterValue.dateTime(now))
            .setUseLegacySql(false).build());

        LOG.info("RptRefer LOADING row created: rpt_id={} rpt_nm={} per_id={}", rptId, rptNm, perId);
        return rptId;
    }

    @Override
    public void updateStatus(long rptId, String staCd) {
        String now = LocalDateTime.now().format(BQ_DATETIME);

        runDml(QueryJobConfiguration.newBuilder(
            "UPDATE " + rptReferTable
            + " SET sta_cd = @staCd, lst_updt_ts = @now WHERE rpt_id = @rptId")
            .addNamedParameter("staCd", QueryParameterValue.string(staCd))
            .addNamedParameter("now",   QueryParameterValue.dateTime(now))
            .addNamedParameter("rptId", QueryParameterValue.int64(rptId))
            .setUseLegacySql(false).build());

        LOG.info("RptRefer updated: rpt_id={} sta_cd={}", rptId, staCd);
    }

    @Override
    public boolean isCompleted(String rptNm, int perId) {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(
            "SELECT rpt_id FROM " + rptReferTable
            + " WHERE rpt_nm = @rptNm AND per_id = @perId AND sta_cd = @completed LIMIT 1")
            .addNamedParameter("rptNm",     QueryParameterValue.string(rptNm))
            .addNamedParameter("perId",     QueryParameterValue.int64(perId))
            .addNamedParameter("completed", QueryParameterValue.string(ReportCheckpoint.STA_COMPLETED))
            .setUseLegacySql(false).build();
        try {
            return bigquery.query(config).iterateAll().iterator().hasNext();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("RptRefer isCompleted query interrupted", e);
        }
    }

    // ── RptDaMap ─────────────────────────────────────────────────────────────

    @Override
    public long addDaMapping(long rptId, long daId) {
        long mapId = nextMapId();
        String now = LocalDateTime.now().format(BQ_DATETIME);

        runDml(QueryJobConfiguration.newBuilder(
            "INSERT INTO " + rptDaMapTable
            + " (map_id, rpt_id, da_id, lst_updt_ts)"
            + " VALUES (@mapId, @rptId, @daId, @now)")
            .addNamedParameter("mapId", QueryParameterValue.int64(mapId))
            .addNamedParameter("rptId", QueryParameterValue.int64(rptId))
            .addNamedParameter("daId",  QueryParameterValue.int64(daId))
            .addNamedParameter("now",   QueryParameterValue.dateTime(now))
            .setUseLegacySql(false).build());

        LOG.info("RptDaMap row created: map_id={} rpt_id={} da_id={}", mapId, rptId, daId);
        return mapId;
    }

    // ── RptStageDa ───────────────────────────────────────────────────────────

    @Override
    public void stageFromDaRec(long mapId, long daId) {
        // Batched staging: copies DaRec's pages into RptStageDa unchanged — one RptStageDa row
        // per DaRec page (≤250 records each, or a FILE-with-header source's
        // {"Data":[...],"DataHeaders":[...]}), not one row per individual source record. This
        // mirrors DaRec's own pagination and cuts RptStageDa's streaming-insert volume by the
        // same ~250x factor DaRec already gets. The per-record view every report's SQL expects
        // is reconstructed on read, in stagedDataSubquery() below — un-nesting moved from write
        // time to read time, entirely inside this adapter. No report's query_template changes.
        // stage_id is computed via ROW_NUMBER() to avoid per-row MAX lookups.
        String sql = "INSERT INTO " + rptStageDaTable
            + " (stage_id, map_id, stage_ds_json_tx, query_config_tx, load_dt, lst_updt_ts)"
            + " SELECT"
            + "   IFNULL((SELECT MAX(stage_id) FROM " + rptStageDaTable + "), 0)"
            + "     + ROW_NUMBER() OVER (),"
            + "   @mapId,"
            + "   row_da_json_tx,"
            + "   @queryConfigTx,"
            + "   CURRENT_DATE(),"
            + "   CURRENT_DATETIME()"
            + " FROM " + daRecTable
            + " WHERE da_id = @daId";

        runDml(QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("mapId",         QueryParameterValue.int64(mapId))
            .addNamedParameter("queryConfigTx",  QueryParameterValue.string(
                "{\"da_id\":" + daId + ",\"map_id\":" + mapId + "}"))
            .addNamedParameter("daId",          QueryParameterValue.int64(daId))
            .setUseLegacySql(false).build());

        LOG.info("Staged DaRec → RptStageDa (page copy): map_id={} da_id={}", mapId, daId);
    }

    @Override
    public String stagedDataSubquery(long mapId) {
        // RptStageDa.stage_ds_json_tx now holds a page (JSON array, or a FILE-with-header
        // source's {"Data":[...],"DataHeaders":[...]}) per row, exactly like DaRec.row_da_json_tx
        // — see stageFromDaRec() above. This subquery un-nests every page for mapId back into
        // individual source-row JSON objects, aliasing the result column back to
        // stage_ds_json_tx, so every caller (report transform SQL via {alias} substitution,
        // writeOutputBqTable(), a BQ/GCS output reading a datasource alias directly) keeps
        // seeing exactly the same "one row = one source record, column stage_ds_json_tx" shape
        // it always has — un-nesting is internal to this method, never something a
        // query_template author has to do. FileHeaderLegend.dataArrayExpr() extracts just the
        // row array either way, so the FILE source's DataHeaders legend is never unnested and
        // never reaches report input data, same guarantee as before, just applied on read
        // instead of on write.
        return "(SELECT row_json AS stage_ds_json_tx FROM " + rptStageDaTable
            + " CROSS JOIN UNNEST(" + FileHeaderLegend.dataArrayExpr("stage_ds_json_tx") + ") AS row_json"
            + " WHERE map_id = " + mapId + ")";
    }

    @Override
    public void clearStagedData(long rptId) {
        String sql = "DELETE FROM " + rptStageDaTable
            + " WHERE map_id IN ("
            + "   SELECT map_id FROM " + rptDaMapTable + " WHERE rpt_id = @rptId"
            + " )";

        runDml(QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("rptId", QueryParameterValue.int64(rptId))
            .setUseLegacySql(false).build());

        LOG.info("Cleared RptStageDa rows for rpt_id={}", rptId);
    }

    // ── RptOutput ────────────────────────────────────────────────────────────

    @Override
    public void writeOutput(long rptId, String outptCd, String outputDs,
                            String lineReferCd, String schedTx, double balAm, String rptTypeCd) {
        int vsnNo = nextVsnNo(rptId, outptCd);
        String now = LocalDateTime.now().format(BQ_DATETIME);

        runDml(QueryJobConfiguration.newBuilder(
            "INSERT INTO " + rptOutputTable
            + " (outpt_cd, rpt_dt, vsn_no, output_ds, line_refer_cd,"
            + "  sched_tx, bal_am, rpt_type_cd, rpt_id, lst_updt_ts)"
            + " VALUES (@outptCd, @now, @vsnNo, @outputDs, @lineReferCd,"
            + "         @schedTx, @balAm, @rptTypeCd, @rptId, @now)")
            .addNamedParameter("outptCd",     QueryParameterValue.string(outptCd))
            .addNamedParameter("now",         QueryParameterValue.dateTime(now))
            .addNamedParameter("vsnNo",       QueryParameterValue.int64(vsnNo))
            .addNamedParameter("outputDs",    QueryParameterValue.string(nvl(outputDs)))
            .addNamedParameter("lineReferCd", QueryParameterValue.string(nvl(lineReferCd)))
            .addNamedParameter("schedTx",     QueryParameterValue.string(nvl(schedTx)))
            .addNamedParameter("balAm",       QueryParameterValue.float64(balAm))
            .addNamedParameter("rptTypeCd",   QueryParameterValue.string(nvl(rptTypeCd)))
            .addNamedParameter("rptId",       QueryParameterValue.int64(rptId))
            .setUseLegacySql(false).build());

        LOG.info("RptOutput row written: rpt_id={} outpt_cd={} vsn_no={}", rptId, outptCd, vsnNo);
    }

    // ── Sequence helpers ──────────────────────────────────────────────────────

    private long nextRptId() {
        return queryLong("SELECT IFNULL(MAX(rpt_id), 0) + 1 AS next_id FROM " + rptReferTable,
                         "next_id");
    }

    private long nextMapId() {
        return queryLong("SELECT IFNULL(MAX(map_id), 0) + 1 AS next_id FROM " + rptDaMapTable,
                         "next_id");
    }

    private int nextVsnNo(long rptId, String outptCd) {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(
            "SELECT IFNULL(MAX(vsn_no), 0) + 1 AS next_vsn FROM " + rptOutputTable
            + " WHERE rpt_id = @rptId AND outpt_cd = @outptCd")
            .addNamedParameter("rptId",   QueryParameterValue.int64(rptId))
            .addNamedParameter("outptCd", QueryParameterValue.string(outptCd))
            .setUseLegacySql(false).build();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return (int) row.get("next_vsn").getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("RptOutput vsn_no query interrupted", e);
        }
        return 1;
    }

    private long queryLong(String sql, String col) {
        try {
            for (FieldValueList row : bigquery.query(
                    QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())
                    .iterateAll()) {
                return row.get(col).getLongValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Report sequence query interrupted", e);
        }
        return 1L;
    }

    private void runDml(QueryJobConfiguration config) {
        try {
            bigquery.query(config);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Report DML interrupted", e);
        }
    }

    private static String nvl(String v) { return v != null ? v : ""; }
}
