package com.yourco.beam.io.report;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Writes one row to {@code COM_CmnRptDtl} for each output file or sink produced
 * by a REPORT_PROCESSING run.
 *
 * <p>Called from driver JVM only — never inside a DoFn.
 */
public final class BigQueryCommonReportDetailAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCommonReportDetailAdapter.class);

    private final BigQuery bigquery;
    private final String   dataset;
    private final String   table;

    public BigQueryCommonReportDetailAdapter(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryCommonReportDetailAdapter(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        this.dataset = project + "." + options.getCheckpointBqDataset();
        this.table   = options.getCmnRptDtlTable();
    }

    /**
     * Inserts one row into COM_CmnRptDtl.
     *
     * @param srceSysNm   report name (SrceNm in DaRefer)
     * @param flNm        output file name, BQ table ref, or API endpoint
     * @param flDaJsonTx  output data as JSON (may be null)
     * @param recCt       number of rows written (may be null if unknown)
     * @param createUserId correlation ID (jobRunId)
     */
    public void insertDetail(String srceSysNm, String flNm, String flDaJsonTx,
                              Long recCt, String createUserId) {
        String now = Instant.now().toString();

        Map<String, Object> row = new HashMap<>();
        row.put("srce_sys_nm",      srceSysNm);
        row.put("fl_nm",           flNm);
        row.put("srce_fl_create_ts", now);
        row.put("fl_da_json_tx",   flDaJsonTx);
        row.put("rec_ct",          recCt);
        row.put("creat_ts",        now);
        row.put("create_user_id",  createUserId);
        row.put("lst_updt_ts",     now);
        row.put("lst_updt_user_id", createUserId);

        String[] parts = dataset.split("\\.");
        String project  = parts[0];
        String datasetId = parts[1];

        try {
            InsertAllResponse response = bigquery.insertAll(
                InsertAllRequest.newBuilder(TableId.of(project, datasetId, table))
                    .addRow(row)
                    .build());
            if (response.hasErrors()) {
                LOG.warn("COM_CmnRptDtl insert had errors for {}: {}",
                         flNm, response.getInsertErrors());
            } else {
                LOG.info("COM_CmnRptDtl row inserted: srceSysNm={} flNm={}", srceSysNm, flNm);
            }
        } catch (Exception e) {
            LOG.error("Failed to insert COM_CmnRptDtl row for {}: {}", flNm, e.getMessage(), e);
        }
    }
}
