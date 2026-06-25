package com.yourco.beam.model;

import com.yourco.beam.options.ReportOutputSinkType;

import java.io.Serializable;

/**
 * Configuration for one output produced by a report.
 *
 * <p>Stored in {@code report_output_config}. After all transformation steps have
 * been materialised, each output config drives a sink operation determined by
 * {@link #sinkType}:
 *
 * <ul>
 *   <li>{@code GCS} — BQ extract job writes the result table to GCS as CSV or JSON.
 *       Produces a file eligible for email attachment.</li>
 *   <li>{@code BQ}  — copies the result table to {@link #bqSinkTable}
 *       (e.g. a shared analytics dataset in another project).</li>
 *   <li>{@code API} — queries the result table row-by-row as JSON and POSTs
 *       the array to {@link #apiEndpoint}.</li>
 * </ul>
 *
 * <h2>BQ table schema additions ({@code report_output_config})</h2>
 * <pre>{@code
 *   sink_type          STRING,          -- GCS (default) | BQ | API
 *   -- GCS-specific
 *   output_format      STRING,          -- CSV | JSON
 *   gcs_path           STRING,          -- gs://bucket/reports/
 *   file_prefix        STRING,
 *   file_suffix        STRING,
 *   include_header     BOOL,
 *   -- BQ-specific
 *   bq_sink_table      STRING,          -- project.dataset.table
 *   -- API-specific
 *   api_endpoint       STRING,          -- https://api.example.com/reports
 *   api_method         STRING,          -- POST | PUT (default: POST)
 *   api_auth_secret_id STRING,          -- Secret Manager secret ID for Bearer token
 *   api_headers_json   STRING           -- {"X-Custom-Header": "value"}
 * }</pre>
 */
public final class ReportOutputConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String CSV  = "CSV";
    public static final String JSON = "JSON";

    public final int                 outputOrder;
    /** Alias (datasource alias or transform step output alias) whose table to export. */
    public final String              inputAlias;
    public final ReportOutputSinkType sinkType;

    // ── GCS sink fields ───────────────────────────────────────────────────────
    public final String  outputFormat;
    /** GCS directory path, e.g. {@code gs://bucket/reports/daily/}. */
    public final String  gcsPath;
    public final String  filePrefix;
    public final String  fileSuffix;
    public final boolean includeHeader;

    // ── BQ sink fields ────────────────────────────────────────────────────────
    /** Destination BQ table for BQ sink: {@code project.dataset.table}. */
    public final String  bqSinkTable;

    // ── API sink fields ───────────────────────────────────────────────────────
    /** Target HTTP endpoint for API sink. */
    public final String  apiEndpoint;
    /** HTTP method: POST or PUT. Defaults to POST. */
    public final String  apiMethod;
    /** Secret Manager secret ID for the Bearer token. Never the raw token. */
    public final String  apiAuthSecretId;
    /** Additional headers as a JSON map: {@code {"X-Key":"value"}}. */
    public final String  apiHeadersJson;

    public ReportOutputConfig(int outputOrder, String inputAlias, ReportOutputSinkType sinkType,
                              String outputFormat, String gcsPath, String filePrefix,
                              String fileSuffix, boolean includeHeader,
                              String bqSinkTable,
                              String apiEndpoint, String apiMethod,
                              String apiAuthSecretId, String apiHeadersJson) {
        this.outputOrder    = outputOrder;
        this.inputAlias     = inputAlias;
        this.sinkType       = sinkType != null ? sinkType : ReportOutputSinkType.GCS;
        this.outputFormat   = outputFormat;
        this.gcsPath        = gcsPath;
        this.filePrefix     = filePrefix != null ? filePrefix : "";
        this.fileSuffix     = fileSuffix != null ? fileSuffix : "";
        this.includeHeader  = includeHeader;
        this.bqSinkTable    = bqSinkTable;
        this.apiEndpoint    = apiEndpoint;
        this.apiMethod      = apiMethod != null ? apiMethod : "POST";
        this.apiAuthSecretId = apiAuthSecretId;
        this.apiHeadersJson  = apiHeadersJson;
    }

    public boolean isCsv()  { return CSV.equalsIgnoreCase(outputFormat); }
    public boolean isJson() { return JSON.equalsIgnoreCase(outputFormat); }
}
