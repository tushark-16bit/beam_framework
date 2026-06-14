package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Configuration for one file output produced by a report.
 *
 * <p>Stored in {@code report_output_config}. After all transformation steps have
 * been materialised, each output config drives a BQ export job that writes the
 * result table to a file in GCS. The file is then attached to the report email.
 *
 * <h2>Supported formats</h2>
 * <ul>
 *   <li>{@link #CSV} — comma-separated values (most compatible, used by BQ extract jobs)</li>
 *   <li>{@link #JSON} — newline-delimited JSON (BQ native)</li>
 * </ul>
 *
 * <h2>File naming</h2>
 * Final GCS path is assembled as:
 * {@code {gcsPath}/{filePrefix}{reportName}_{periodId}{fileSuffix}.{ext}}
 */
public final class ReportOutputConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String CSV  = "CSV";
    public static final String JSON = "JSON";

    public final int     outputOrder;
    /** Alias (datasource alias or transform step output alias) whose table to export. */
    public final String  inputAlias;
    public final String  outputFormat;
    /** GCS directory path, e.g. {@code gs://bucket/reports/daily/}. */
    public final String  gcsPath;
    public final String  filePrefix;
    public final String  fileSuffix;
    public final boolean includeHeader;

    public ReportOutputConfig(int outputOrder, String inputAlias, String outputFormat,
                              String gcsPath, String filePrefix, String fileSuffix,
                              boolean includeHeader) {
        this.outputOrder   = outputOrder;
        this.inputAlias    = inputAlias;
        this.outputFormat  = outputFormat;
        this.gcsPath       = gcsPath;
        this.filePrefix    = filePrefix != null ? filePrefix : "";
        this.fileSuffix    = fileSuffix != null ? fileSuffix : "";
        this.includeHeader = includeHeader;
    }

    public boolean isCsv()  { return CSV.equalsIgnoreCase(outputFormat); }
    public boolean isJson() { return JSON.equalsIgnoreCase(outputFormat); }
}
