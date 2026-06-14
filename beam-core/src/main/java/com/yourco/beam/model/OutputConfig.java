package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Per-source output destination for a {@code DATA_SOURCE_DOWNLOAD} pipeline.
 *
 * <p>Each {@link SourceConfig} carries one {@code OutputConfig} that tells the pipeline
 * where to write the fetched (and optionally transformed) data for that specific source.
 * Sources are never merged — every source has its own independent output.
 *
 * <p>Stored in the {@code source_config} table under the {@code output_*} columns.
 */
public final class OutputConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String TYPE_BQ  = "BQ";
    public static final String TYPE_GCS = "GCS";

    public static final String WRITE_MODE_TRUNCATE = "TRUNCATE";
    public static final String WRITE_MODE_APPEND   = "APPEND";

    /** Output type: {@code BQ} or {@code GCS}. */
    public final String outputType;

    // ── BigQuery output ──────────────────────────────────────────────────────
    public final String bqProjectId;
    public final String bqDataset;
    public final String bqTable;

    // ── GCS output ───────────────────────────────────────────────────────────
    public final String gcsPath;

    /** Write behaviour: {@code TRUNCATE} (overwrite) or {@code APPEND}. Default: TRUNCATE. */
    public final String writeMode;

    public OutputConfig(String outputType, String bqProjectId, String bqDataset, String bqTable,
                        String gcsPath, String writeMode) {
        this.outputType  = outputType;
        this.bqProjectId = bqProjectId;
        this.bqDataset   = bqDataset;
        this.bqTable     = bqTable;
        this.gcsPath     = gcsPath;
        this.writeMode   = (writeMode != null && !writeMode.isBlank()) ? writeMode : WRITE_MODE_TRUNCATE;
    }

    /** Returns {@code project:dataset.table} for use with BigQuery APIs. */
    public String bqTableRef() {
        return bqProjectId + ":" + bqDataset + "." + bqTable;
    }

    public boolean isBq()  { return TYPE_BQ.equalsIgnoreCase(outputType); }
    public boolean isGcs() { return TYPE_GCS.equalsIgnoreCase(outputType); }
    public boolean isTruncate() { return WRITE_MODE_TRUNCATE.equalsIgnoreCase(writeMode); }

    @Override
    public String toString() {
        return "OutputConfig{type=" + outputType
            + (isBq() ? ", table=" + bqTableRef() : ", gcs=" + gcsPath)
            + ", mode=" + writeMode + "}";
    }
}
