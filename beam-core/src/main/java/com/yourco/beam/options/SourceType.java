package com.yourco.beam.options;

/**
 * Supported input source types.
 *
 * <p>For {@link ProcessType#REPORT_PROCESSING}, pass {@code --sourceType} on the CLI.
 * For {@link ProcessType#DATA_SOURCE_DOWNLOAD}, source types come from the parameter DB
 * (one per {@link com.yourco.beam.model.SourceConfig}); {@code --sourceType} is ignored.
 */
public enum SourceType {
    /** Newline-delimited JSON files on GCS. */
    GCS,
    /** BigQuery table or SQL query. */
    BQ,
    /** Pub/Sub subscription (streaming). */
    PUBSUB,
    /** REST API with optional pagination. Configuration fetched from parameter DB. */
    API,
    /** CSV or Excel file on GCS. Configuration fetched from parameter DB. */
    FILE
}
