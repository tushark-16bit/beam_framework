package com.yourco.beam.options;

/**
 * Controls how the BigQuery sink behaves when the destination table already
 * contains data. Passed via {@code --writeDisposition} on the CLI.
 */
public enum WriteDispositionType {
    /** Appends new rows to the existing table. Not idempotent — re-runs duplicate data. */
    APPEND,
    /** Truncates the table before writing. Safe to re-run; full-refresh semantics. */
    TRUNCATE
}
