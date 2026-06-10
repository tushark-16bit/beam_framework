package com.yourco.beam.options;

/** Supported input source types. Passed via --sourceType on the CLI. */
public enum SourceType {
    GCS,
    BQ,
    PUBSUB
}
