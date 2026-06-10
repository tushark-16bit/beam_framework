package com.yourco.beam.options;

/** Supported output sink types. Passed via --sinkType on the CLI. */
public enum SinkType {
    GCS,
    BQ,
    PUBSUB
}
