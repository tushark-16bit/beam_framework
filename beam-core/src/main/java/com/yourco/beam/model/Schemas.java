package com.yourco.beam.model;

import org.apache.beam.sdk.schemas.Schema;

/**
 * Shared schema constants used across sources and transforms.
 *
 * <p>Sources that read opaque text (GCS newline-JSON, Pub/Sub payloads) emit
 * rows using {@link #RAW_JSON} — a single {@code raw_json} string field.
 * Downstream transforms (e.g., {@code flatten-json}) are responsible for
 * parsing and expanding that field into typed columns.
 */
public final class Schemas {

    private Schemas() {}

    /** Single-field schema for raw text sources (GCS, Pub/Sub). */
    public static final Schema RAW_JSON = Schema.builder()
            .addStringField("raw_json")
            .build();
}
