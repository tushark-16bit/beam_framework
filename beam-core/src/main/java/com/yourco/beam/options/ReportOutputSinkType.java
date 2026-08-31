package com.yourco.beam.options;

/** Output sink type for a single report output step. */
public enum ReportOutputSinkType {
    /** BQ extract job → GCS file (CSV or JSON). Eligible for email attachment. */
    GCS,
    /** Copy result table to a downstream BQ table (WRITE_TRUNCATE). */
    BQ,
    /** POST result rows as a JSON array to an HTTP endpoint. */
    API
}
