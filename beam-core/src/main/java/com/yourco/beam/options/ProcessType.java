package com.yourco.beam.options;

/**
 * Governs which half of the framework runs.
 *
 * <p>The two process types are always scheduled as separate Airflow DAGs so that
 * data acquisition and report generation can fail, retry, and scale independently.
 *
 * <ul>
 *   <li>{@link #DATA_SOURCE_DOWNLOAD} — fetch raw data from external systems (API, file, BQ)
 *       and persist it to GCS or BQ. Source configuration is resolved from the parameter DB;
 *       {@code --sourceType} is not used. Runs multiple sources in parallel in one Beam job.</li>
 *   <li>{@link #REPORT_PROCESSING} — read already-downloaded data, apply the transform chain,
 *       and write results. Uses {@code --sourceType} / {@code --sinkType} from CLI as usual.</li>
 * </ul>
 */
public enum ProcessType {
    DATA_SOURCE_DOWNLOAD,
    REPORT_PROCESSING
}
