package com.yourco.beam.options;

/**
 * Governs which half of the framework runs.
 *
 * <p>The two process types are always scheduled as separate Airflow DAGs so that
 * data acquisition and report generation can fail, retry, and scale independently.
 *
 * <ul>
 *   <li>{@link #DATA_SOURCE_DOWNLOAD} — fetch raw data from external systems (API, file, BQ)
 *       and persist every row as a JSON blob to {@code DaRec} (keyed by {@code DaId} from {@code DaRefer}).
 *       Source configuration is resolved per-source from {@code source_config} in the parameter DB;
 *       {@code --sourceType} is not used. Multiple sources run as parallel Beam branches in one job.</li>
 *   <li>{@link #REPORT_PROCESSING} — read data already written to {@code DaRec}, apply the transform
 *       chain, and route output to one or more sinks (GCS / BQ / API). Full lifecycle tracked in
 *       {@code DaRefer}; per-output detail written to {@code COM_CmnRptDtl}.</li>
 * </ul>
 */
public enum ProcessType {
    DATA_SOURCE_DOWNLOAD,
    REPORT_PROCESSING
}
