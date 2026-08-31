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
 *       Source configuration is resolved per-source from the parameter DB;
 *       {@code --sourceType} is not used. Multiple sources run as parallel Beam branches in one job.
 *       Checkpoint (COMPLETED / FAILED_BNC / FAILED) is updated inside the pipeline by
 *       {@code PostDownloadFinalizeTransform} — no separate post-pipeline invocation needed.</li>
 *   <li>{@link #REPORT_PROCESSING} — read data already written to {@code DaRec}, apply the transform
 *       chain, and route output to one or more sinks (GCS / BQ / API). Full lifecycle tracked in
 *       {@code DaRefer}; per-output detail written to {@code RptOutput}.</li>
 *   <li>{@link #PIPELINE} — same {@code --reportName}/{@code --reportSubprocess} as
 *       {@link #REPORT_PROCESSING}; runs every datasource the report's own
 *       {@code ReportConfig.datasources[]} declares (batched into one Dataflow job, skipping any
 *       already {@code COMPLETED}), then the report itself. There is no separate pipeline config
 *       — the report's own datasource list and {@code is_required} flags already declare which
 *       datasources feed it and which are mandatory, so {@code PIPELINE} reuses that directly
 *       instead of redeclaring it. Differs from plain {@link #REPORT_PROCESSING} only in what
 *       happens when a declared datasource isn't {@code COMPLETED} yet: {@code REPORT_PROCESSING}
 *       fails immediately; {@code PIPELINE} runs it first. See {@code PipelineSequenceFactory}.
 *       Composes {@link #DATA_SOURCE_DOWNLOAD} and {@link #REPORT_PROCESSING} rather than
 *       replacing either — both remain independently runnable.</li>
 * </ul>
 */
public enum ProcessType {
    DATA_SOURCE_DOWNLOAD,
    REPORT_PROCESSING,
    PIPELINE
}
