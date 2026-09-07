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
 *       {@link #REPORT_PROCESSING}; <b>submits</b> (does not wait for) a single batched Dataflow
 *       job covering every datasource the report's own {@code ReportConfig.datasources[]}
 *       declares and isn't already {@code COMPLETED}, then returns immediately. There is no
 *       separate pipeline config — the report's own datasource list and {@code is_required}
 *       flags already declare which datasources feed it and which are mandatory. This call does
 *       <b>not</b> run the report itself and does <b>not</b> block until the datasources finish —
 *       see {@link #STATUS_CHECK} for how a caller (an Airflow sensor) learns when it's safe to
 *       invoke {@link #REPORT_PROCESSING} next. See {@code PipelineSequenceFactory}.</li>
 *   <li>{@link #STATUS_CHECK} — fast, synchronous, DB-only readiness check; submits nothing and
 *       never blocks. Reads {@code DaRefer} (the same row {@code PostDownloadFinalizeTransform}
 *       writes from the Beam worker) to report whether previously-submitted
 *       {@link #DATA_SOURCE_DOWNLOAD}/{@link #PIPELINE} work has reached a terminal state.
 *       {@code --reportName} set → checks every datasource a report's {@code datasources[]}
 *       declares (mirrors the required/optional gate {@link #PIPELINE} used to run inline);
 *       {@code --reportName} blank → checks the single {@code --datasourceName}/
 *       {@code --subprocessName}/{@code --periodId}. Exists because this framework's runner
 *       platform forbids {@code PipelineResult.waitUntilFinish()} — the driver JVM cannot block
 *       for a submitted job's full runtime, so job outcome must be polled externally instead.
 *       See {@code DataSourceStatusChecker} and {@code Main.runStatusCheck()}.</li>
 * </ul>
 */
public enum ProcessType {
    DATA_SOURCE_DOWNLOAD,
    REPORT_PROCESSING,
    PIPELINE,
    STATUS_CHECK
}
