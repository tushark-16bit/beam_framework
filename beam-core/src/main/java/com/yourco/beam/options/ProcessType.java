package com.yourco.beam.options;

/**
 * Governs which half of the framework runs.
 *
 * <p>The two process types are always scheduled as separate Airflow DAGs so that
 * data acquisition and report generation can fail, retry, and scale independently.
 *
 * <h2>Why every call here blocks until fully done</h2>
 * This framework's runner platform forbids {@code PipelineResult.waitUntilFinish()} — calling it
 * on a submitted job's {@code PipelineResult} specifically does not work here — but the invoking
 * process (Airflow) <em>can</em> hold a single JVM invocation open for as long as needed. So
 * instead of returning the moment a Beam job is submitted, {@link #DATA_SOURCE_DOWNLOAD} and
 * {@link #PIPELINE} submit, then block in their own plain {@code Thread.sleep} poll loop —
 * re-reading {@code DaRefer} (the same row {@code PostDownloadFinalizeTransform} writes from the
 * worker) via {@code DataSourceStatusChecker} — until the work reaches a terminal state, and only
 * then return (or throw). One Airflow task, one JVM invocation, everything through to email.
 *
 * <ul>
 *   <li>{@link #DATA_SOURCE_DOWNLOAD} — fetch raw data from external systems (API, file, BQ)
 *       and persist every row as a JSON blob to {@code DaRec} (keyed by {@code DaId} from {@code DaRefer}).
 *       Source configuration is resolved per-source from the parameter DB;
 *       {@code --sourceType} is not used. Multiple sources run as parallel Beam branches in one job.
 *       Checkpoint (COMPLETED / FAILED_BNC / FAILED) is updated inside the pipeline by
 *       {@code PostDownloadFinalizeTransform} — no separate post-pipeline invocation needed.
 *       Submits, then blocks (poll loop, not {@code waitUntilFinish()}) until the source reaches
 *       {@code COMPLETED} or throws on a terminal failure/timeout — see {@code Main.runDataSourceDownload()}.</li>
 *   <li>{@link #REPORT_PROCESSING} — read data already written to {@code DaRec}, apply the transform
 *       chain, and route output to one or more sinks (GCS / BQ / API). Full lifecycle tracked in
 *       {@code DaRefer}; per-output detail written to {@code RptOutput}. Runs entirely in the
 *       driver JVM (no Beam pipeline submitted), so it was never affected by the
 *       {@code waitUntilFinish()} restriction.</li>
 *   <li>{@link #PIPELINE} — same {@code --reportName}/{@code --reportSubprocess} as
 *       {@link #REPORT_PROCESSING}. One blocking call: submits a single batched Dataflow job
 *       covering every datasource the report's own {@code ReportConfig.datasources[]} declares
 *       and isn't already {@code COMPLETED}, blocks in a poll loop until every <em>required</em>
 *       one reaches {@code COMPLETED} (an optional one that fails just logs a warning), then runs
 *       the report itself and returns. There is no separate pipeline config — the report's own
 *       datasource list and {@code is_required} flags already declare which datasources feed it
 *       and which are mandatory. See {@code PipelineSequenceFactory}.</li>
 *   <li>{@link #STATUS_CHECK} — fast, synchronous, DB-only readiness check; submits nothing and
 *       never blocks or sleeps — a single BQ read (or a handful, for a pipeline check), then
 *       returns immediately. <b>Optional diagnostic only</b> — normal operation doesn't need it,
 *       since {@link #DATA_SOURCE_DOWNLOAD}/{@link #PIPELINE} now poll internally and return only
 *       once actually done. Useful for an ops dashboard, a manual check, or a script that wants a
 *       cheap one-shot answer without holding a JVM open. {@code --reportName} set → checks every
 *       datasource a report's {@code datasources[]} declares; blank → checks the single
 *       {@code --datasourceName}/{@code --subprocessName}/{@code --periodId}.
 *       See {@code DataSourceStatusChecker} and {@code Main.runStatusCheck()}.</li>
 * </ul>
 */
public enum ProcessType {
    DATA_SOURCE_DOWNLOAD,
    REPORT_PROCESSING,
    PIPELINE,
    STATUS_CHECK
}
