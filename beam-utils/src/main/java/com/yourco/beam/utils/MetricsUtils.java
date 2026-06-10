package com.yourco.beam.utils;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;

/**
 * Factory helpers for creating consistently-named Beam metrics.
 *
 * <h2>Why use this instead of calling Metrics.counter() directly?</h2>
 * Beam metric names appear in the Dataflow UI, Cloud Monitoring, and alerting
 * dashboards. Consistent naming conventions make them easy to find and query.
 * This class enforces a {@code namespace/name} convention across the framework.
 *
 * <h2>Standard namespaces</h2>
 * <pre>
 *   pipeline          — top-level pipeline metrics (rows in, rows out, DLQ count)
 *   transform.{name}  — per-transform metrics (e.g., transform.filter-nulls)
 *   io.source         — source read metrics
 *   io.sink           — sink write metrics
 * </pre>
 *
 * <h2>Usage inside a DoFn</h2>
 * Metrics must be declared as instance fields on the {@code DoFn}, not as local
 * variables, because Beam aggregates them per-worker instance.
 *
 * <pre>{@code
 * public static final class MyDoFn extends DoFn<Row, Row> {
 *
 *     // Declare as fields — Beam registers these per worker instance
 *     private final Counter rowsProcessed  = MetricsUtils.transformCounter("my-transform", "rows_processed");
 *     private final Counter rowsDropped    = MetricsUtils.transformCounter("my-transform", "rows_dropped");
 *     private final Distribution rowSizeMs = MetricsUtils.transformDistribution("my-transform", "processing_ms");
 *
 *     @ProcessElement
 *     public void processElement(@Element Row row, OutputReceiver<Row> out) {
 *         long start = System.currentTimeMillis();
 *         // ... do work ...
 *         rowsProcessed.inc();
 *         rowSizeMs.update(System.currentTimeMillis() - start);
 *         out.output(row);
 *     }
 * }
 * }</pre>
 */
public final class MetricsUtils {

    private MetricsUtils() {}

    // ── Pipeline-level metrics ────────────────────────────────────────────────

    /** Total rows entering the pipeline from the source. */
    public static Counter pipelineRowsIn() {
        return Metrics.counter("pipeline", "rows_in");
    }

    /** Total rows successfully written to the sink. */
    public static Counter pipelineRowsOut() {
        return Metrics.counter("pipeline", "rows_out");
    }

    /** Total rows routed to the dead-letter queue across all transforms. */
    public static Counter pipelineDlqTotal() {
        return Metrics.counter("pipeline", "dlq_total");
    }

    // ── Transform-level metrics ───────────────────────────────────────────────

    /**
     * Counter scoped to a specific transform.
     * The metric appears in Dataflow UI as {@code transform.{transformName}/{metricName}}.
     *
     * @param transformName the value of {@link com.yourco.beam.transform.BeamTransform#name()}
     * @param metricName    snake_case name, e.g. {@code rows_dropped}, {@code rows_enriched}
     */
    public static Counter transformCounter(String transformName, String metricName) {
        return Metrics.counter("transform." + transformName, metricName);
    }

    /**
     * Distribution scoped to a specific transform.
     * Tracks min/max/mean/count — useful for processing latency or value distributions.
     *
     * @param transformName the transform this metric belongs to
     * @param metricName    snake_case name, e.g. {@code processing_ms}, {@code value_size_bytes}
     */
    public static Distribution transformDistribution(String transformName, String metricName) {
        return Metrics.distribution("transform." + transformName, metricName);
    }

    // ── IO metrics ────────────────────────────────────────────────────────────

    /** Rows read from the pipeline source. */
    public static Counter sourceRowsRead(String sourceType) {
        return Metrics.counter("io.source", "rows_read." + sourceType.toLowerCase());
    }

    /** Rows written to the pipeline sink. */
    public static Counter sinkRowsWritten(String sinkType) {
        return Metrics.counter("io.sink", "rows_written." + sinkType.toLowerCase());
    }

    // ── Gauge (current value, e.g. queue depth) ───────────────────────────────

    /**
     * Gauge for tracking a current value (as opposed to a cumulative count).
     * Example uses: active connection count, current queue depth, memory usage.
     *
     * @param namespace  metric namespace
     * @param metricName snake_case metric name
     */
    public static Gauge gauge(String namespace, String metricName) {
        return Metrics.gauge(namespace, metricName);
    }
}
