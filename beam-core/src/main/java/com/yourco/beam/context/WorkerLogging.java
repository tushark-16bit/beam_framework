package com.yourco.beam.context;

import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.MDC;

/**
 * Open/close hooks every Dataflow step (each DoFn) wraps its {@code @ProcessElement} in:
 *
 * <pre>{@code
 * @ProcessElement
 * public void processElement(@Element X x, ..., PipelineOptions pipelineOptions) {
 *     WorkerLogging.open(pipelineOptions);      // first thing
 *     try {
 *         ... work, with its own catches ...
 *     } finally {
 *         WorkerLogging.close();                // always
 *     }
 * }
 * }</pre>
 *
 * <p>{@link #open} makes the request id current on the worker thread (see {@link RequestContext}).
 * {@link #close} releases it. These two methods are the single place to call a deployment's own
 * worker-logging setup/teardown (e.g. {@code WorkerLoggingUtil.addWorkerLoggingResourced(...)} and
 * its close counterpart), so every step picks it up without further edits.
 */
public final class WorkerLogging {

    private WorkerLogging() {}

    public static void open(PipelineOptions pipelineOptions) {
        RequestContext.attach(pipelineOptions.as(FrameworkOptions.class));
        // <- deployment hook: WorkerLoggingUtil.addWorkerLoggingResourced(...)
    }

    public static void close() {
        // <- deployment hook: the matching close call of WorkerLoggingUtil
        MDC.remove(RequestContext.MDC_KEY);
    }
}
