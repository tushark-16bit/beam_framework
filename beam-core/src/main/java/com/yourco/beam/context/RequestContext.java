package com.yourco.beam.context;

import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.MDC;

import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * The one id that follows a request through every channel: driver JVM logs, Dataflow workers,
 * BigQuery jobs, the Dataflow job itself and failure emails.
 *
 * <p>The id is {@code --jobRunId}: {@link #init} generates a UUID when none was passed and writes
 * it back into the options, so it travels to the workers inside the serialized pipeline options.
 * A worker (or any code that only has the options) calls {@link #attach} to make it current there.
 *
 * <ul>
 *   <li><b>Logs</b> — {@code jobRunId} MDC key; add {@code %X{jobRunId}} to the log pattern.
 *       MDC is per thread, so each worker DoFn entry point calls {@link #attach}.</li>
 *   <li><b>Any code, any module</b> — {@link #current()} (process-wide, so it also works from
 *       threads that did not call {@code attach}).</li>
 *   <li><b>BigQuery / Dataflow</b> — {@link #labels()} gives {@code job_run_id} in the lowercase
 *       form those services accept; query it in {@code INFORMATION_SCHEMA.JOBS} (the worker
 *       logs are not visible, the job labels are).</li>
 * </ul>
 */
public final class RequestContext {

    public static final String MDC_KEY = "jobRunId";
    public static final String LABEL_KEY = "job_run_id";

    private static volatile String current = "";

    private RequestContext() {}

    /** Driver JVM, once, at the top of main(): keeps a supplied id, else generates a UUID. */
    public static String init(FrameworkOptions options) {
        String id = options.getJobRunId();
        if (id == null || id.isBlank()) {
            id = UUID.randomUUID().toString();
            options.setJobRunId(id);
        }
        attach(id);
        return id;
    }

    /** Makes {@code id} the current request id process-wide and for the calling thread's logs. */
    public static void attach(String id) {
        if (id == null || id.isBlank()) return;
        current = id;
        MDC.put(MDC_KEY, id);
    }

    /** Worker entry points: attach the id carried in the pipeline options. */
    public static void attach(FrameworkOptions options) {
        attach(options.getJobRunId());
    }

    /** The current request id, or "" before {@link #init}/{@link #attach}. */
    public static String current() {
        return current;
    }

    /** Label map for BigQuery jobs / Dataflow ({@code job_run_id}); empty when no id is set. */
    public static Map<String, String> labels() {
        String label = toLabel(current);
        return label.isEmpty() ? Map.of() : Map.of(LABEL_KEY, label);
    }

    /**
     * GCP label values allow only lowercase letters, digits, '-' and '_' (max 63 chars):
     * lowercases and replaces anything else with '_'.
     */
    static String toLabel(String id) {
        if (id == null) return "";
        String s = id.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_-]", "_");
        return s.length() > 63 ? s.substring(0, 63) : s;
    }
}
