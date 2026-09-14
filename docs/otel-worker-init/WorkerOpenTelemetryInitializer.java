package com.aexp.facoe.lumi.runner;

import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Runs {@link OpenTelemetryConfiguration#configureOpenTelemetry()} on a Dataflow Runner v2
 * worker JVM — the worker-side equivalent of what {@code Main.main()} already does for the
 * launcher JVM.
 *
 * <p>Discovered and invoked by Beam's SDK harness via {@code ServiceLoader}
 * (see {@code META-INF/services/org.apache.beam.sdk.harness.JvmInitializer}), the same discovery
 * mechanism this framework already uses for {@code BeamTransform} (via {@code TransformRegistry})
 * and {@code EmailSendUtility}. {@code onStartup()} is called exactly once per worker JVM, before
 * any DoFn is instantiated or any bundle is processed — this is NOT a per-DoFn {@code @Setup}
 * hook, so there is no risk of {@code buildAndRegisterGlobal()} being called twice in the same
 * process (which throws {@code IllegalStateException}).
 *
 * <p>Do NOT also call {@code configureOpenTelemetry()} from inside a DoFn's {@code @Setup} —
 * this class is the single place worker-side OTel initialization belongs. Application code that
 * needs a {@code Tracer}/{@code Logger} on a worker (e.g. from inside a DoFn) should fetch the
 * instance this already registered globally, rather than build its own:
 *
 * <pre>{@code
 * OpenTelemetry otel = GlobalOpenTelemetry.get();
 * }</pre>
 *
 * <p>{@code Main.main()}'s own {@code --traceparent=} extraction and root-span linking logic is
 * launcher-specific (it links the launcher process's own span to whatever invoked it — e.g.
 * Airflow) and is intentionally NOT duplicated here; workers create spans under their own
 * resource attributes and don't need that CLI-arg-based context extraction.
 */
public final class WorkerOpenTelemetryInitializer implements JvmInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerOpenTelemetryInitializer.class);

    @Override
    public void onStartup() {
        try {
            OpenTelemetryConfiguration.configureOpenTelemetry();
            LOG.info("OpenTelemetry initialized on worker JVM startup");
        } catch (IOException e) {
            // Thrown by loadTrustedCertificates() inside configureOpenTelemetry(). Failing the
            // worker JVM's startup here (rather than swallowing) is deliberate: a worker running
            // with no OTel logging/tracing configured would silently produce no worker logs at
            // all under the ELF pipeline, which is worse than a loud, early failure.
            throw new UncheckedIOException(
                "Failed to initialize OpenTelemetry on worker startup", e);
        }
    }

    @Override
    public void beforeProcessing(PipelineOptions options) {
        // No-op — all setup happens in onStartup(), before PipelineOptions are even available.
    }
}
