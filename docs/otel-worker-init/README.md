# Worker-side OpenTelemetry init — JvmInitializer approach

Reference drop, not part of this repo's buildable framework. The classes referenced here
(`OpenTelemetryConfiguration`, `Main`) live in a separate, internal org repo (package
`com.aexp.facoe.lumi.runner`) that isn't attached to this session — these files are checked in
here purely as a transport, since that repo isn't reachable from this chat. Copy them into the
real repo at the paths below; nothing here needs to compile against `com.yourco.beam.*`.

## Background

That internal repo's `Main.java` calls `OpenTelemetryConfiguration.configureOpenTelemetry()` at
the top of `main()` — building an `OpenTelemetrySdk`, registering it globally
(`buildAndRegisterGlobal()`), and installing `OpenTelemetryAppender.install(sdk)` to bridge the
existing Logback/Log4j2 config into the OTel Logs SDK, exporting to an internal ELF ingest
endpoint over OTLP/gRPC.

Because that setup lives entirely inside `main()`'s method body, it only ever runs in the
launcher/driver JVM Dataflow spins up to submit the job. Runner v2 workers are separate JVM
processes — they execute the serialized DoFn graph, never the code inside `main()` — so none of
that OTel configuration exists on a worker, and worker-side logs never make it into the same
OTLP/ELF pipeline the driver's logs do (only Dataflow's own default Cloud Logging capture of
WARN/ERROR was visible; INFO was being dropped upstream of anything OTel-related).

## The fix: `JvmInitializer`, not a javaagent, not per-DoFn `@Setup`

Beam ships an SPI, `org.apache.beam.sdk.harness.JvmInitializer`, that the Runner v2 SDK harness
calls exactly once when a worker JVM boots — before any DoFn is instantiated or any bundle is
processed. This is the correct worker-side equivalent of what `main()` already does for the
launcher: run the exact same `configureOpenTelemetry()` call, just triggered by Beam's own worker
bootstrap instead of by `Main.main()`.

Two alternatives were considered and rejected:
- **OTel javaagent + Dockerfile env vars** — works, but is redundant here: the target repo already
  has a manual, code-based OTel setup (`OpenTelemetryAppender.install()`). Adding a javaagent on
  top would attempt a second `GlobalOpenTelemetry.set()` and throw `IllegalStateException`.
- **Calling `configureOpenTelemetry()` from every DoFn's `@Setup`** — works but is fragile: `@Setup`
  runs once per DoFn *instance*, and a worker JVM can host several DoFn instances across
  bundles/threads, risking the same double-registration crash, and requires touching every DoFn
  class in the framework instead of one.

`JvmInitializer.onStartup()` runs exactly once per JVM process, full stop — matching this
framework's existing convention of discovering pluggable implementations via `ServiceLoader`
(`TransformRegistry` for `BeamTransform`, `ReportPipelineFactory.discoverEmailUtility()` for
`EmailSendUtility`), just using a Beam-provided SPI instead of one the framework defines itself.

## Files in this drop

```
WorkerOpenTelemetryInitializer.java                                    → src/main/java/com/aexp/facoe/lumi/runner/
META-INF-services/org.apache.beam.sdk.harness.JvmInitializer          → src/main/resources/META-INF/services/
                                                                          org.apache.beam.sdk.harness.JvmInitializer
```

(`META-INF-services/` here is just a flat stand-in for the real `META-INF/services/` nesting —
keep the filename exactly as-is, only the containing directory changes on copy.)

## What NOT to duplicate

`Main.main()`'s `--traceparent=` extraction and `TextMapPropagator`-based context linking is
launcher-specific — it stitches the launcher process's own span onto whatever externally
provided trace context invoked it (Airflow, `gcloud`, etc.). Workers don't need this: they create
their own spans under their own resource attributes, under the same registered global
`OpenTelemetrySdk`. If a DoFn ever wants to create a custom span or emit a log through the OTel
bridge, it should fetch the already-registered instance rather than build another:

```java
OpenTelemetry otel = GlobalOpenTelemetry.get();
```

## Two things to verify before shipping

1. **Cert bundling** — `loadTrustedCertificates()` needs to resolve the same way on a worker as it
   does on the launcher. If it reads a classpath resource bundled inside the fat jar (the same jar
   ships to both launcher and worker per the Dockerfile's `FLEX_TEMPLATE_JAVA_CLASSPATH`), this
   just works. If it reads from an absolute host filesystem path that only exists on the
   launcher's base image, either move it into the jar or make sure that path exists in every
   container this image starts.
2. **Worker egress** — confirm the worker VM's network/firewall policy allows reaching the ELF
   ingest host on 443. Worker subnets/firewall rules sometimes differ from whatever the launcher's
   network policy allows — a common silent-failure point, since the exporter would just fail to
   publish, batched and swallowed, with nothing obviously breaking the pipeline.
