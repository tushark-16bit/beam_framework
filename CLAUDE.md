# Agent Guide — beam-pipeline-framework

This file is the primary reference for any AI agent working in this repository.
Read it fully before making any changes. It is written to be followed by any
capable language model, not only Claude.

For human-readable documentation, see [`README.md`](README.md) and the per-module
`README.md` files. This file is specifically optimised for agent comprehension.

---

## 1. What this project is (3-sentence orientation)

This is a configurable Apache Beam ETL pipeline framework written in Java 17 that runs
on GCP Dataflow and is triggered by Apache Airflow DAGs. Everything the pipeline does —
what it reads, which transforms it applies, where it writes — is controlled by CLI flags
at runtime with no code changes. New data transformations are added by implementing a
single Java interface and registering a class name in a text file; the framework discovers
and wires them automatically.

---

## 2. Mandatory directive — README updates

> **Every code change in this repository MUST be reflected in the README files.**
> This is not optional. Before committing, update every README that is affected.

### Which README to update

| Type of change | READMEs to update |
|---|---|
| New transform added | `beam-transforms/README.md` (add to table), root `README.md` (Built-in transforms table) |
| New source or sink | `beam-io/README.md` (update table and SourceRouter/SinkRouter sections) |
| New utility class | `beam-utils/README.md` (add to Utilities table with description) |
| New pipeline option (flag) | `beam-core/README.md` (Adding a new flag section), root `README.md` (config table) |
| Change to pipeline assembly | `beam-runner/README.md` (PipelineFactory section) |
| New module added | Root `README.md` (Module structure section), parent `pom.xml` `<modules>` |
| Any architectural change | Root `README.md` (Architecture section) |
| Change to serialization contract | `beam-core/README.md` (Serialization rules table) |
| Change to build or run steps | `beam-runner/README.md` (Building, Running sections) |

### The update rule in plain terms

If you add a class → describe it in the module's README.
If you add a flag → add it to the config table in root README.
If you change how something works → update the section that describes it.
If you're unsure which README → update all that mention the affected area.
If a README becomes wrong due to your change → fix it, do not leave it stale.

---

## 3. File reading order — fastest path to understanding

To understand this codebase from scratch, read these files in this order:

```
1.  beam-core/.../options/FrameworkOptions.java       — all CLI flags; the config contract
2.  beam-core/.../transform/BeamTransform.java         — the SPI interface; the extension contract
3.  beam-runner/.../runner/PipelineFactory.java        — how the pipeline is assembled end-to-end
4.  beam-runner/.../runner/Main.java                   — the entry point; how options become a run
5.  beam-core/.../transform/TransformRegistry.java     — how transforms are discovered at startup
6.  beam-io/.../source/SourceRouter.java               — how source type maps to a connector
7.  beam-io/.../sink/SinkRouter.java                   — how sink type maps to a connector
8.  beam-transforms/.../FilterNullsTransform.java      — simplest real transform; the pattern to follow
9.  beam-transforms/.../MaskPiiTransform.java          — transform with runtime config from options
10. beam-transforms/.../EnrichFromExternalApiTransform.java — @Setup/@Teardown lifecycle pattern
11. beam-core/.../retry/RetryingDoFn.java              — how DLQ routing works element-by-element
12. beam-utils/.../CalendarUtils.java                  — the stubs that need implementation
```

---

## 4. Complete file map

Every source file in the project, one line each:

### beam-core — contracts only, no GCP code

```
options/FrameworkOptions.java          All CLI flags. Every pipeline option lives here.
options/SourceType.java                Enum: GCS | BQ | PUBSUB
options/SinkType.java                  Enum: GCS | BQ | PUBSUB
options/RetryPolicyType.java           Enum: NONE | FIXED | EXPONENTIAL
options/WriteDispositionType.java      Enum: APPEND | TRUNCATE

transform/BeamTransform.java           SPI interface. Defines name(), toComposite(). Has SUCCESS_TAG and DEAD_LETTER_TAG.
transform/TransformRegistry.java       Loads BeamTransform impls via ServiceLoader. resolve(chainSpec) → List<BeamTransform>.

retry/RetryPolicy.java                 Interface: shouldRetry(attempt, cause), delayMs(attempt). Delay capped at 200ms.
retry/ExponentialRetryPolicy.java      Exponential back-off, ThreadLocalRandom jitter, hard 200ms cap.
retry/FixedRetryPolicy.java            Fixed delay, also capped at 200ms.
retry/RetryingDoFn.java                DoFn wrapping a SerializableFunction with retry + DLQ routing via TupleTag.

model/FailedRecord.java                DLQ envelope. @DefaultCoder(SerializableCoder.class). Has of() and ofRaw() factories.
model/Schemas.java                     Shared constants. RAW_JSON = single-field Schema for GCS/PubSub sources.
```

### beam-io — connectors

```
source/SourceRouter.java               Stateless factory. switch(sourceType) → source PTransform.
source/BigQuerySourceTransform.java    Extends PTransform<PBegin,PCollection<Row>>. Reads BQ table or SQL query.
source/GcsSourceTransform.java         Extends PTransform<PBegin,PCollection<Row>>. Reads GCS glob as JSON lines.
source/PubSubSourceTransform.java      Extends PTransform<PBegin,PCollection<Row>>. Reads Pub/Sub subscription.

sink/SinkRouter.java                   Stateless factory. switch(sinkType) → sink PTransform.
sink/BigQuerySinkTransform.java        Writes PCollection<Row> to BQ. Reads --writeDisposition (TRUNCATE default).
sink/GcsSinkTransform.java             Writes PCollection<Row> as newline-delimited JSON to GCS.
sink/PubSubSinkTransform.java          Publishes each Row as a JSON string to a Pub/Sub topic.
sink/DeadLetterSinkTransform.java      Writes PCollection<FailedRecord> as JSON lines to GCS DLQ path.

util/JsonUtils.java                    Shared Row→JSON serializer. Handles types correctly (not everything quoted).
```

### beam-utils — stateless helpers, no pipeline graph code

```
BigQuerySchemaUtils.java    Fetch real BQ Schema at pipeline-build time. fetchBeamSchema(), tableExists(), fetchRowCount().
GcsUtils.java               GCS operations for driver JVM: pathHasFiles(), listFiles(), writeTextFile(), deletePrefix().
SecretManagerUtils.java     Fetch secrets from GCP Secret Manager by ID. Never store secret values in options.
RowValidationUtils.java     Row validators for use inside DoFns: requireFields(), matchesPattern(), inRange(), oneOf().
MetricsUtils.java           Factory for consistently-named Beam counters/distributions: transformCounter(), pipelineDlqTotal().
CalendarUtils.java          STUBS — isBusinessDay(), nextBusinessDay(), applyOffset(), businessDaysInRange(). Implement these.
DateUtils.java              Run date resolution, formatting (ISO/compact/display), partitionedPath(), shardedTable().
```

### beam-transforms — pluggable transform implementations

```
FilterNullsTransform.java             Token: filter-nulls. Drops rows with any null. Routes dropped to DLQ. Counter metric.
MaskPiiTransform.java                 Token: mask-pii. SHA-256 hashes fields listed in --piiFields. Configurable at runtime.
EnrichFromExternalApiTransform.java   Token: enrich-from-api. SAMPLE ONLY — shows @Setup/@Teardown for HTTP clients.

META-INF/services/com.yourco.beam.transform.BeamTransform   SPI manifest — one class name per line.
```

### beam-runner — entry point only

```
Main.java             Parses CLI args → PipelineFactory → pipeline.run() → waitUntilFinish() for batch only.
PipelineFactory.java  Assembles graph: source → transform loop → sink + DLQ flatten. Wires retry policy.
```

---

## 5. Architecture rules — non-negotiable

### Module dependency direction

```
beam-runner → beam-core, beam-io, beam-utils, beam-transforms
beam-transforms → beam-core, beam-utils
beam-io → beam-core
beam-utils → beam-core
beam-core → (nothing internal)
```

**Never violate this.** If `beam-core` imports from `beam-io`, you have introduced a circular dependency. If `beam-utils` imports from `beam-transforms`, you have coupled utilities to business logic. The compiler will catch circular deps; architectural violations it will not.

### The wire type

All transforms communicate via `PCollection<Row>` with a declared `Schema`.
`Row` is Beam's schema-aware type. Every transform input and output must call `.setRowSchema()`.
Do not use raw bytes, Strings, or Avro as the inter-transform wire format.

### The output contract

Every `BeamTransform.toComposite()` returns a `PTransform<PCollection<Row>, PCollectionTuple>`.
The tuple MUST contain outputs for exactly these two tags (defined as constants on `BeamTransform`):

```java
BeamTransform.SUCCESS_TAG      TupleTag<Row>          — successfully processed rows
BeamTransform.DEAD_LETTER_TAG  TupleTag<FailedRecord> — failed rows after all retries
```

`PipelineFactory` collects all `DEAD_LETTER_TAG` outputs, flattens them, and writes to DLQ.
If your transform cannot produce failures, emit an empty `PCollection<FailedRecord>`:
```java
PCollection<FailedRecord> empty = input.getPipeline()
    .apply(Create.empty(SerializableCoder.of(FailedRecord.class)));
return PCollectionTuple.of(SUCCESS_TAG, successRows).and(DEAD_LETTER_TAG, empty);
```

---

## 6. Serialization rules — violations fail silently at build time and loudly at runtime

Beam serializes every `DoFn` instance and ships it to remote Dataflow workers.
Violations cause `NotSerializableException` at runtime, not at compile time.

| Rule | Correct | Wrong |
|---|---|---|
| DoFn class type | Named `static final` inner class | Anonymous class or lambda |
| DoFn field types | All `Serializable` (String, int, List, etc.) | Non-serializable objects (HttpClient, Connection) |
| Non-serializable resources | Declare `transient`, create in `@Setup`, close in `@Teardown` | Hold in a non-transient field |
| TupleTag instances | `static final` on the class | Created inline in `processElement` |
| Function interfaces | `SerializableFunction<A,B>` (Beam's) | `java.util.function.Function<A,B>` |
| Lambda captures | Capture only serializable local variables | Capture `this` of an outer non-serializable class |

See `EnrichFromExternalApiTransform` for a fully annotated example of the `@Setup`/`@Teardown` pattern.

---

## 7. How to make each type of change

### Add a new transform

1. Create class in `beam-transforms/src/main/java/com/yourco/beam/transforms/`
2. Implement `BeamTransform` — `name()` returns the CLI token, `toComposite()` returns the `PTransform`
3. Use only named `static final` inner classes for composites and DoFns
4. Output to `SUCCESS_TAG` and `DEAD_LETTER_TAG` in every code path
5. Call `.setRowSchema()` on the success output inside the composite's `expand()`
6. Add the fully-qualified class name to `beam-transforms/src/main/resources/META-INF/services/com.yourco.beam.transform.BeamTransform`
7. **Update `beam-transforms/README.md`** — add a row to the Built-in transforms table
8. **Update root `README.md`** — add a row to the Built-in transforms reference table

### Add a new pipeline option (CLI flag)

1. Add getter + setter pair in `beam-core/.../options/FrameworkOptions.java`
2. Annotate with `@Description("...")` — describe what it does and give an example value
3. Add `@Default.String/Integer/Enum(...)` if it has a sensible default; `@Validation.Required` if mandatory
4. Read it in the transform or utility via `options.getMyNewFlag()`
5. **Update `beam-core/README.md`** — add to the Adding a new flag section
6. **Update root `README.md`** — add to the How to configure behaviour table

### Add a new source connector

1. Create class in `beam-io/src/main/java/com/yourco/beam/io/source/`
2. Extend `PTransform<PBegin, PCollection<Row>>`
3. Use `Schemas.RAW_JSON` for raw text sources, or `BigQuerySchemaUtils.fetchBeamSchema()` for typed sources
4. Call `.setRowSchema()` on the returned `PCollection<Row>`
5. Use named `static final SerializableFunction` — never anonymous lambdas — for `MapElements.via()`
6. Add a value to `SourceType` enum in `beam-core`
7. Add a case to `SourceRouter.route()` switch expression
8. Add required flags to `FrameworkOptions`
9. **Update `beam-io/README.md`** — update the connectors table and SourceRouter section
10. **Update root `README.md`** — update the source config flags table

### Add a new utility class

1. Create class in `beam-utils/src/main/java/com/yourco/beam/utils/`
2. Make it a `final` class with a `private` constructor — all methods static
3. No Beam pipeline graph code — no `PTransform`, no `DoFn`, no `Pipeline`
4. If called from a `DoFn`, ensure every method called inside `@ProcessElement` is thread-safe and stateless
5. **Update `beam-utils/README.md`** — add to the Utilities table

### Add a new module

1. Create directory `beam-newmodule/` with `pom.xml`, `src/main/java/`, `src/test/java/`
2. Add `<module>beam-newmodule</module>` to root `pom.xml` `<modules>` in the correct dependency order
3. Add `<dependency>` entry to root `pom.xml` `<dependencyManagement>`
4. Add the dependency to whichever module needs it
5. Create `beam-newmodule/README.md`
6. **Update root `README.md`** — add to Module structure section and dependency diagram

### Implement a CalendarUtils stub

1. Open `beam-utils/src/main/java/com/yourco/beam/utils/CalendarUtils.java`
2. Replace the `throw new UnsupportedOperationException(...)` body with real logic
3. The Javadoc in each method describes what it should do and suggests integration approaches
4. Once implemented, `CalendarUtils.resolveEffectiveDate(options)` and `DateUtils` work automatically
5. **Update `beam-utils/README.md`** — remove the "stubs" label, describe the integration

---

## 8. How the pipeline runs — the full execution path

Understanding this is essential before changing `PipelineFactory` or `Main`.

```
Step 1: Main.main(args)
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FrameworkOptions.class)
        → produces a typed FrameworkOptions object
        → validation fails fast here if @Validation.Required flags are missing

Step 2: PipelineFactory.assemble(options)
        → Pipeline.create(options)                   — creates an empty pipeline
        → SourceRouter.route(pipeline, options)      — adds source node to graph, returns PCollection<Row>
        → TransformRegistry.load()                   — ServiceLoader discovers all registered BeamTransforms
        → registry.resolve(options.getTransformChain()) — splits "a,b,c" → [transformA, transformB, transformC]

Step 3: Transform loop (for each transform in chain)
        → transform.toComposite(options)             — creates the PTransform
        → current = current.apply(name, composite)  — adds node to graph; returns PCollectionTuple
        → deadLetterOutputs.add(result.get(DEAD_LETTER_TAG))
        → current = result.get(SUCCESS_TAG)          — next transform sees the success path only

Step 4: SinkRouter.route(current, options)           — adds sink node to graph

Step 5: DLQ wiring (if deadLetterOutputs is not empty and --deadLetterSink is set)
        → PCollectionList.of(deadLetterOutputs).apply(Flatten.pCollections())
        → allFailures.apply(new DeadLetterSinkTransform(options))

Step 6: pipeline.run()                               — NOW data actually moves
        DirectRunner  → runs in this JVM
        DataflowRunner → serialises graph, submits to GCP Dataflow, workers download fat JAR

Step 7: result.waitUntilFinish()   — only for batch (GCS, BQ sources)
        PUBSUB source → streaming, never blocks
```

Nothing in steps 1–5 processes any data. They only describe the computation graph.

---

## 9. SPI registration — how transforms are discovered

The framework uses Java's `ServiceLoader` to discover `BeamTransform` implementations.
This means you NEVER need to modify `PipelineFactory`, `TransformRegistry`, or `Main`
when adding a new transform. Only the manifest file changes.

**The manifest file (one class name per line):**
```
beam-transforms/src/main/resources/META-INF/services/com.yourco.beam.transform.BeamTransform
```

**Current contents:**
```
com.yourco.beam.transforms.FilterNullsTransform
com.yourco.beam.transforms.MaskPiiTransform
com.yourco.beam.transforms.EnrichFromExternalApiTransform
```

**Critical build note:** the `maven-shade-plugin` in `beam-runner/pom.xml` uses
`ServicesResourceTransformer`. This merges all `META-INF/services` files from all
JARs on the classpath into one combined file. If you add a new transform module,
add it as a Maven dependency in `beam-runner/pom.xml` and the merger handles the rest.

---

## 10. Things you must never do

| Never do this | Do this instead |
|---|---|
| Anonymous class or lambda as a DoFn | Named `static final` inner class |
| `java.util.function.Function` as a DoFn field | `org.apache.beam.sdk.transforms.SerializableFunction` |
| `Thread.sleep()` for more than 200ms inside a DoFn | Cap at 200ms or use retry-topic pattern |
| Import from `beam-io` or `beam-transforms` inside `beam-core` | `beam-core` has no internal dependencies |
| Import from `beam-io` inside `beam-utils` | `beam-utils` depends only on `beam-core` |
| Hold secrets as pipeline options values | Pass the Secret Manager ID; fetch the value at runtime |
| Call `BigQuerySchemaUtils` or `GcsUtils` inside a `DoFn` `@ProcessElement` | Call in driver JVM (constructors/PipelineFactory) |
| Create a `TupleTag` inside `@ProcessElement` | `static final` fields on the DoFn class |
| Modify `PipelineFactory` to hardcode a new transform | Register via SPI manifest |
| Call `result.waitUntilFinish()` for streaming pipelines | Check source type first (`Main.isBatchSource()`) |
| Commit version changes only in child `pom.xml` | All versions in parent `pom.xml` `<dependencyManagement>` |
| Bypass `@DefaultCoder` on custom types used in `PCollection` | Annotate with `@DefaultCoder(SerializableCoder.class)` |
| Leave a README out of date after a code change | Update the README in the same commit |

---

## 11. Retry and dead-letter pattern

The DLQ system works at the element level. Every transform that can fail routes
failures to `DEAD_LETTER_TAG`. `PipelineFactory` collects all dead-letter streams,
flattens them, and writes to GCS via `DeadLetterSinkTransform`.

```
element enters transform
    │
    ├─ success path ──────────────────────────────────────────── SUCCESS_TAG → next transform
    │
    └─ failure path
           │
           ├─ RetryPolicy.shouldRetry(attempt, exception)?
           │       yes → wait RetryPolicy.delayMs(attempt) (≤200ms) → retry
           │       no  → FailedRecord.of(row, exception, attemptCount)
           │                    └──────────────────────────── DEAD_LETTER_TAG → DLQ sink
```

`RetryingDoFn` implements this pattern generically. Transforms use it by passing a
`SerializableFunction<Row, Row>` to `new RetryingDoFn(fn, retryPolicy)`.

---

## 12. Calendar and date options for report pipelines

These options are consumed by `CalendarUtils` and `DateUtils` in `beam-utils`:

```
--runDate=2024-01-15      Business date for this run. ISO-8601. Defaults to today UTC.
--calendarName=NYSE        Which holiday calendar to use. DEFAULT = Mon-Fri, no holidays.
--businessDayOffset=-1     Move N business days from runDate. -1 = previous business day (T-1).
```

`DateUtils.resolveRunDate(options)` — parses `--runDate` or returns today.
`CalendarUtils.resolveEffectiveDate(options)` — applies the offset using the calendar.
`CalendarUtils.*` methods are stubs. See `beam-utils/CalendarUtils.java` Javadoc for how to implement.

---

## 13. Email and notification options

```
--businessEmail=reports@co.com            Stakeholder / report delivery address
--devErrorEmail=oncall@co.com             Developer / failure alert address
--emailSmtpHost=smtp.gmail.com            SMTP server
--emailSmtpPort=587                       SMTP port
--smtpPasswordSecretId=projects/p/secrets/smtp/versions/latest
```

Fetch the SMTP password at runtime:
```java
String password = SecretManagerUtils.fetchSecret(options.getSmtpPasswordSecretId());
```

Never log `password`. Never store it in any field. Pass it directly to the SMTP client constructor.
The Dataflow and Cloud Composer service accounts need `roles/secretmanager.secretAccessor`.

---

## 14. Build and run reference

```bash
# Build the fat JAR (from project root)
mvn package -pl beam-runner -am -DskipTests

# Run locally
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner --sourceType=GCS --gcsSourcePath=gs://b/in/*.json \
  --transformChain=filter-nulls --sinkType=GCS --gcsSinkPath=gs://b/out/

# Upload to GCS for Dataflow
gsutil cp beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  gs://my-bucket/jars/beam-runner-latest.jar
```

---

## 15. Key invariants to preserve

1. `beam-core` has zero dependencies on sibling modules — it is the root.
2. All sources return `PCollection<Row>` with `setRowSchema()` called.
3. All transforms receive and return `PCollection<Row>` (via `PCollectionTuple`).
4. The transform chain is dynamic — never hardcoded in `PipelineFactory`.
5. Dead-letter records are never silently dropped — they route to `DEAD_LETTER_TAG`.
6. Secrets are never stored in `FrameworkOptions` values — only Secret Manager IDs are.
7. Every code change is accompanied by a README update.
