# beam-runner

Entry point only. Wires all other modules together and produces the deployable fat JAR.
You should rarely need to edit this module.

---

## What lives here

| Class | Purpose |
|---|---|
| `Main` | Parses CLI args, routes by `--processType` (and `--reportName` for REPORT_PROCESSING), delegates to the right factory. Wraps the whole dispatch in one catch that calls `FailureNotifier.notify()` then rethrows — see its own section below. `runDataSourceDownload()` submits (`pipeline.run()`) then BLOCKS in `DataSourceStatusChecker.awaitSingle()`'s poll loop until done. `runStatusCheck()` handles `STATUS_CHECK`, an optional non-blocking diagnostic |
| `DataSourcePipelineFactory` | `DATA_SOURCE_DOWNLOAD`: validates params, fetches configs, creates LOADING checkpoints, assembles per-source Beam branches. `assemble(options)` (single `--datasourceName`) delegates to public `assembleForConfigs(options, List<SourceConfig>)`, which `PipelineSequenceFactory` also calls directly with several explicitly-fetched configs to batch them into one job. Both classify their own (config/assembly-time) failures into `DataSourceDownloadException`; only builds+returns the `Pipeline`, never calls `run()` itself |
| `PostDownloadFinalizeTransform` | Final pipeline step for each `DATA_SOURCE_DOWNLOAD` source: row/BnC validation, optional `data_transform_query` (replaces stored rows once validated, via an atomic DELETE+INSERT transaction), checkpoint update (COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED), `--manualOverrun` cleanup of the superseded previous run's DaRec rows, and failure email — all running in the Beam worker, independent of whether the driver JVM that submitted the job is still running (though now it usually still is, blocked in its poll loop) |
| `ReportPipelineFactory` | `REPORT_PROCESSING` (DB-configured): orchestrates BQ jobs + email in driver JVM; uses `ReportCheckpointAdapter` for RptRefer/RptDaMap/RptStageDa/RptOutput tracking; writes final result to per-report BQ table (`output_bq_table` from config) if set; no Beam pipeline submitted, so no waiting to do. Report-completion email uses `EmailSendUtility` (`beam-io`), discovered via `ServiceLoader` SPI or injected via constructor — see its own section below. Classifies its own failures into `ReportProcessingException`, one `Reason` per phase |
| `SmtpReportEmailAdapter` | SMTP implementation of `ReportEmailAdapter`; used only by `PostDownloadFinalizeTransform`'s DATA_SOURCE_DOWNLOAD failure email now |
| `PipelineFactory` | `REPORT_PROCESSING` (legacy): assembles generic source → transform chain → sink Beam pipeline. Its non-streaming path still calls `waitUntilFinish()` directly — a known gap, not fixed alongside the rest of this section since it has no checkpoint table to poll instead (see "Why blocking is a poll loop, not `waitUntilFinish()`" below) |
| `PipelineSequenceFactory` | `PIPELINE`: same `--reportName`/`--reportSubprocess` as `REPORT_PROCESSING`, no separate config — one blocking call: submits one batched Dataflow job for whichever not-yet-`COMPLETED` datasources the report's own `datasources[]` declares, blocks until every required one is `COMPLETED`, then runs the report and returns — see its own section below. A `DataSourceDownloadException`/`ReportProcessingException`/`PipelineException` from any composed step passes through unchanged; anything else becomes `PipelineException` |
| `DataSourceStatusChecker` | Package-private, two layers: `checkSingle()`/`checkPipeline()` — one non-blocking DB read, `STATUS_CHECK`'s implementation. `awaitSingle()`/`awaitPipeline()` — a sleep loop around the check above; what `Main`/`PipelineSequenceFactory` actually block in. See its own section below |
| `FailureNotifier` | Package-private: `Main`'s single failure-notification entry point — templates by exception type, logs always, emails only if `--opsFailureEmail` is set and an `EmailSendUtility` is available — see its own section below |

### Why blocking is a poll loop, not `waitUntilFinish()`

This framework's runner platform cannot reliably block on a submitted job's own `PipelineResult`
— calling `PipelineResult.waitUntilFinish()` on it specifically does not work here. The process
Airflow invokes, though, *can* be held open for as long as needed — so `DATA_SOURCE_DOWNLOAD` and
`PIPELINE` (the two process types that submit real Beam pipelines) submit, then block in their
own plain `Thread.sleep` poll loop (`DataSourceStatusChecker.awaitSingle()`/`awaitPipeline()`)
that re-reads `DaRefer` until the work reaches a terminal state, and only then return or throw.
One Airflow task, one JVM invocation, all the way through email — no external poller needed.
`PostDownloadFinalizeTransform` still writes `DaRefer`'s terminal status from inside the worker
exactly as before; the poll loop just notices it as soon as it happens, since it's actively
watching. `REPORT_PROCESSING` (DB-configured) was never affected either way — it never submits a
Beam pipeline at all.

`STATUS_CHECK` still exists as an optional, non-blocking, single-check diagnostic (an ops
dashboard, a manual look) — it is no longer part of the normal Airflow flow, since
`DATA_SOURCE_DOWNLOAD`/`PIPELINE` poll internally now.

---

## DataSourcePipelineFactory — DATA_SOURCE_DOWNLOAD

Sources are **never merged**. Each `SourceConfig` is an independent Beam DAG branch
that reads, transforms, validates, and writes to its own output table.

```
DataSourcePipelineFactory.assemble(options)
    │
    ├─ 1. BigQuerySourceConfigRepository.fetchSourceConfigs()  (throws if row missing)
    │       Each SourceConfig carries: queryConfig, sourceTransforms, validationConfig
    │
    ├─ 2. BigQueryDataSourceCheckpointAdapter.isCompleted()  skip COMPLETED sources
    │       (bypassed entirely when --manualOverrun=true or --overrideDownload=true)
    │
    ├─ 2b. Under --manualOverrun only: fetchLatestCompletedDaId() per source, BEFORE the new
    │        checkpoint is created — captured so PostDownloadFinalizeTransform can delete this
    │        superseded run's DaRec rows once the new run reaches COMPLETED
    │
    ├─ 3. BigQueryDataSourceCheckpointAdapter.createCheckpoint() → dataSourceId per source (LOADING row)
    │       Always a fresh INSERT — DaRefer only ever gains new rows, never overwritten
    │
    └─ 4. For each SourceConfig independently (no merge!):
            a. resolveQueryTokens()                   inject {periodStart}/{periodEnd} into BQ query
            b. fetchBqSchema()                        1. BqFetchConfig.schema (operator-declared bq_schema_json)
                                                          via BigQuerySchemaUtils.toBeamSchema() — no BQ call
                                                       2. else BigQuerySchemaUtils.fetchBeamSchema() (table metadata)
                                                       3. else null → BigQuerySourceTransform resolves column names
                                                          itself via a preview query (see beam-io/README.md)
            c. SourceRouter.routeFromConfig(schema)   read raw data (typed if schema non-null)
            d. SourceTransformChainAssembler.assemble()
                   ├─ LOOKUP: BQ side input → LookupEnrichTransform
                   ├─ GROUP_BY:  GroupByTransform
                   └─ SORT_BY:   SortByTransform (per-bundle, not global)
            e. DataSourceRecordSinkTransform(daId)    rows → streaming inserts → DaRec
                   → returns PCollection<Long> (count after all inserts commit)
            f. PostDownloadFinalizeTransform(daId)    [runs in Beam worker, not driver JVM]
                   ├─ BigQueryDataSourceRecordAdapter.countRecords(daId) → storedRowCount
                   ├─ row_count_mismatch check: storedRowCount == pipelineRowCount (always-on)
                   ├─ min/max row count bounds check (optional, from config)
                   ├─ data_transform_query (optional; only if the checks above passed):
                   │     a `WITH data AS (...)` UNNEST(DaRec) CTE is always prepended — never
                   │     opt-in, the operator's query just references `data` as a plain table —
                   │     then BigQueryJobService.runQueryToTable() runs it; validates output row
                   │     count; only then replaceStoredRows() runs
                   │     DELETE + INSERT (re-paginated at 250 rows/page) as ONE atomic BigQuery
                   │     multi-statement transaction (BEGIN TRANSACTION...COMMIT, ROLLBACK on
                   │     error) — retried as a whole with backoff (~30s) since this run's rows were
                   │     just streamed in and can still be in BigQuery's streaming buffer
                   │     (DML-ineligible despite being SELECT-visible); on any failure — query
                   │     error, bounds failure, or exhausted retries — the original rows are
                   │     completely untouched, never partially deleted or duplicated
                   ├─ BigQueryDataSourceRecordAdapter.sumField(daId, field) per BnC rule (optional;
                   │     runs against transformed rows if the transform above applied)
                   ├─ updateStatus(daId, COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED, bncJson)
                   ├─ manualOverrun cleanup (only on COMPLETED, only if this run superseded a
                   │     previous COMPLETED da_id): BigQueryDataSourceRecordAdapter.deleteRecords()
                   │     on the previous da_id's DaRec rows — DaRefer itself is untouched, it only
                   │     ever gains new rows
                   └─ SmtpReportEmailAdapter.send() if SourceFailureEmailConfig.isPresent()
```

The terminal checkpoint update and failure email are part of the pipeline itself, so they happen
regardless of the driver JVM's own behavior. What changed: `Main` no longer just calls
`pipeline.run()` and returns — it blocks afterward in `DataSourceStatusChecker.awaitSingle()`
(see below), re-reading `DaRefer` — the same row this section's `updateStatus()` step writes —
until it reaches a terminal state.

## DataSourceStatusChecker — STATUS_CHECK, and the poll loop behind DATA_SOURCE_DOWNLOAD/PIPELINE

Two layers in one class:

- **`checkSingle()`/`checkPipeline()`** — one non-blocking BigQuery read (or a handful, for a
  pipeline check), returns immediately. This pair is `--processType=STATUS_CHECK`'s
  implementation — an optional diagnostic, not required for normal operation.
- **`awaitSingle()`/`awaitPipeline()`** — a thin blocking wrapper: loop calling the check above,
  `Thread.sleep`ing `--jobPollIntervalSeconds` between attempts, until `Outcome.READY` or
  `--jobPollTimeoutMinutes` elapses. **This is what `Main.runDataSourceDownload()` and
  `PipelineSequenceFactory.execute()` actually call** — never `waitUntilFinish()`, just a plain
  sleep loop re-reading the DB.

```
DataSourceStatusChecker.checkPipeline(options)         [one pass — called in a loop by awaitPipeline()]
    1. BigQueryReportRepository.fetchReportConfig() → ReportConfig.datasources[]
    2. For each ref: BigQueryDataSourceCheckpointAdapter.getLatest(name, periodId)
         COMPLETED                       → satisfied, skip
         LOADING / no row, required=true  → Outcome.PENDING (keep polling)
         FAILED*, required=true            → collect into failedRequired
         anything, required=false           → log warning, never blocks
    3. failedRequired non-empty → throw PipelineException(ABORTED_REQUIRED_DATASOURCE)
       else anyRequiredPending  → Outcome.PENDING
       else                     → Outcome.READY

DataSourceStatusChecker.checkSingle(options)            [one pass — called in a loop by awaitSingle()]
    BigQueryDataSourceCheckpointAdapter.getLatest(datasourceName, periodId)
        absent / LOADING  → Outcome.PENDING
        COMPLETED         → Outcome.READY
        FAILED*           → throw DataSourceDownloadException(JOB_FAILURE, ...)

DataSourceStatusChecker.awaitPipeline(options) / awaitSingle(options):
    loop:
        outcome = checkPipeline(options) / checkSingle(options)
        outcome == READY            → return
        elapsed >= --jobPollTimeoutMinutes → throw *Exception(TIMEOUT)
        else                        → Thread.sleep(--jobPollIntervalSeconds), loop again
        (checkX throws)             → propagates straight out, unchanged
```

`getLatest()` reads the exact same `DaRefer` row `PostDownloadFinalizeTransform` writes from the
Beam worker — no new table, no new write path. `Main.runStatusCheck()` (the standalone diagnostic
mode) calls `checkSingle()`/`checkPipeline()` directly, once, and maps the `Outcome` to a process
exit code:

```
Main.runStatusCheck() outcome → exit code:
    Outcome.READY   → exit 0
    Outcome.PENDING → exit Main.STATUS_PENDING_EXIT_CODE (75) — not an error
    (thrown)        → propagates to Main.main()'s catch → FailureNotifier → non-zero JVM exit
```

## SourceTransformChainAssembler

| Transform | What it does | Beam mechanism |
|---|---|---|
| `LOOKUP` | Left-join rows with a lookup table from BQ | Side input (`PCollectionView<Map<String,String>>`) |
| `GROUP_BY` | Group by fields + aggregate (SUM, COUNT, AVG, MIN, MAX) | `GroupByKey` + `ParDo(AggregateDoFn)` |
| `SORT_BY` | Sort within each Beam bundle (per-bundle, not global) | Buffer + sort in `@FinishBundle` |

For global ordering, use an `ORDER BY` clause in the downstream BQ view instead of `SORT_BY`.

## PipelineFactory — REPORT_PROCESSING

```
PipelineFactory.assemble(options)
    ├─ 1. fetchBqSchema()               BQ table sources: BigQuerySchemaUtils.fetchBeamSchema()
    │       null for query-only or failed fetch → BigQuerySourceTransform resolves
    │       column names itself via a preview query (see beam-io/README.md)
    ├─ 2. SourceRouter.route(schema)    reads --sourceType; typed if schema non-null
    ├─ 3. TransformRegistry + chain loop
    ├─ 4. SinkRouter.route()
    └─ 5. Flatten DLQ → DeadLetterSinkTransform
```

No data moves during assembly — it only describes the computation graph.

---

## ReportPipelineFactory's report-completion email — EmailSendUtility

`ReportPipelineFactory`'s Phase 7 (email) no longer constructs `SmtpReportEmailAdapter` inline —
that call had a real, long-standing constructor-signature bug (`new SmtpReportEmailAdapter(options)`
never matched the class's actual 4-arg constructor), so report-completion email was never actually
reachable through that path. It's replaced with `EmailSendUtility` (`beam-io/io/email/`), a port
this repository defines but ships no implementation of — the real implementation is expected to be
an organization's own existing email-gateway client, kept outside this codebase.

```java
private static EmailSendUtility discoverEmailUtility() {
    Iterator<EmailSendUtility> found = ServiceLoader.load(EmailSendUtility.class).iterator();
    return found.hasNext() ? found.next() : null;
}
```

Two ways to supply a real implementation:
1. **SPI (preferred)** — a JAR on the classpath with
   `META-INF/services/com.yourco.beam.io.email.EmailSendUtility` naming the implementation class.
   `ReportPipelineFactory`'s no-arg and 2-arg constructors call `discoverEmailUtility()`
   automatically — same `ServiceLoader` mechanism `TransformRegistry` uses for `BeamTransform`,
   merged into the fat jar the same way (`maven-shade-plugin`'s `ServicesResourceTransformer`).
2. **Constructor injection** — `new ReportPipelineFactory(bqJobService, sinkRouter, emailUtility)`.

If neither yields an `EmailSendUtility`, `sendEmail()` logs a warning and returns without sending
— it does **not** fail the report. `PostDownloadFinalizeTransform`'s DATA_SOURCE_DOWNLOAD failure
email is unaffected — it still calls `SmtpReportEmailAdapter`'s real 4-arg constructor correctly,
with values straight from `SourceFailureEmailConfig`.

```java
List<EmailAttachment> attachments = exportedFiles.stream()
    .map(f -> new EmailAttachment(f.fileName(), emailUtility.FetchFileFromGcs(f.gcsUri()), f.contentType()))
    .toList();
EmailParams params = emailUtility.SetEmailParams(fromAddress, subject, toList, ccList, encrypted);
emailUtility.CreateEmailRequest(params, bodyHtml, attachments);
```

`fromAddress`/`encrypted` come from `ReportEmailConfig.fromAddress`/`.encrypted` — parsed from the
report config's `email.from_address` / `email.encrypted` keys (see `beam-io/README.md`).

---

## PipelineSequenceFactory — PIPELINE

Runs a complete `PIPELINE` sequence in one blocking call. Composes `DataSourcePipelineFactory`,
`DataSourceStatusChecker`, and `ReportPipelineFactory` — it does not reimplement any of them.

**There is no separate pipeline config.** `PIPELINE` takes the exact same `--reportName`/
`--reportSubprocess` as `REPORT_PROCESSING`, and the report's own `ReportConfig.datasources[]`
(with each entry's `is_required`) IS the pipeline — it already declares which datasources feed
the report and which are mandatory.

```
PipelineSequenceFactory.execute(options)
    ├─ 1. BigQueryReportRepository.fetchReportConfig(reportName, reportSubprocess, periodId)
    │       → ReportConfig.datasources[] (List<ReportDatasourceRef>)
    │
    ├─ 2. For every declared datasource: BigQuerySourceConfigRepository.fetchSourceConfigs()
    │       (one call per named datasource — each returns exactly one SourceConfig)
    │
    ├─ 3. DataSourcePipelineFactory.assembleForConfigs(options, allFetchedConfigs)
    │       ONE Dataflow job for every declared datasource — never one job per datasource.
    │       Internally skips any already COMPLETED, same as standalone
    │       DATA_SOURCE_DOWNLOAD (DaRefer skip-logic, unchanged).
    │       pipeline.run() — submitted.
    │
    ├─ 4. DataSourceStatusChecker.awaitPipeline(options)
    │       BLOCKS here — a Thread.sleep poll loop (--jobPollIntervalSeconds, default 30s)
    │       re-reading DaRefer, never waitUntilFinish() (blocking on the job's own PipelineResult
    │       specifically doesn't work on this platform, though the JVM process itself can) —
    │       until every required datasource reaches COMPLETED, or:
    │         a required datasource terminal-failed → throw PipelineException(ABORTED_REQUIRED_DATASOURCE)
    │         --jobPollTimeoutMinutes elapses (default 180) → throw PipelineException(TIMEOUT)
    │
    └─ 5. ReportPipelineFactory.execute(options)
            unchanged — report + its completion email, same call REPORT_PROCESSING makes standalone
```

**Why no separate required/optional flag anywhere else**: the terminal report already declares
which of its datasources are required, via the pre-existing `ReportDatasourceRef.required` —
enforced both by `ReportPipelineFactory.checkDatasourceAvailability()` (a belt-and-suspenders
re-check when the report actually runs, step 5) and by `DataSourceStatusChecker.checkPipeline()`
(the poll gate in step 4). A second, independently-set flag anywhere in a pipeline-specific config
could disagree with the first about the same datasource; instead there is exactly one place that
decision is declared.

**Why batch instead of one job per datasource**: sources are independent Beam branches (the
"never merged" rule still holds — no `Flatten.pCollections()` across sources), so submitting
every declared datasource in this run as one Dataflow job is just `DataSourcePipelineFactory`'s
existing multi-source behavior, reused rather than reinvented.

**`--manualOverrun` applies uniformly across the whole sequence** — no PIPELINE-specific flag or
logic needed, because `PipelineSequenceFactory` passes the exact same `options` instance straight
into both `DataSourcePipelineFactory.assembleForConfigs()` and `ReportPipelineFactory.execute()`:
- Every declared datasource bypasses its own `COMPLETED` skip-guard and re-downloads, superseding
  its previous run's `DaRec` rows once the new run completes — identical to standalone
  `DATA_SOURCE_DOWNLOAD` under `--manualOverrun`, since it's the same `filterByCheckpoint` check
  reading the same flag off the same options object.
- The terminal `REPORT` step needs no equivalent handling: `ReportPipelineFactory` has no
  `COMPLETED`-skip guard of its own to begin with — every invocation already inserts a fresh
  `RptRefer` row and re-runs, `--manualOverrun` or not.

---

## Failure handling — the exception hierarchy

Each process type's own factory classifies its failures into a typed, unchecked exception —
`DataSourceDownloadException`, `ReportProcessingException`, `PipelineException` (all in
`beam-core/exception/`, see `beam-core/README.md`) — before it ever reaches `Main`. `Main` never
has to inspect a message string to know what happened.

| Exception | Thrown from | `Reason` values |
|---|---|---|
| `DataSourceDownloadException` | `DataSourcePipelineFactory.assemble()`/`assembleForConfigs()` (config/assembly + submission failures); `DataSourceStatusChecker.checkSingle()`/`awaitSingle()` — the common case now, `Main.runDataSourceDownload()`'s in-process poll loop observing a terminal non-COMPLETED row in `DaRefer`, or its own poll timing out | `FILE_NOT_FOUND`, `INVALID_INPUT`, `CONNECTIVITY_FAILURE`, `JOB_FAILURE`, `TIMEOUT`, `UNKNOWN` |
| `ReportProcessingException` | `ReportPipelineFactory.execute()` | `CONFIG_NOT_FOUND`, `PREPROCESSING_FAILURE`, `DATASOURCE_UNAVAILABLE`, `STAGING_FAILURE`, `TRANSFORM_FAILURE`, `OUTPUT_FAILURE`, `EMAIL_FAILURE`, `UNKNOWN` |
| `PipelineException` | `PipelineSequenceFactory.execute()` (its own config lookup, or wrapping anything unrecognized) and, within that same call, `DataSourceStatusChecker.awaitPipeline()`'s poll loop (`ABORTED_REQUIRED_DATASOURCE`, `TIMEOUT`) | `CONFIGURATION_ERROR`, `CONFIG_NOT_FOUND`, `ABORTED_REQUIRED_DATASOURCE`, `DATASOURCE_PHASE_FAILURE`, `REPORT_PHASE_FAILURE`, `TIMEOUT`, `UNKNOWN` |

**Picking the `Reason`** — two different techniques, because the failure surfaces two different ways:
- **Driver-JVM phase tracking** (`ReportPipelineFactory`): a `currentReason` local is updated right
  before each phase runs; the catch block wraps with whatever it was last set to.
- **Checkpoint-status mapping** (`DataSourceDownloadException`/`PipelineException` from a poll
  loop): there is no cause-chain to walk here — this platform can't block on the submitted job's
  own `PipelineResult`, so nothing catches a Beam exception synchronously (the old
  `DataSourceFailureClassifier`, which did that by `instanceof`-checking a
  `FileSourceAdapter.FileSourceException`/`IllegalArgumentException` in the cause chain, has been
  removed — it was reachable only from a `waitUntilFinish()` catch block, and `waitUntilFinish()`
  is never called). `DataSourceStatusChecker` classifies from the observed `sta_cd` string in
  `DaRefer` instead, always as `JOB_FAILURE` (a status string alone can't distinguish *why* the
  worker failed the way an exception's cause chain could), or `TIMEOUT` if the poll loop itself
  exceeds `--jobPollTimeoutMinutes`.

**`PipelineSequenceFactory`'s pass-through rule**: a `DataSourceDownloadException` raised while
submitting, a `ReportProcessingException` from the report phase, or a `PipelineException` from
`awaitPipeline()`'s poll loop all propagate **unchanged** through `execute()`'s catch — each
already carries the right specific detail. Only PIPELINE's own config lookup or an unrecognized
exception type gets wrapped in `PipelineException` here.

**Catching, in `Main`** — a poll-loop failure hits the exact same catch as every other process
type, which is what makes it still reach the ops failure email even though it was discovered
asynchronously: it's thrown from inside the same `Main.runDataSourceDownload()`/
`runPipelineSequence()` call that submitted the job, just later, after the poll loop unblocks —
not from some separate invocation:
```java
try {
    switch (options.getProcessType()) {
        case DATA_SOURCE_DOWNLOAD -> runDataSourceDownload(options);
        case REPORT_PROCESSING    -> runReportProcessing(options);
        case PIPELINE             -> runPipelineSequence(options);
        case STATUS_CHECK         -> runStatusCheck(options);
    }
} catch (Exception e) {
    FailureNotifier.notify(options, e);
    throw e;   // rethrown unchanged — process still exits non-zero, same as before
}
```

`FailureNotifier.notify()` picks a subject/body template by matching `e`'s type
(`DataSourceDownloadException`/`ReportProcessingException`/`PipelineException`), falling back to a
**default** template — `[BEAM PIPELINE FRAMEWORK FAILED] <class name>` — for anything that isn't
one of the three (the legacy `PipelineFactory` REPORT_PROCESSING path never throws one of these,
and a failure before any factory even runs, e.g. CLI arg parsing, has nothing to match). The
notification is always logged; it's only emailed if `--opsFailureEmail` is set and an
`EmailSendUtility` is discoverable via SPI (same mechanism as `ReportPipelineFactory`'s
report-completion email) — see `beam-core/README.md`'s "Global failure notification" section for
the two flags. Every step inside `sendBestEffort()` is wrapped so a notification failure can never
mask the original exception `Main` is already in the middle of rethrowing.

---

## Building the fat JAR

```bash
# From project root — builds all modules and produces the deployable JAR
mvn package -pl beam-runner -am -DskipTests

# Output:
# beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar
```

The `maven-shade-plugin` in `beam-runner/pom.xml` does two critical things:
1. Bundles all dependencies into one JAR for Dataflow to execute
2. **`ServicesResourceTransformer`** merges all `META-INF/services/` files from all JARs
   so the SPI registry sees transforms from every module

---

## Running locally (DirectRunner)

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --sourceType=GCS \
  --gcsSourcePath=gs://my-bucket/input/*.json \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=GCS \
  --gcsSinkPath=gs://my-bucket/output/ \
  --deadLetterSink=gs://my-bucket/dlq/ \
  --piiFields=email,phone
```

---

## Running on Dataflow

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DataflowRunner \
  --project=my-gcp-project \
  --region=us-central1 \
  --tempLocation=gs://my-bucket/temp \
  --sourceType=BQ \
  --bqSourceTable=my-project:my-dataset.orders \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=BQ \
  --bqSinkTable=my-project:my-dataset.orders_clean \
  --writeDisposition=TRUNCATE \
  --retryPolicy=EXPONENTIAL \
  --maxRetries=3 \
  --deadLetterSink=gs://my-bucket/dlq/ \
  --runDate=2024-01-15 \
  --calendarName=NYSE \
  --businessEmail=reports@company.com \
  --devErrorEmail=oncall@company.com \
  --smtpPasswordSecretId=projects/my-project/secrets/smtp-pass/versions/latest
```

---

## Streaming mode (Pub/Sub)

```bash
java -jar beam-runner-bundled.jar \
  --runner=DataflowRunner \
  --sourceType=PUBSUB \
  --pubSubSubscription=projects/my-project/subscriptions/my-sub \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=PUBSUB \
  --pubSubTopic=projects/my-project/topics/clean-output \
  --deadLetterSink=gs://my-bucket/dlq/
```

For streaming jobs, `Main` does NOT call `waitUntilFinish()` — the job runs
indefinitely until cancelled in the Dataflow console or via:
```bash
gcloud dataflow jobs cancel JOB_ID --region=us-central1
```

---

## Adding beam-utils or other modules to the fat JAR

If you add a new module that `beam-runner` needs, add it to `beam-runner/pom.xml`:
```xml
<dependency>
    <groupId>com.yourco.beam</groupId>
    <artifactId>beam-utils</artifactId>
</dependency>
```

The shade plugin will include it automatically. No other changes needed.
