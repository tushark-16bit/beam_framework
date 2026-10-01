# beam-runner

Entry point only. Wires all other modules together and produces the deployable fat JAR.
You should rarely need to edit this module.

---

## What lives here

| Class | Purpose |
|---|---|
| `Main` | Parses CLI args, routes by `--processType` (and `--reportName` for REPORT_PROCESSING), delegates to the right factory. Wraps the whole dispatch in one catch that calls `FailureNotifier.notify()` then rethrows — see its own section below. `runDataSourceDownload()` and `runPipelineSequence()` submit (`pipeline.run()`) and RETURN IMMEDIATELY — no poll loop, no `waitUntilFinish()`. `runStatusCheck()` handles `STATUS_CHECK`, an optional non-blocking diagnostic |
| `DataSourcePipelineFactory` | `DATA_SOURCE_DOWNLOAD`: validates params, fetches configs, creates LOADING checkpoints, assembles per-source Beam branches. `assemble(options)` (single `--datasourceName`) delegates to public `assembleForConfigs(options, List<SourceConfig>)`, which `PipelineSequenceFactory` also calls directly with several explicitly-fetched configs to batch them into one job. `assembleForConfigs()` returns a `DataSourceAssembly` (pipeline + one finalize-signal `PCollection<Long>` per source branch), not a bare `Pipeline`, so a report step can be wired onto the same pipeline before submission. Both classify their own (config/assembly-time) failures into `DataSourceDownloadException`; only builds+returns the assembly, never calls `run()` itself. Finance Automation scheduling: each source is evaluated with `RunDateCalculator.evaluateDataSource()`; only ELIGIBLE sources get a DaRefer row and a branch — others are logged and skipped (NOT_EVALUABLE also goes through `FailureNotifier`). `assemble()` now returns the `DataSourceAssembly` so `Main` can skip submitting an empty job. `--periodId` is no longer mandatory (calculated per scheduled source); `--manualOverrun` bypasses only the COMPLETED check, never eligibility or dates. A source with no run schedule or calendar is not processed (NOT_EVALUABLE + failure notification) — no fallback to CLI dates. |
| `DataSourceAssembly` | Package-private holder: `{pipeline, finalizeSignals}` — lets the pipeline and its per-source finalize signals travel together from `DataSourcePipelineFactory` to `PipelineSequenceFactory` `isEmpty()` — no source branch; callers don't submit an empty job. |
| `PostDownloadFinalizeTransform` | Final pipeline step for each `DATA_SOURCE_DOWNLOAD` source: row/BnC validation, optional `data_transform_query` (replaces stored rows once validated, via an atomic DELETE+INSERT transaction), checkpoint update (COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED), `--manualOverrun` cleanup of the superseded previous run's DaRec rows, and failure email — all running in the Beam worker, entirely independent of the driver JVM (which has already returned by the time this runs). Its output is a `PCollection<Long>` signal element — this source's `da_id`, emitted in a `finally` block whether validation succeeded or failed — used by a downstream `Wait.on()` when this branch is part of a batched `PIPELINE` run |
| `ReportPipelineFactory` | `REPORT_PROCESSING` (DB-configured): orchestrates BQ jobs + email in driver JVM; uses `ReportCheckpointAdapter` for RptRefer/RptDaMap/RptStageDa/RptOutput tracking; writes final result to per-report BQ table (`output_bq_table` from config) if set; no Beam pipeline submitted, so no waiting to do. `execute(options)` fetches `ReportConfig` then delegates to `execute(options, config, dates)` — the overload `ReportFinalizeTransform` calls from a worker with a pre-fetched config, skipping the `BigQueryReportRepository` call a DoFn may never make. Report-completion email uses `EmailSendUtility` (`beam-io`), discovered via `ServiceLoader` SPI or injected via constructor — see its own section below. Classifies its own failures into `ReportProcessingException`, one `Reason` per phase. Every date in the run — RptRefer/DaRefer `per_id`, query tokens, GCS output file names via `ReportOutputSinkRouter`, email tokens — comes from those `RunDates` Standalone runs evaluate the report's own schedule (`RunDateCalculator.evaluateReport()`): not eligible → return with no RptRefer row; not evaluable → `ReportProcessingException`; scheduled + already COMPLETED for the period → skip unless `--manualOverrun` (`isAlreadyCompleted()`, shared with `PipelineSequenceFactory`). A report with no run schedule or calendar is not processed (`ReportProcessingException`). |
| `SmtpReportEmailAdapter` | SMTP implementation of `ReportEmailAdapter`; used only by `PostDownloadFinalizeTransform`'s DATA_SOURCE_DOWNLOAD failure email now |
| `PipelineFactory` | `REPORT_PROCESSING` (legacy): assembles generic source → transform chain → sink Beam pipeline. Its non-streaming path still calls `waitUntilFinish()` directly — a known gap, left as-is since it has no checkpoint table to gate on and isn't launched via the Flex Template path the rest of this section describes |
| `PipelineSequenceFactory` | `PIPELINE`: same `--reportName`/`--reportSubprocess` as `REPORT_PROCESSING`, no separate config — assembles one batched Dataflow job for whichever not-yet-`COMPLETED` datasources the report's own `datasources[]` declares, wires the report step onto that SAME pipeline via `ReportFinalizeTransform.wire()` (gated on `Wait.on()`), submits ONCE, and returns immediately — see its own section below. A `DataSourceDownloadException` from assembly/submission passes through unchanged; anything else becomes `PipelineException`. Resolves the report's `RunDates` in the driver JVM at submission and passes them to `wire()`; warns when a datasource's own resolved `periodId` differs from the report's `decideReportRun()` applies the report's own schedule + COMPLETED check at submission; a skipped report doesn't stop its data sources (BAU: scheduled independently); nothing eligible → no job submitted. `--periodId` is no longer mandatory (each item's period id is calculated from the Business Date). |
| `ReportFinalizeTransform` | Worker-side step wiring PIPELINE's report onto the batched pipeline: `Create.of(1) → Wait.on(finalizeSignals) → ParDo(ReportRunDoFn)`. `ReportRunDoFn` reconstructs `FrameworkOptions` via Beam's `PipelineOptions` DoFn-parameter injection, one-shot-verifies every required datasource actually reached `COMPLETED`, calls `ReportPipelineFactory.execute(options, config, dates)`, and calls `FailureNotifier` itself (without rethrowing) on any exception — see its own section below. `ReportRunDoFn` carries the report's `RunDates` (resolved at submission) as a field and uses `dates.periodId` for the required-datasource check |
| `DataSourceStatusChecker` | Package-private, non-blocking only: `checkSingle()`/`checkPipeline()` — one DB read (or a handful), `STATUS_CHECK`'s entire implementation. No blocking wrappers anymore — `DATA_SOURCE_DOWNLOAD`/`PIPELINE` no longer poll from the driver JVM at all |
| `FailureNotifier` | Package-private: the single failure-notification entry point — called from `Main`'s driver-JVM catch block AND from `ReportFinalizeTransform`'s worker DoFn. Templates by exception type, logs always, emails only if `--opsFailureEmail` is set and an `EmailSendUtility` is available — see its own section below |

### Why nothing blocks in `main()` — the Flex Template launch contract

This deployment launches via a Dataflow Flex Template, whose launch contract requires the launcher
process (`main()`) to build the pipeline, call `pipeline.run()`, and exit promptly — the launch
operation is considered complete once the launcher exits, not once the submitted job itself
finishes. A driver-JVM poll loop (the previous design here) violates that contract exactly as much
as `PipelineResult.waitUntilFinish()` would, and was traced to a real incident: Airflow observed
the launch never completing and timed out at the graph level, independent of whether the Dataflow
job itself succeeded or had already finished.

So `DATA_SOURCE_DOWNLOAD` and `PIPELINE` (the two process types that submit real Beam pipelines)
now submit and return immediately. Everything that used to block the driver JVM — waiting for a
datasource, running the report, sending completion/failure email — happens worker-side instead,
gated by `Wait.on()` (a Beam data-dependency barrier, not a sleep loop): `PostDownloadFinalizeTransform`
for datasource finalization, `ReportFinalizeTransform` for `PIPELINE`'s report step. One Airflow
task, one JVM invocation to submit — everything through to email now runs inside the pipeline
itself. `REPORT_PROCESSING` (DB-configured) was never affected either way — it never submits a
Beam pipeline at all.

`STATUS_CHECK` still exists as an optional, non-blocking, single-check diagnostic (an ops
dashboard, a manual look) — it was never part of the blocking design and remains a plain one-shot
DB read; neither `DATA_SOURCE_DOWNLOAD` nor `PIPELINE` need it internally anymore.

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
    ├─ 1b. RunDateCalculator.evaluateDataSource()  per source: eligible today? which period? (the period id
    │        is calculated — --periodId not required for a scheduled source; --manualOverrun has no effect here)
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
            f. PostDownloadFinalizeTransform(daId)    [runs in Beam worker, not driver JVM;
                                                          returns PCollection<Long>, not PDone —
                                                          emits daId as a signal element, success
                                                          or failure, for a downstream Wait.on()]
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
regardless of the driver JVM's own behavior — and the driver JVM has, in fact, already returned
by the time this runs: `Main` calls `pipeline.run()` and returns immediately, submitting nothing
else and waiting for nothing.

## DataSourceStatusChecker — the STATUS_CHECK diagnostic (non-blocking only)

One class, one job: `checkSingle()`/`checkPipeline()` — a non-blocking BigQuery read (or a
handful, for a pipeline check), returns immediately. This is `--processType=STATUS_CHECK`'s
entire implementation — an optional diagnostic, never required for normal operation, since
neither `DATA_SOURCE_DOWNLOAD` nor `PIPELINE` polls internally anymore (see
`ReportFinalizeTransform` below for how `PIPELINE` waits instead — `Wait.on()`, worker-side, no
sleep loop).

```
DataSourceStatusChecker.checkPipeline(options)
    1. BigQueryReportRepository.fetchReportConfig() → ReportConfig.datasources[]
    2. For each ref: BigQueryDataSourceCheckpointAdapter.getLatest(name, periodId)
         COMPLETED                       → satisfied, skip
         LOADING / no row, required=true  → Outcome.PENDING
         FAILED*, required=true            → collect into failedRequired
         anything, required=false           → log warning, never blocks
    3. failedRequired non-empty → throw PipelineException(ABORTED_REQUIRED_DATASOURCE)
       else anyRequiredPending  → Outcome.PENDING
       else                     → Outcome.READY

DataSourceStatusChecker.checkSingle(options)
    BigQueryDataSourceCheckpointAdapter.getLatest(datasourceName, periodId)
        absent / LOADING  → Outcome.PENDING
        COMPLETED         → Outcome.READY
        FAILED*           → throw DataSourceDownloadException(JOB_FAILURE, ...)
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

Assembles a complete `PIPELINE` sequence — datasources + report step — as ONE batched Dataflow
job, submits it once, and returns immediately. Composes `DataSourcePipelineFactory` and
`ReportPipelineFactory` — it does not reimplement either.

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
    │       Returns DataSourceAssembly { pipeline, finalizeSignals }.
    │
    ├─ 4. ReportFinalizeTransform.wire(assembly.pipeline, assembly.finalizeSignals, reportConfig, reportDates, options)
    │       Adds Create.of(1) → Wait.on(finalizeSignals) → ParDo(ReportRunDoFn) to the SAME
    │       pipeline — no separate job, no second pipeline.run().
    │
    └─ 5. assembly.pipeline.run()
            ONE submit — datasources + report step together. execute() returns immediately.
```

Everything downstream of submission now runs worker-side, inside `ReportRunDoFn`, only once
`Wait.on()` confirms every batched datasource branch reached a terminal state:

```
ReportRunDoFn.processElement(trigger, PipelineOptions)
    FrameworkOptions options = pipelineOptions.as(FrameworkOptions.class)
    ├─ verifyRequiredDatasources(options)
    │     for each required ReportDatasourceRef: BigQueryDataSourceCheckpointAdapter.isCompleted()
    │     any not COMPLETED → throw PipelineException(ABORTED_REQUIRED_DATASOURCE)
    │     (one-shot — no retry, no timeout; Wait.on() already guaranteed a terminal state)
    └─ ReportPipelineFactory.execute(options, config, dates)
          unchanged — report + its completion email, same logic REPORT_PROCESSING runs standalone
    (any exception from either step) → FailureNotifier.notify(options, e) — caught here, NOT
          rethrown; this is the pipeline's last step, so rethrowing would only trigger a pointless
          Beam bundle retry
```

**Why no separate required/optional flag anywhere else**: the terminal report already declares
which of its datasources are required, via the pre-existing `ReportDatasourceRef.required` —
enforced both by `ReportPipelineFactory.checkDatasourceAvailability()` (a belt-and-suspenders
re-check when the report actually runs) and by `ReportRunDoFn.verifyRequiredDatasources()` (the
one-shot check above). A second, independently-set flag anywhere in a pipeline-specific config
could disagree with the first about the same datasource; instead there is exactly one place that
decision is declared.

**Why batch instead of one job per datasource**: sources are independent Beam branches (the
"never merged" rule still holds — no `Flatten.pCollections()` across sources), so submitting
every declared datasource plus the report step in this run as one Dataflow job is just
`DataSourcePipelineFactory`'s existing multi-source behavior, extended with one more
`Wait.on()`-gated step rather than reinvented.

**`--manualOverrun` applies uniformly across the whole sequence** — no PIPELINE-specific flag or
logic needed, because `PipelineSequenceFactory` passes the exact same `options` instance into
`DataSourcePipelineFactory.assembleForConfigs()`, and the worker reconstructs an equivalent view
of it via `PipelineOptions` injection before calling `ReportPipelineFactory.execute()`:
- Every declared datasource bypasses its own `COMPLETED` skip-guard and re-downloads, superseding
  its previous run's `DaRec` rows once the new run completes — identical to standalone
  `DATA_SOURCE_DOWNLOAD` under `--manualOverrun`, since it's the same `filterByCheckpoint` check
  reading the same flag off the same options object.
- The terminal `REPORT` step: a report that has a run schedule and is already `COMPLETED` in
  `RptRefer` for its calculated period is skipped (`ReportPipelineFactory.isAlreadyCompleted()`)
  unless `--manualOverrun` (every report must have a schedule and calendar — one without is not
  processed).
- **`--manualOverrun` only bypasses the `COMPLETED` check / overwrites storage — it never changes
  whether an item is eligible or which dates are calculated** (`DATE_SCHEDULING_RULES.md`, D1). To
  force a re-run, also pass the `--runDate` that is eligible for the item.

---

## Failure handling — the exception hierarchy

Each process type's own factory classifies its failures into a typed, unchecked exception —
`DataSourceDownloadException`, `ReportProcessingException`, `PipelineException` (all in
`beam-core/exception/`, see `beam-core/README.md`) — before it ever reaches `FailureNotifier`.
Neither `FailureNotifier` nor its caller ever has to inspect a message string to know what
happened.

| Exception | Thrown from | `Reason` values |
|---|---|---|
| `DataSourceDownloadException` | `DataSourcePipelineFactory.assemble()`/`assembleForConfigs()` (config/assembly + submission failures, synchronous in the driver JVM); `DataSourceStatusChecker.checkSingle()` — the optional `STATUS_CHECK` diagnostic only | `FILE_NOT_FOUND`, `INVALID_INPUT`, `CONNECTIVITY_FAILURE`, `JOB_FAILURE`, `UNKNOWN` |
| `ReportProcessingException` | `ReportPipelineFactory.execute()`/`execute(options, config, dates)` | `CONFIG_NOT_FOUND`, `PREPROCESSING_FAILURE`, `DATASOURCE_UNAVAILABLE`, `STAGING_FAILURE`, `TRANSFORM_FAILURE`, `OUTPUT_FAILURE`, `EMAIL_FAILURE`, `UNKNOWN` |
| `PipelineException` | `PipelineSequenceFactory.execute()` (its own config lookup, or wrapping anything unrecognized during assembly/submission — synchronous, driver JVM) and, worker-side, `ReportFinalizeTransform`'s `ReportRunDoFn.verifyRequiredDatasources()` (`ABORTED_REQUIRED_DATASOURCE`) | `CONFIGURATION_ERROR`, `CONFIG_NOT_FOUND`, `ABORTED_REQUIRED_DATASOURCE`, `DATASOURCE_PHASE_FAILURE`, `REPORT_PHASE_FAILURE`, `UNKNOWN` |

**Picking the `Reason`**:
- **Driver-JVM phase tracking** (`ReportPipelineFactory`): a `currentReason` local is updated right
  before each phase runs; the catch block wraps with whatever it was last set to.
- **One-shot worker-side check** (`PipelineException(ABORTED_REQUIRED_DATASOURCE)`): thrown
  directly by `ReportRunDoFn.verifyRequiredDatasources()` once `Wait.on()` has already guaranteed
  every batched datasource branch reached a terminal state — no retry, no timeout, no cause-chain
  to walk.
- There is no `TIMEOUT` reason anymore on either `DataSourceDownloadException` or
  `PipelineException` — `Wait.on()` is a direct Beam data-dependency signal, not a poll loop with
  a deadline to exceed.

**`PipelineSequenceFactory`'s pass-through rule**: a `DataSourceDownloadException` raised while
assembling/submitting propagates **unchanged** through `execute()`'s catch — it already carries
the right specific detail. Only PIPELINE's own config lookup or an unrecognized exception type
gets wrapped in `PipelineException` here. A `ReportProcessingException` from the report itself, or
a `PipelineException(ABORTED_REQUIRED_DATASOURCE)` from the worker check, never reaches this
method at all — `execute()` has already returned by then (see `ReportFinalizeTransform` above).

**Catching** — two separate call sites now converge on the same `FailureNotifier.notify()`:
`Main.main()`'s catch block (synchronous, driver-JVM failures only) and `ReportFinalizeTransform`'s
worker DoFn (a failure discovered only after `main()` has already returned):
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
