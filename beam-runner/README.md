# beam-runner

Entry point only. Wires all other modules together and produces the deployable fat JAR.
You should rarely need to edit this module.

---

## What lives here

| Class | Purpose |
|---|---|
| `Main` | Parses CLI args, routes by `--processType` (and `--reportName` for REPORT_PROCESSING), delegates to the right factory |
| `DataSourcePipelineFactory` | `DATA_SOURCE_DOWNLOAD`: validates params, fetches configs, creates LOADING checkpoints, assembles per-source Beam branches. `assemble(options)` (single `--datasourceName`) delegates to public `assembleForConfigs(options, List<SourceConfig>)`, which `PipelineSequenceFactory` also calls directly with several explicitly-fetched configs to batch them into one job |
| `PostDownloadFinalizeTransform` | Final pipeline step for each `DATA_SOURCE_DOWNLOAD` source: row/BnC validation, optional `data_transform_query` (replaces stored rows once validated, via an atomic DELETE+INSERT transaction), checkpoint update (COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED), `--manualOverrun` cleanup of the superseded previous run's DaRec rows, and failure email — all running in the Beam worker |
| `ReportPipelineFactory` | `REPORT_PROCESSING` (DB-configured): orchestrates BQ jobs + email in driver JVM; uses `ReportCheckpointAdapter` for RptRefer/RptDaMap/RptStageDa/RptOutput tracking; writes final result to per-report BQ table (`output_bq_table` from config) if set; no Beam pipeline submitted. Report-completion email uses `EmailSendUtility` (`beam-io`), discovered via `ServiceLoader` SPI or injected via constructor — see its own section below |
| `SmtpReportEmailAdapter` | SMTP implementation of `ReportEmailAdapter`; used only by `PostDownloadFinalizeTransform`'s DATA_SOURCE_DOWNLOAD failure email now |
| `PipelineFactory` | `REPORT_PROCESSING` (legacy): assembles generic source → transform chain → sink Beam pipeline |
| `PipelineSequenceFactory` | `PIPELINE`: same `--reportName`/`--reportSubprocess` as `REPORT_PROCESSING`, no separate config — runs whichever datasources the report's own `datasources[]` declares (batched into one Dataflow job via `DataSourcePipelineFactory.assembleForConfigs`), then the report (via the unchanged `ReportPipelineFactory`). Composes both existing factories — see its own section below |

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

The terminal checkpoint update and failure email are part of the pipeline itself. When the job reaches DONE state in Dataflow, the checkpoint has already been updated. No post-pipeline driver-JVM step is needed.

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

Composes `DataSourcePipelineFactory` and `ReportPipelineFactory` — it does not reimplement
either. Only decides which data sources to batch together and whether an incomplete one should
stop the sequence before the report runs.

**There is no separate pipeline config.** `PIPELINE` takes the exact same `--reportName`/
`--reportSubprocess` as `REPORT_PROCESSING`, and the report's own `ReportConfig.datasources[]`
(with each entry's `is_required`) IS the pipeline — it already declares which datasources feed
the report and which are mandatory, so nothing redeclares that as a second, separately-maintained
sequence. `PIPELINE` differs from plain `REPORT_PROCESSING` only in what happens when a declared
datasource isn't `COMPLETED` yet: `REPORT_PROCESSING` fails immediately
(`checkDatasourceAvailability()`); `PIPELINE` runs it first.

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
    │       pipeline.run().waitUntilFinish()
    │
    ├─ 4. Re-check each declared datasource's checkpoint status (isCompleted()).
    │       Still incomplete + is_required=true
    │           → abort: PipelineSequenceFactory.PipelineAbortedException
    │             (report never runs)
    │       Still incomplete + is_required=false
    │           → log and continue
    │
    └─ 5. ReportPipelineFactory.execute(options)   [unchanged — options.reportName/
            reportSubprocess were never touched, they're the operator's original values]
```

**Why no separate required/optional flag anywhere else**: the terminal report already declares
which of its datasources are required, via the pre-existing `ReportDatasourceRef.required` —
enforced on every report run (`ReportPipelineFactory.checkDatasourceAvailability()`), pipeline or
standalone. A second, independently-set flag anywhere in a pipeline-specific config could
disagree with the first about the same datasource; instead there is exactly one place that
decision is declared, and `PipelineSequenceFactory` reads it directly rather than duplicating it.

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
