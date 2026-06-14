# Agent Guide — beam-pipeline-framework

This file is the primary reference for any AI agent working in this repository.
Read it fully before making any changes. Written for any capable language model (Claude, GPT, Gemini, etc.).

For human-readable documentation, see [`README.md`](README.md), [`WALKTHROUGH.md`](WALKTHROUGH.md),
and the per-module `README.md` files.

---

## 1. What this project is

A configurable Apache Beam ETL pipeline framework in Java 17, running on GCP Dataflow and
triggered by Apache Airflow. It supports two process types:

- **DATA_SOURCE_DOWNLOAD** — fetches raw data from external sources (API, file, BigQuery),
  applies per-source transforms (lookup, group-by, sort), validates output, and writes to
  per-source BQ tables. Entirely configured from a JDBC parameter DB — no code changes for
  new sources.
- **REPORT_PROCESSING** — reads downloaded data, applies a chained BigQuery transformation
  sequence, exports results to GCS files, and sends email with attachments. When `--reportName`
  is set, runs entirely in the driver JVM (no Dataflow job). Falls back to a generic
  source → transform chain → sink Beam pipeline when `--reportName` is blank.

---

## 2. Mandatory directive — README updates

> **Every code change MUST be reflected in the README files. This is non-negotiable.**

| Type of change | READMEs to update |
|---|---|
| New transform | `beam-transforms/README.md` + root `README.md` |
| New source or sink connector | `beam-io/README.md` |
| New utility class | `beam-utils/README.md` |
| New pipeline option / CLI flag | `beam-core/README.md` + root `README.md` |
| Change to pipeline assembly logic | `beam-runner/README.md` |
| New module | Root `README.md` (module structure + dependency diagram) |
| Any architectural change | Root `README.md` + `WALKTHROUGH.md` (update the relevant diagram) |
| New DB table or column | `beam-utils/README.md` (DDL section) + root `README.md` |
| New model class | `beam-core/README.md` (model table) + this file (section 4 file map) |

If you add a class → describe it.
If you add a flag → add it to the config table.
If you change behavior → update the section that describes it.
If a README becomes wrong → fix it. Do not leave it stale.

---

## 3. File reading order — fastest path to understanding

Read in this order for a complete mental model:

```
1.  WALKTHROUGH.md                                    — UML diagrams + execution flows (read this first)
2.  beam-core/.../options/FrameworkOptions.java       — all CLI flags; the config contract
3.  beam-runner/.../runner/Main.java                  — entry point; how process type routes
4.  beam-runner/.../runner/DataSourcePipelineFactory  — DATA_SOURCE_DOWNLOAD orchestration
5.  beam-runner/.../runner/ReportPipelineFactory      — REPORT_PROCESSING orchestration
6.  beam-core/.../transform/BeamTransform.java        — SPI interface; the extension contract
7.  beam-runner/.../runner/PipelineFactory.java       — legacy REPORT_PROCESSING (transform chain)
8.  beam-utils/.../db/ParameterRepository.java        — how source config is loaded from DB
9.  beam-utils/.../db/ReportRepository.java           — how report config is loaded from DB
10. beam-io/.../io/source/SourceRouter.java            — source type → connector mapping
11. beam-runner/.../runner/SourceTransformChainAssembler — LOOKUP/GROUP_BY/SORT_BY assembly
12. beam-io/.../io/report/BigQueryJobService.java      — how BQ jobs run for reports
13. beam-io/.../io/status/ProcessStatusAdapter.java    — status tracking interface
```

---

## 4. Complete file map

Every source file, one line each.

### beam-core — contracts only, no GCP code

```
options/FrameworkOptions.java         All CLI flags. Every pipeline option. Read this first.
options/ProcessType.java              Enum: DATA_SOURCE_DOWNLOAD | REPORT_PROCESSING
options/SourceType.java               Enum: GCS | BQ | PUBSUB | API | FILE
options/SinkType.java                 Enum: GCS | BQ | PUBSUB
options/RetryPolicyType.java          Enum: NONE | FIXED | EXPONENTIAL
options/WriteDispositionType.java     Enum: APPEND | TRUNCATE

transform/BeamTransform.java          SPI interface. name() + toComposite(). SUCCESS_TAG + DEAD_LETTER_TAG.
transform/TransformRegistry.java      ServiceLoader discovery. resolve(chainSpec) → List<BeamTransform>.

retry/RetryPolicy.java                Interface: shouldRetry(attempt, cause), delayMs(attempt).
retry/ExponentialRetryPolicy.java     Exponential back-off, ThreadLocalRandom jitter, 200ms cap.
retry/FixedRetryPolicy.java           Fixed delay, 200ms cap.
retry/RetryingDoFn.java               Generic retry + DLQ routing via TupleTag.

model/FailedRecord.java               DLQ envelope. @DefaultCoder(SerializableCoder.class).
model/Schemas.java                    RAW_JSON schema constant.
model/CheckpointRecord.java           Checkpoint row: job_run_id, datasource, state, etc.
model/CheckpointState.java            Enum: STARTED_ACCESSING | FINISHED_ACCESSING | FAILED_DOWNLOADING

-- DATA_SOURCE_DOWNLOAD models --
model/SourceConfig.java               Per-source config with Builder. Carries ALL per-source config.
model/ApiSourceConfig.java            REST API config: endpoint, auth, pagination.
model/FileSourceConfig.java           File config: CSV/Excel, GCS location, delimiter, header.
model/BqFetchConfig.java              BQ source: project, dataset, table, query, queryParams map.
model/OutputConfig.java               Per-source output: BQ or GCS, write mode.
model/QueryConfig.java                Query template + paramMappings for token injection.
model/SourceTransformConfig.java      One transform step: GROUP_BY | SORT_BY | LOOKUP.
model/AggregationConfig.java          SUM/COUNT/AVG/MIN/MAX per field (used by GROUP_BY).
model/LookupConfig.java               Lookup table config: BQ or JDBC source, key fields.
model/ValidationConfig.java           Post-fetch validation: header check, row count, BnC rules.
model/BncRule.java                    One Balance-and-Control check: SUM(field) within tolerance %.
model/ProcessStatusRecord.java        Status row with Builder. pending()/pendingReport()/completed()/failed().

-- REPORT_PROCESSING models --
model/ReportConfig.java               Full report config assembled from 6 DB tables.
model/ReportDatasourceRef.java        Required DS for a report + transform alias.
model/ReportPreprocessingStep.java    Pre-run step: BQ_QUERY or API_ENRICHMENT.
model/ReportTransformStep.java        One BQ query in the chain: inputAlias → outputAlias.
model/ReportOutputConfig.java         File output: CSV/JSON, GCS path, prefix, suffix.
model/ReportEmailConfig.java          Email: to, cc, subject/body templates with tokens.
```

### beam-io — connectors and I/O adapters

```
source/SourceRouter.java              Stateless factory: routeByOptions() + routeFromConfig().
source/BigQuerySourceTransform.java   BigQueryIO.read() with typed schema.
source/GcsSourceTransform.java        GCS glob → newline-delimited JSON rows.
source/PubSubSourceTransform.java     Pub/Sub subscription → streaming rows.
source/ApiSourceAdapter.java          Pure HTTP adapter: auth, PAGE_NUMBER/CURSOR/OFFSET pagination.
source/ApiSourceTransform.java        Beam wrapper for ApiSourceAdapter. @Setup/@Teardown for HttpClient.
source/FileSourceAdapter.java         CSV (Commons CSV) + Excel (Apache POI) from GCS bytes.
source/FileSourceTransform.java       Beam wrapper for FileSourceAdapter.

sink/SinkRouter.java                  Stateless factory: route(data, options).
sink/BigQuerySinkTransform.java       Writes PCollection<Row> to BQ. Returns WriteResult.
sink/GcsSinkTransform.java            Writes PCollection<Row> as newline-delimited JSON.
sink/PubSubSinkTransform.java         Publishes each Row as JSON to Pub/Sub.
sink/DeadLetterSinkTransform.java     Writes FailedRecord objects to GCS DLQ path.

checkpoint/CheckpointAdapter.java         Interface: write(record), getLatest(jobRunId, source, period).
checkpoint/BigQueryCheckpointAdapter.java BQ streaming insert + interactive query.

status/ProcessStatusAdapter.java          Interface: write(record), getLatest(), queryRowCount(), querySum().
status/BigQueryProcessStatusAdapter.java  BQ-backed: streaming inserts + interactive queries for BnC.

email/EmailAttachment.java            Attachment model: InputStream + fileName + contentType.
email/ReportEmailAdapter.java         Interface: send(subject, body, to, cc, List<EmailAttachment>).

report/BigQueryJobService.java        Driver-JVM BQ jobs: runQueryToTable(), exportToCsv(), exportToJson().

util/JsonUtils.java                   Row → JSON with correct type handling.
```

### beam-utils — stateless helpers, no pipeline graph code

```
BigQuerySchemaUtils.java    fetchBeamSchema(), tableExists(), fetchRowCount(). Call in driver JVM only.
GcsUtils.java               pathHasFiles(), listFiles(), writeTextFile(), readTextFile(), readBytes(), deletePrefix().
SecretManagerUtils.java     fetchSecret(secretId). Never log result. Never store in options value.
RowValidationUtils.java     requireFields(), matchesPattern(), inRange(), oneOf(). Thread-safe.
MetricsUtils.java           transformCounter(), pipelineDlqTotal(). Consistent naming for Dataflow UI.
CalendarUtils.java          STUBS — isBusinessDay(), nextBusinessDay(), applyOffset(). Must be implemented.
DateUtils.java              resolveRunDate(), partitionedPath(), shardedTable(), toDisplayString().
QueryParameterResolver.java resolve(template, paramMappings, options). Two-pass: standard then custom tokens.

db/DatabaseAdapter.java         Interface: query(), queryOne(), update(), close().
db/JdbcDatabaseAdapter.java     JDBC + HikariCP. One pool per instance. Always try-with-resources.
db/DatabaseAdapterFactory.java  Static factory: reads --paramDb* options, fetches password from Secret Manager.
db/ParameterRepository.java     Source config queries: validate params, fetchSourceConfigs() with all columns.
db/ReportRepository.java        Report config queries: fetchReportConfig(), fetchDatasourceOutputTable().
db/DatabaseException.java       Unchecked wrapper for SQLException.
```

### beam-transforms — pluggable transform implementations

```
FilterNullsTransform.java           Token: filter-nulls. Drops null rows → DLQ. Counter metric.
MaskPiiTransform.java               Token: mask-pii. SHA-256 hashes --piiFields list.
EnrichFromExternalApiTransform.java Token: enrich-from-api. SAMPLE — shows @Setup/@Teardown pattern.

source/GroupByTransform.java        MapElements → GroupByKey → AggregateDoFn. SUM/COUNT/AVG/MIN/MAX.
source/SortByTransform.java         Per-bundle sort (@StartBundle/@FinishBundle). NOT global. Logs warning.
source/LookupEnrichTransform.java   Left-join via PCollectionView<Map<String,String>> (key → JSON blob).

side/SideEffectEmailTransform.java  Sends SMTP email per Row. No attachments. Best-effort (logs on fail).
side/SideEffectDbWriteTransform.java Inserts each Row into a JDBC table.

META-INF/services/...BeamTransform  SPI manifest. One class name per line.
```

### beam-runner — entry point and orchestrators

```
Main.java                       Parses CLI → routes by processType + reportName.
PipelineFactory.java            Legacy REPORT_PROCESSING: source → transform chain → sink.
DataSourcePipelineFactory.java  DATA_SOURCE_DOWNLOAD: per-source branches, post-pipeline validation.
ReportPipelineFactory.java      REPORT_PROCESSING (DB-configured): driver-JVM BQ jobs + email.
SourceTransformChainAssembler.java Assembles LOOKUP/GROUP_BY/SORT_BY per source; loads lookup views.
SmtpReportEmailAdapter.java     SMTP impl of ReportEmailAdapter. MimeMultipart for attachments.
```

---

## 5. Architecture rules — non-negotiable

### Module dependency direction

```
beam-runner → beam-core, beam-io, beam-utils, beam-transforms
beam-transforms → beam-core, beam-utils
beam-io → beam-core   (NOT beam-utils, NOT beam-transforms)
beam-utils → beam-core
beam-core → (nothing internal)
```

**Violations**: if `beam-io` imports from `beam-utils`, it breaks this rule. The compiler will
not catch it — but it creates a circular risk and violates the isolation contract.
`SmtpReportEmailAdapter` is in `beam-runner` (not `beam-io`) precisely because it needs
`SecretManagerUtils` from `beam-utils` and `angus-mail` from `beam-transforms`.

### Wire type

All Beam transforms communicate via `PCollection<Row>` with a declared `Schema`.
Call `.setRowSchema()` on every output. Do not use raw bytes, Strings, or Avro.

### Output contract (BeamTransform SPI)

Every `BeamTransform.toComposite()` returns `PTransform<PCollection<Row>, PCollectionTuple>`.
The tuple MUST include both:
- `BeamTransform.SUCCESS_TAG` — `TupleTag<Row>`
- `BeamTransform.DEAD_LETTER_TAG` — `TupleTag<FailedRecord>`

### Per-source independence (DATA_SOURCE_DOWNLOAD)

Sources are **never merged**. Each `SourceConfig` is an independent Beam DAG branch.
`Flatten.pCollections()` across different sources is **forbidden**.

---

## 6. Serialization rules

| Rule | Correct | Wrong |
|---|---|---|
| DoFn class type | Named `static final` inner class | Anonymous class or lambda |
| DoFn field types | All `Serializable` (String, int, List, Map) | Non-serializable (HttpClient, Connection) |
| Non-serializable resources | `transient` field, create in `@Setup`, close in `@Teardown` | Non-transient field |
| TupleTag instances | `static final` on the DoFn class | Created inside `@ProcessElement` |
| Function interfaces | `SerializableFunction<A,B>` (Beam) | `java.util.function.Function<A,B>` |
| Models in `PCollection` | `@DefaultCoder(SerializableCoder.class)` on the class | No coder annotation |

---

## 7. How to make each type of change

### Add a new data source type (DATA_SOURCE_DOWNLOAD)

1. Insert a row in `source_config` with `source_type = MY_TYPE`
2. Add `MY_TYPE` to `SourceType` enum in `beam-core`
3. Create `MySourceAdapter` (pure Java, no Beam) in `beam-io/source/`
4. Create `MySourceTransform` (thin Beam wrapper) in `beam-io/source/`
5. Add a case to `SourceRouter.routeFromConfig()` switch
6. Add required config fields to `SourceConfig` model and `ParameterRepository`
7. Update `beam-io/README.md` and root `README.md`

### Add a new per-source transform type

1. Create the transform class in `beam-transforms/source/` extending `PTransform<PCollection<Row>, PCollection<Row>>`
2. Add the type constant to `SourceTransformConfig` (e.g., `public static final String MY_TYPE = "MY_TYPE"`)
3. Add a case to `SourceTransformChainAssembler.assemble()` switch
4. Add config fields to `SourceTransformConfig` and its JSON parsing in `ParameterRepository.parseSourceTransforms()`
5. Update `beam-transforms/README.md`

### Add a new report transformation step type

The report transformation chain uses raw BQ SQL — no new Java code needed.
Add a row to `report_transformation_config` with a new `query_template` referencing any alias
in the registry. Custom tokens go in `query_params_json`.

### Add a new BeamTransform (pluggable, SPI-registered)

1. Create class implementing `BeamTransform` in `beam-transforms/`
2. Use named `static final` inner classes for composite and DoFn
3. Output to both `SUCCESS_TAG` and `DEAD_LETTER_TAG`
4. Add to `META-INF/services/com.yourco.beam.transform.BeamTransform`
5. Update `beam-transforms/README.md`

### Add a new CLI flag

1. Add getter + setter in `FrameworkOptions.java` with `@Description`
2. Add `@Default.*` if it has a sensible default; `@Validation.Required` if mandatory
3. Update `beam-core/README.md` and root `README.md`

### Add a new report type

1. Insert rows in the 6 report DB tables (see DDL in `README.md`)
2. No Java code changes needed unless a new preprocessing step type is required
3. For new preprocessing types, add a `case` in `ReportPipelineFactory.runPreprocessing()`

---

## 8. DATA_SOURCE_DOWNLOAD — execution path

```
Main.runDataSourceDownload(options)
│
├─ DataSourcePipelineFactory.assemble(options)
│   ├─ DatabaseAdapterFactory.create(options)          JDBC connection pool (open)
│   ├─ ParameterRepository.allRequiredParametersExist  fail fast if config missing
│   ├─ ParameterRepository.fetchSourceConfigs()        load all SourceConfig rows
│   ├─ db.close()
│   │
│   └─ for each SourceConfig:
│       ├─ BigQueryCheckpointAdapter.getCheckpoint()   skip if FINISHED_ACCESSING
│       ├─ BigQueryCheckpointAdapter.write(STARTED)
│       ├─ BigQueryProcessStatusAdapter.write(PENDING)
│       ├─ SourceRouter.routeFromConfig()              API / FILE / BQ → PCollection<Row>
│       │   └─ QueryParameterResolver.resolve()        inject {periodStart}, custom tokens
│       ├─ SourceTransformChainAssembler.assemble()    LOOKUP → GROUP_BY → SORT_BY chain
│       └─ wirePerSourceSink()                         BigQueryIO.writeTableRows() per source
│
├─ Pipeline assembled. No data has moved.
├─ pipeline.run()                                      submit to Dataflow (or DirectRunner)
├─ result.waitUntilFinish()
│
└─ DataSourcePipelineFactory.runPostPipelineSteps(state, error)
    └─ for each SourceConfig that ran:
        ├─ ProcessStatusAdapter.queryRowCount(outputTable)
        ├─ ValidationConfig checks (row count bounds, BnC SUM checks)
        ├─ write COMPLETED / VALIDATION_FAILED / FAILED to process_status
        └─ write FINISHED_ACCESSING / FAILED_DOWNLOADING to pipeline_checkpoints
```

---

## 9. REPORT_PROCESSING — execution path (DB-configured mode)

Triggered when `--reportName` is set. Runs entirely in driver JVM — **no Beam pipeline**.

```
Main.runReportProcessing(options)
│
└─ ReportPipelineFactory.execute(options)
    ├─ DatabaseAdapterFactory.create()
    ├─ ReportRepository.fetchReportConfig()            load all 6 report tables
    ├─ db.close()
    ├─ BigQueryProcessStatusAdapter.write(PENDING)
    │
    ├─ Phase 1: Preprocessing (optional)
    │   └─ for each ReportPreprocessingStep (by step_order):
    │       └─ BigQueryJobService.runQueryToTable(resolvedSQL, outputTable)
    │
    ├─ Phase 2: Datasource availability check
    │   └─ for each required ReportDatasourceRef:
    │       └─ ProcessStatusAdapter.getLatest() → must be COMPLETED or FAIL
    │
    ├─ Phase 3: Build alias registry
    │   └─ ReportRepository.fetchDatasourceOutputTable() × N
    │       → alias → "project.dataset.table"
    │
    ├─ Phase 4: Transformation chain
    │   └─ for each ReportTransformStep (by step_order):
    │       ├─ resolveAliasTokens({alias} → `project.dataset.table`)
    │       ├─ QueryParameterResolver.resolve(sql, step.queryParams, options)
    │       ├─ BigQueryJobService.runQueryToTable(resolvedSQL, step.outputBqTable)
    │       └─ aliasRegistry.put(step.outputAlias, step.outputBqTable)
    │
    ├─ Phase 5: File export
    │   └─ for each ReportOutputConfig:
    │       ├─ BigQueryJobService.exportToCsv() or exportToJson()
    │       └─ record ExportedFile(gcsUri, fileName, contentType)
    │
    ├─ Phase 6: Email (optional)
    │   ├─ GcsUtils.readBytes(gcsUri) for each exported file
    │   ├─ resolve subject/body templates ({reportName}, {periodId}, etc.)
    │   └─ SmtpReportEmailAdapter.send(subject, body, to, cc, attachments)
    │
    └─ BigQueryProcessStatusAdapter.write(COMPLETED)
       or write(FAILED) if any phase threw
```

---

## 10. Query token resolution — three layers in order

For both DATA_SOURCE_DOWNLOAD (BQ queries) and REPORT_PROCESSING (transform chain):

```
Layer 1 — Alias tokens (REPORT_PROCESSING only)
    resolveAliasTokens(template, aliasRegistry)
    {trades} → `project.dataset.trades_output`

Layer 2 — Standard tokens (both process types)
    QueryParameterResolver.resolve() — pass 1
    {periodStart} → options.getPeriodStart()
    {periodEnd}   → options.getPeriodEnd()
    {periodId}    → options.getPeriodId()
    {runDate}     → DateUtils.resolveRunDate(options).toString()

Layer 3 — Custom tokens (both process types, from query_params_json column)
    QueryParameterResolver.resolve() — pass 2
    {exchange}  → "NYSE"    (from query_params_json)
    {threshold} → "10000"   (from query_params_json)
    Note: param values may reference standard tokens — those are resolved first.

Any number of custom tokens are supported. Unknown tokens are left unchanged.
```

---

## 11. Things you must never do

| Never | Do instead |
|---|---|
| Anonymous DoFn (lambda or anon class) | Named `static final` inner class |
| `java.util.function.Function` as DoFn field | `SerializableFunction` (Beam) |
| Import from `beam-utils` or `beam-transforms` inside `beam-io` | Keep `beam-io → beam-core` only |
| Import from `beam-io` inside `beam-utils` | `beam-utils → beam-core` only |
| Merge per-source outputs with `Flatten.pCollections()` | Keep each source as an independent branch |
| Hold secrets in FrameworkOptions values | Pass Secret Manager ID, fetch value at runtime |
| Call `BigQuerySchemaUtils`, `GcsUtils`, `ReportRepository` inside a DoFn | Call in driver JVM only |
| Create `TupleTag` inside `@ProcessElement` | `static final` field on the DoFn |
| Hardcode a new transform in `PipelineFactory` | Register via SPI manifest |
| Call `result.waitUntilFinish()` for streaming | Check source type first |
| Leave READMEs stale after a code change | Update in the same commit |
| Add `query_params_json` custom tokens that shadow alias names | Use distinct token names |
| Put SMTP credentials in pipeline options | Use `--smtpPasswordSecretId` + Secret Manager |

---

## 12. ProcessStatusAdapter — status tracking contract

One `process_status` BQ row per source (DATA_SOURCE_DOWNLOAD) or per report (REPORT_PROCESSING).

```
ProcessStatusAdapter.write(ProcessStatusRecord.pending(...))   — before data moves
ProcessStatusAdapter.write(ProcessStatusRecord.completed(...)) — success + validation passed
ProcessStatusAdapter.write(ProcessStatusRecord.validationFailed(...)) — BnC/row check failed
ProcessStatusAdapter.write(ProcessStatusRecord.failed(...))    — exception thrown

For reports:
ProcessStatusRecord.pendingReport(jobRunId, processType, reportName, subprocess, periodId, ...)
    datasource_name = reportName
    subprocess_name = reportSubprocess
    process_type    = "REPORT_PROCESSING"

queryRowCount(bqTableRef) — used for row count validation (DATA_SOURCE_DOWNLOAD)
querySum(bqTableRef, field) — used for BnC validation (DATA_SOURCE_DOWNLOAD)
getLatest(jobRunId, datasourceName, subprocessName) — used by REPORT_PROCESSING to check DS availability
    Note: jobRunId=null matches any run — finds the most recent status for that datasource/period
```

---

## 13. BigQueryJobService — BQ job contract

Used exclusively in driver JVM (ReportPipelineFactory). Not used in Beam workers.

```java
// Run a query and materialise result to a BQ table
bqJobService.runQueryToTable(resolvedSql, "project.dataset.table");

// Run a query with no destination (DDL, DML)
bqJobService.runQuery(resolvedSql);

// Export BQ table to GCS as CSV
bqJobService.exportToCsv("project.dataset.table", "gs://bucket/path/file.csv", includeHeader);

// Export BQ table to GCS as newline-delimited JSON
bqJobService.exportToJson("project.dataset.table", "gs://bucket/path/file.json");
```

Table refs use `project.dataset.table` (dot-separated, 3 parts) or `dataset.table` (2 parts).
All methods block until the BQ job completes. Failures throw `RuntimeException`.

---

## 14. Email adapter contract

```java
// Interface (beam-io)
ReportEmailAdapter.send(subject, body, to, cc, attachments);

// EmailAttachment — InputStream is consumed exactly once by the adapter
EmailAttachment.csv(inputStream, "report.csv")    // contentType=text/csv
EmailAttachment.json(inputStream, "report.json")  // contentType=application/json

// Concrete impl (beam-runner — needs jakarta.mail from beam-transforms transitive dep)
SmtpReportEmailAdapter(options)  // reads --emailSmtpHost, --emailSmtpPort, --smtpPasswordSecretId
```

To add a different email provider (SendGrid, SES), implement `ReportEmailAdapter` and
inject it into `ReportPipelineFactory` via constructor.

---

## 15. Checkpoint vs process_status — what each tracks

| Concept | Table | Written by | Read by |
|---|---|---|---|
| "Did we start/finish downloading this source?" | `pipeline_checkpoints` | `DataSourcePipelineFactory` | `DataSourcePipelineFactory` (skip logic) |
| "Did validation pass? How many rows?" | `process_status` | `BigQueryProcessStatusAdapter` | `ReportPipelineFactory` (DS availability check) |

Checkpoints use `STARTED_ACCESSING → FINISHED_ACCESSING / FAILED_DOWNLOADING`.
Process status uses `PENDING → COMPLETED / VALIDATION_FAILED / FAILED`.

---

## 16. Build and run reference

```bash
# Build fat JAR from project root
mvn package -pl beam-runner -am -DskipTests

# Run locally (DirectRunner)
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --processType=DATA_SOURCE_DOWNLOAD \
  --datasourceName=trades \
  --periodId=2024-01 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramDbUrl=jdbc:postgresql://localhost:5432/params \
  --paramDbUser=user \
  --checkpointBqDataset=pipeline_metadata \
  --processStatusBqDataset=pipeline_metadata

# Run REPORT_PROCESSING (DB-configured)
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --processType=REPORT_PROCESSING \
  --reportName=daily_trades_report \
  --reportSubprocess=eod \
  --periodId=2024-01 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramDbUrl=jdbc:postgresql://localhost:5432/params \
  --emailSmtpHost=smtp.gmail.com \
  --smtpPasswordSecretId=projects/p/secrets/smtp/versions/latest
```

---

## 17. Key invariants to preserve

1. `beam-core` has zero dependencies on sibling modules — it is the root.
2. `beam-io` depends only on `beam-core` — never on `beam-utils` or `beam-transforms`.
3. All Beam sources return `PCollection<Row>` with `.setRowSchema()` called.
4. Each `SourceConfig` produces exactly one independent Beam branch — never merged.
5. `BeamTransform` implementations always output to both `SUCCESS_TAG` and `DEAD_LETTER_TAG`.
6. Secrets are never stored in `FrameworkOptions` values — only Secret Manager IDs.
7. `BigQueryJobService`, `GcsUtils`, and `ReportRepository` are driver-JVM only — never inside DoFns.
8. Query token resolution order is always: alias tokens → standard tokens → custom tokens.
9. Every code change is accompanied by a README update in the same commit.
10. `process_status` rows are written before and after every source download and every report run.
