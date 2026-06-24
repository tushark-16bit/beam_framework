# Agent Guide — beam-pipeline-framework

This file is the primary reference for any AI agent working in this repository.
Read it fully before making any changes. Written for any capable language model (Claude, GPT, Gemini, etc.).

---

> ## ⚠️ SELF-ENFORCEMENT — READ BEFORE TOUCHING ANY FILE
>
> **Documentation is part of every task. Update it in the same response as the code change.
> Do not wait to be asked. The user should never need to say "document that".**
>
> ### After every code change, before responding to the user:
>
> | Changed | Must also update |
> |---|---|
> | Any `.java` file in `beam-io/` | `beam-io/README.md` — add/edit the class entry |
> | Any `.java` file in `beam-utils/` | `beam-utils/README.md` |
> | Any `.java` file in `beam-transforms/` | `beam-transforms/README.md` |
> | Any `.java` file in `beam-runner/` | `beam-runner/README.md` |
> | New class added anywhere | Section 4 file map in this file (`CLAUDE.md`) |
> | New CLI flag in `FrameworkOptions` | Section 17 build reference + `beam-core/README.md` + root `README.md` |
> | Execution path changed | Section 8 or 9 in this file + `WALKTHROUGH.md` sequence diagram |
> | New BQ table or schema | Section 10 in this file + root `README.md` |
> | Architecture rule changed | Section 5 in this file + root `README.md` |
> | New example or runnable | `EXAMPLE.md` if it teaches the BQ param pattern |
>
> ### Enforcement mechanisms (automatically active in this repo)
>
> - **Stop hook** (`.claude/settings.json`) — fires at the end of every Claude response and
>   prints a warning if code files changed without any doc files being touched.
> - **git pre-commit hook** (`.git/hooks/pre-commit`) — **blocks the commit** if `.java` files
>   are staged but no `README.md` / `CLAUDE.md` / `WALKTHROUGH.md` is staged alongside them.
>   Override only for genuine doc-free changes: `git commit --no-verify`.
>
> ### What "documented" means
>
> - The class appears in Section 4 of this file with a one-line description.
> - Its module README has an entry or updated entry.
> - If it changes an execution path, the execution path diagram (Section 8/9) reflects it.
> - If it adds a CLI flag, the flag appears in Section 17 and `beam-core/README.md`.
> - The commit message is descriptive enough that a reader can understand the change without
>   reading the diff.

---

For human-readable documentation, see [`README.md`](README.md), [`WALKTHROUGH.md`](WALKTHROUGH.md),
and the per-module `README.md` files.

---

## 1. What this project is

A configurable Apache Beam ETL pipeline framework in Java 17, running on GCP Dataflow and
triggered by Apache Airflow. It supports two process types:

- **DATA_SOURCE_DOWNLOAD** — fetches raw data from external sources (API, file, BigQuery),
  applies per-source transforms (lookup, group-by, sort), validates output, and writes to
  per-source BQ tables. Source config fetched from the **BigQuery** `source_config` table via
  `BigQuerySourceConfigRepository` — no JDBC, no code changes for new sources.
- **REPORT_PROCESSING** — reads downloaded data, applies a chained BigQuery transformation
  sequence, exports results to GCS files, and sends email with attachments. When `--reportName`
  is set, runs entirely in the driver JVM (no Dataflow job). All report configuration
  (6 report config tables + key-value parameter store) is fetched from **BigQuery** — no JDBC.
  Falls back to a generic source → transform chain → sink Beam pipeline when `--reportName` is blank.

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
| New BQ param/config table | `beam-io/README.md` (BQ config section) + root `README.md` |
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
2.  EXAMPLE.md                                        — end-to-end BQ param store example with DDL + run command
3.  beam-core/.../options/FrameworkOptions.java       — all CLI flags; the config contract
4.  beam-runner/.../runner/Main.java                  — entry point; how process type routes
5.  beam-runner/.../runner/DataSourcePipelineFactory  — DATA_SOURCE_DOWNLOAD orchestration
6.  beam-runner/.../runner/ReportPipelineFactory      — REPORT_PROCESSING orchestration (BQ-based)
7.  beam-io/.../io/params/BigQueryParameterAdapter    — key-value BQ param store interface + impl
8.  beam-io/.../io/config/BigQueryReportRepository    — report config tables fetched from BQ
9.  beam-core/.../transform/BeamTransform.java        — SPI interface; the extension contract
10. beam-runner/.../runner/PipelineFactory.java       — legacy REPORT_PROCESSING (transform chain)
11. beam-io/.../io/config/BigQuerySourceConfigRepository — source config rows fetched from BQ (DATA_SOURCE_DOWNLOAD)
12. beam-io/.../io/source/SourceRouter.java            — source type → connector mapping
13. beam-runner/.../runner/SourceTransformChainAssembler — LOOKUP/GROUP_BY/SORT_BY assembly
14. beam-io/.../io/report/BigQueryJobService.java      — how BQ jobs run for reports
15. beam-io/.../io/status/ProcessStatusAdapter.java    — status tracking interface
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

params/BigQueryParameterAdapter.java     Interface: fetchRequiredKeys(), fetchParameters(), fetchRequiredParameters().
params/BigQueryParameterAdapterImpl.java BQ client impl. Named query params (@key). Reads --paramBqProject/Dataset/StoreTable/RequiredTable.
                                         fetchRequiredParameters() = look up index → fetch values → validate all present.

config/BigQueryReportRepository.java       Queries all 6 report config BQ tables using named params.
                                           fetchReportConfig() and fetchDatasourceOutputTable().
config/BigQuerySourceConfigRepository.java Queries source_config BQ table for DATA_SOURCE_DOWNLOAD.
                                           fetchSourceConfigs(), getMissingParameters(). Row → SourceConfig mapping.

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

META-INF/services/...BeamTransform  SPI manifest. One class name per line.
```

### beam-runner — entry point and orchestrators

```
Main.java                       Parses CLI → routes by processType + reportName.
PipelineFactory.java            Legacy REPORT_PROCESSING: source → transform chain → sink.
DataSourcePipelineFactory.java  DATA_SOURCE_DOWNLOAD: per-source branches, post-pipeline validation.
ReportPipelineFactory.java      REPORT_PROCESSING (BQ-configured): driver-JVM BQ jobs + email.
                                Uses BigQueryReportRepository (not JDBC) for all config loading.
SourceTransformChainAssembler.java Assembles LOOKUP/GROUP_BY/SORT_BY per source; loads lookup views.
SmtpReportEmailAdapter.java     SMTP impl of ReportEmailAdapter. MimeMultipart for attachments.

example/ExampleWorkflow.java    Self-contained end-to-end example. Shows: BigQueryParameterAdapter
                                → fetchRequiredParameters → resolve tokens → BigQueryJobService
                                → exportToCsv → GCS. See EXAMPLE.md for BQ setup + run command.
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
6. Add required config fields to `SourceConfig` model and `BigQuerySourceConfigRepository.rowToSourceConfig()`
7. Update `beam-io/README.md` and root `README.md`

### Add a new per-source transform type

1. Create the transform class in `beam-transforms/source/` extending `PTransform<PCollection<Row>, PCollection<Row>>`
2. Add the type constant to `SourceTransformConfig` (e.g., `public static final String MY_TYPE = "MY_TYPE"`)
3. Add a case to `SourceTransformChainAssembler.assemble()` switch
4. Add config fields to `SourceTransformConfig` and its JSON parsing in `BigQuerySourceConfigRepository.toSourceTransforms()`
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
│   ├─ BigQuerySourceConfigRepository.getMissingParameters()  fail fast if config missing
│   ├─ BigQuerySourceConfigRepository.fetchSourceConfigs()    load all SourceConfig rows from BQ
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
    ├─ BigQueryReportRepository.fetchReportConfig()    BQ query all 6 report config tables
    │                                                  (report_config, report_datasource_ref,
    │                                                   report_preprocessing_config,
    │                                                   report_transformation_config,
    │                                                   report_output_config, report_email_config)
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
    │   └─ BigQueryReportRepository.fetchDatasourceOutputTable() × N
    │       → alias → "project.dataset.table"  (queries source_config BQ table)
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

## 10. BigQuery parameter store — how config is fetched for REPORT_PROCESSING

All REPORT_PROCESSING configuration lives in BigQuery, in the dataset specified by
`--paramBqProject` + `--paramBqDataset`. There are two conceptual layers:

### Layer A — Structured report config (6 tables, queried by BigQueryReportRepository)

These mirror the old JDBC tables, now stored in BQ:

| BQ Table | Contents | Key columns |
|---|---|---|
| `report_config` | One row per report variant | report_name, report_subprocess, period_id, override_key |
| `report_datasource_ref` | Which datasources a report needs | datasource_name, transform_alias, is_required |
| `report_preprocessing_config` | Pre-run BQ queries / API enrichment | step_order, bq_query, query_params_json |
| `report_transformation_config` | Chain of BQ transform queries | step_order, query_template, input_alias, output_alias |
| `report_output_config` | File output specs | output_format (CSV/JSON), gcs_path, file_prefix, file_suffix |
| `report_email_config` | Email recipients + templates | to_list, cc_list, subject_template, body_template |

### Layer B — Generic key-value parameter store (2 tables, queried by BigQueryParameterAdapter)

Used by `ExampleWorkflow` and any custom report code that prefers flat key-value config
over the structured 6-table schema:

| BQ Table | Contents | Key columns |
|---|---|---|
| `parameter_store` | One row per param per period | process_name, subprocess_name, period_id, param_key, param_value |
| `required_parameters_index` | Which keys each process needs | process_name, subprocess_name, param_key, is_required |

**Typical call sequence:**
```java
BigQueryParameterAdapter adapter = new BigQueryParameterAdapterImpl(options);
// 1. Look up required keys from required_parameters_index
// 2. Fetch values from parameter_store
// 3. Validate all required keys present → throws IllegalStateException if missing
Map<String, String> params = adapter.fetchRequiredParameters(reportName, subprocess, periodId);
```

### New CLI flags (replace --paramDb* JDBC flags for REPORT_PROCESSING)

| Flag | Default | Purpose |
|---|---|---|
| `--paramBqProject` | `--project` | GCP project for all config BQ tables |
| `--paramBqDataset` | `pipeline_config` | BQ dataset |
| `--paramStoreTable` | `parameter_store` | Key-value store table |
| `--paramRequiredTable` | `required_parameters_index` | Required-params index table |
| `--paramSourceConfigTable` | `source_config` | Source config table (for alias registry) |

**Note**: All configuration — for both DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING — is fetched
from **BigQuery** via `BigQuerySourceConfigRepository` or `BigQueryReportRepository`. No JDBC.

---

## 11. Query token resolution — three layers in order

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

## 12. Things you must never do

| Never | Do instead |
|---|---|
| Anonymous DoFn (lambda or anon class) | Named `static final` inner class |
| `java.util.function.Function` as DoFn field | `SerializableFunction` (Beam) |
| Import from `beam-utils` or `beam-transforms` inside `beam-io` | Keep `beam-io → beam-core` only |
| Import from `beam-io` inside `beam-utils` | `beam-utils → beam-core` only |
| Merge per-source outputs with `Flatten.pCollections()` | Keep each source as an independent branch |
| Hold secrets in FrameworkOptions values | Pass Secret Manager ID, fetch value at runtime |
| Call `BigQuerySchemaUtils`, `GcsUtils`, `BigQueryReportRepository` inside a DoFn | Call in driver JVM only |
| Make any JDBC / SQL database connection | All config is in BigQuery — use BigQuerySourceConfigRepository or BigQueryReportRepository |
| Hard-code param key names in Java for REPORT_PROCESSING | Fetch required keys from required_parameters_index |
| Create `TupleTag` inside `@ProcessElement` | `static final` field on the DoFn |
| Hardcode a new transform in `PipelineFactory` | Register via SPI manifest |
| Call `result.waitUntilFinish()` for streaming | Check source type first |
| Leave READMEs stale after a code change | Update in the same commit |
| Add `query_params_json` custom tokens that shadow alias names | Use distinct token names |
| Put SMTP credentials in pipeline options | Use `--smtpPasswordSecretId` + Secret Manager |

---

## 13. ProcessStatusAdapter — status tracking contract

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

## 14. BigQueryJobService — BQ job contract

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

## 15. Email adapter contract

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

## 16. Checkpoint vs process_status — what each tracks

| Concept | Table | Written by | Read by |
|---|---|---|---|
| "Did we start/finish downloading this source?" | `pipeline_checkpoints` | `DataSourcePipelineFactory` | `DataSourcePipelineFactory` (skip logic) |
| "Did validation pass? How many rows?" | `process_status` | `BigQueryProcessStatusAdapter` | `ReportPipelineFactory` (DS availability check) |

Checkpoints use `STARTED_ACCESSING → FINISHED_ACCESSING / FAILED_DOWNLOADING`.
Process status uses `PENDING → COMPLETED / VALIDATION_FAILED / FAILED`.

---

## 17. Build and run reference

```bash
# Build fat JAR from project root
mvn package -pl beam-runner -am -DskipTests

# Run DATA_SOURCE_DOWNLOAD locally (DirectRunner) — still uses JDBC for source config
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

# Run REPORT_PROCESSING (BQ-configured) — no JDBC required
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --processType=REPORT_PROCESSING \
  --reportName=daily_trades_report \
  --reportSubprocess=eod \
  --periodId=2024-01 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=pipeline_config \
  --emailSmtpHost=smtp.gmail.com \
  --smtpPasswordSecretId=projects/p/secrets/smtp/versions/latest

# Run ExampleWorkflow (BQ params → BQ transform → GCS CSV)
# See EXAMPLE.md for required BQ table setup
mvn -pl beam-runner exec:java \
  -Dexec.mainClass=com.yourco.beam.runner.example.ExampleWorkflow \
  "-Dexec.args=--project=my-gcp-project --paramBqProject=my-gcp-project \
    --paramBqDataset=pipeline_config --reportName=daily_trades_summary \
    --reportSubprocess=eod --periodId=2024-01 \
    --periodStart=2024-01-01 --periodEnd=2024-01-31 \
    --processType=REPORT_PROCESSING --sinkType=GCS"
```

---

## 18. Key invariants to preserve

1. `beam-core` has zero dependencies on sibling modules — it is the root.
2. `beam-io` depends only on `beam-core` — never on `beam-utils` or `beam-transforms`.
3. All Beam sources return `PCollection<Row>` with `.setRowSchema()` called.
4. Each `SourceConfig` produces exactly one independent Beam branch — never merged.
5. `BeamTransform` implementations always output to both `SUCCESS_TAG` and `DEAD_LETTER_TAG`.
6. Secrets are never stored in `FrameworkOptions` values — only Secret Manager IDs.
7. `BigQueryJobService`, `GcsUtils`, `BigQueryReportRepository`, `BigQuerySourceConfigRepository`, and `BigQueryParameterAdapter` are driver-JVM only — never inside DoFns.
11. No JDBC. No SQL database connections. All config and data flow through BigQuery.
8. Query token resolution order is always: alias tokens → standard tokens → custom tokens.
9. Every code change is accompanied by a README update in the same commit.
10. `process_status` rows are written before and after every source download and every report run.
