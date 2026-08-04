# beam-io

Source and sink connectors for GCS, BigQuery, and Pub/Sub.
Also contains the `DeadLetterSinkTransform` for writing failed records.

---

## What lives here

```
io/source/
    SourceRouter              — two modes: routeByOptions (REPORT_PROCESSING) + routeFromConfig (DATA_SOURCE_DOWNLOAD); overload with nullable Schema param passes pre-fetched schema to BigQuerySourceTransform
    BigQuerySourceTransform   — reads from BQ table or SQL query; typed mode uses a pre-fetched Schema (passed from beam-runner) with a custom TableRow→Row mapping; generic fallback when schema is null
                                resolves real column names via a SELECT * LIMIT 1 preview query (no tables.get needed), all-STRING;
                                falls back further to Schemas.RAW_JSON blob if even that query fails
    GcsSourceTransform        — reads newline-delimited JSON from GCS glob
    PubSubSourceTransform     — reads from a Pub/Sub subscription (streaming)
    ApiSourceAdapter          — pure HTTP adapter: auth, pagination (PAGE_NUMBER/CURSOR/OFFSET)
    ApiSourceTransform        — Beam wrapper for ApiSourceAdapter (@Setup/@Teardown for HttpClient)
    FileSourceAdapter         — pure file adapter: CSV (Commons CSV) + Excel (Apache POI)
    FileSourceTransform       — Beam wrapper for FileSourceAdapter (downloads GCS bytes, parses)

io/sink/
    SinkRouter                — factory: picks the right sink from --sinkType
    BigQuerySinkTransform     — writes to a BQ table (TRUNCATE or APPEND)
    GcsSinkTransform          — writes as newline-delimited JSON to GCS
    PubSubSinkTransform       — publishes each Row as a JSON message
    DeadLetterSinkTransform   — writes FailedRecords as JSON lines to GCS

io/checkpoint/
    DataSourceCheckpointAdapter         — interface: createCheckpoint(), updateStatus(), isCompleted(), getLatest(), fetchLatestCompletedDaId(). perId is int.
    BigQueryDataSourceCheckpointAdapter — BQ DML impl; MAX(da_id)+1 sequence, MAX(vsn_no)+1 per (srce_nm, per_id). All timestamps DATETIME (LocalDateTime).
                                          Has a String-tableRef constructor for in-worker use (DoFn @Setup).
    ReportCheckpointAdapter             — interface for 4 report tables: RptRefer (createCheckpoint/updateStatus/isCompleted), RptDaMap (addDaMapping), RptStageDa (stageFromDaRec/stagedDataSubquery/clearStagedData), RptOutput (writeOutput).
    BigQueryReportCheckpointAdapter     — BQ DML impl for all 4 report tables. All timestamps DATETIME. Stage_id generated via MAX+ROW_NUMBER() OVER().
                                          stageFromDaRec uses CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx)) to un-nest
                                          DaRec pages into individual source-row JSON objects in stage_da_json_tx.

io/records/
    DataSourceRecordAdapter         — interface: countRecords(daId), sumField(daId, field)
    BigQueryDataSourceRecordAdapter — row_da_json_tx is a JSON array per page; countRecords uses
                                      SUM(JSON_ARRAY_LENGTH(...)); sumField unnests with
                                      CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(...)) AS row_json then
                                      SUM(CAST(JSON_VALUE(row_json, '$.field') AS FLOAT64)).
                                      Has a String-tableRef constructor for in-worker use (DoFn @Setup).

io/sink/
    DataSourceRecordSinkTransform   — Collects ALL source rows, paginates at 250 rows/page, writes one
                                      DaRec row per page with row_da_json_tx = JSON array of that page.
                                      Uses streaming inserts (rows immediately queryable).
                                      Returns PCollection<Long> = total source rows (not page count),
                                      held until all inserts complete, for PostDownloadFinalizeTransform
                                      to Wait.on(). DaRec schema gains a page_no INT64 column.

io/util/
    JsonUtils                 — shared type-aware Row → JSON serializer

io/config/
    BigQuerySourceConfigRepository — reads source connector config from parameter_store (parameters_val_json).
                                     Key: (parameter_group_name=parentId, parameter_data_source=subprocessName,
                                     parameter_name=datasourceName). Validates required fields from schema_of_json;
                                     parses parameters_val_json into SourceConfig. No separate source_config table.
                                     Also parses the optional bq_schema_json array into BqFetchConfig.schema
                                     (List<SourceSchemaField>) — an operator-declared column schema for BQ sources.
    BigQueryReportRepository       — reads report config nested JSON from parameter_store for REPORT_PROCESSING.
                                     Key: (parameter_group_name=parentId, parameter_data_source=reportSubprocess,
                                     parameter_name=reportName). Parses parameters_val_json into ReportConfig.
                                     Includes datasources, preprocessing, transforms, outputs, email arrays,
                                     and top-level output_bq_table / output_bq_input_alias for per-report BQ write.

io/email/
    ReportEmailAdapter        — interface: send(subject, body, to, cc, List<EmailAttachment>)
    EmailAttachment           — attachment model (InputStream + fileName + contentType)

io/report/
    BigQueryJobService        — driver-JVM BQ jobs: query→table, table→GCS export (CSV/JSON)
```

### Adapter pattern

Every external I/O boundary has **two layers**:
- **Adapter** (`ApiSourceAdapter`, `FileSourceAdapter`) — pure Java, no Beam, unit-testable without a pipeline
- **Transform** (`ApiSourceTransform`, `FileSourceTransform`) — thin Beam wrapper, only handles `@Setup`/`@Teardown` lifecycle

This separation keeps Beam boilerplate out of business logic and makes adapters easy to test independently.

---

## How SourceRouter and SinkRouter work

Both are stateless factories. They read `--sourceType` / `--sinkType` from options
and delegate to the right connector. The switch expression is exhaustive — adding a
new enum value without a case is a **compile error**, not a silent bug.

```java
// SourceRouter.java
return switch (options.getSourceType()) {
    case GCS    -> pipeline.apply("Source-GCS",    new GcsSourceTransform(options));
    case BQ     -> pipeline.apply("Source-BQ",     new BigQuerySourceTransform(options));
    case PUBSUB -> pipeline.apply("Source-PubSub", new PubSubSourceTransform(options));
};
```

---

## Adding a new source

1. Create `MyNewSourceTransform extends PTransform<PBegin, PCollection<Row>>` in `io/source/`
2. Add `MY_NEW_SOURCE` to `SourceType` enum in `beam-core`
3. Add a case to `SourceRouter`
4. Add required flags to `FrameworkOptions`

The compiler will tell you every place you forgot a case.

---

## Schema contract

All sources produce `PCollection<Row>` with a declared schema set via `setRowSchema()`.

- **GCS** and **Pub/Sub** produce a single-field schema: `raw_json STRING`.
  Downstream transforms must parse this field (e.g., a `flatten-json` transform).
- **BigQuery** (typed path): when a pre-fetched `Schema` is supplied by the beam-runner
  caller (`DataSourcePipelineFactory.fetchBqSchema()` / `PipelineFactory.fetchBqSchema()`),
  each `TableRow` field is converted using a custom type-safe mapping:
  INTEGER → `Long`, FLOAT → `Double`, BOOLEAN → `Boolean`, BYTES → `byte[]`,
  and TIMESTAMP/DATE/DATETIME/TIME + STRING + NUMERIC/BIGNUMERIC → `String` (ISO strings /
  exact decimal text as-is from `BigQueryIO.readTableRows()` JSON encoding). Does **not** use
  `BigQueryUtils.toBeamRow()` — that method assumes Avro encoding and throws
  `NumberFormatException` on ISO temporal
  strings like `"2024-01-07T00:00:00"`. A value that doesn't match its declared type throws
  `IllegalStateException` naming the column, declared type, and offending value — this is
  the hard validation failure for a source with an explicit `bq_schema_json` schema.
  The `Schema` for the typed path is resolved in three tiers by
  `DataSourcePipelineFactory.fetchBqSchema()`: (1) operator-declared `BqFetchConfig.schema`
  via `BigQuerySchemaUtils.toBeamSchema()` — no BQ call at all, authoritative; (2) BQ table
  metadata via `BigQuerySchemaUtils.fetchBeamSchema()`; (3) `null`, deferring to the fallback
  below.
  Fallback (schema is `null`): `BigQuerySourceTransform.expand()` runs a lightweight
  `SELECT * FROM (...) LIMIT 1` preview query in the driver JVM to learn real column
  *names* — this needs only query-execution rights, not `bigquery.tables.get`, so it still
  works when the caller's `BigQuerySchemaUtils.fetchBeamSchema()` was denied table-metadata
  access. Builds one nullable-STRING field per resolved column name and applies that same
  schema to both `.setRowSchema()` and every emitted `Row` (no `_row_json`/per-row schema
  mismatch). If even the preview query fails, falls back further to `Schemas.RAW_JSON`
  (single `raw_json` blob field, whole row as JSON text) — same convention as GCS/Pub/Sub.

---

## BigQuery write disposition

Controlled by `--writeDisposition`:

| Value | Behaviour | Idempotent? |
|---|---|---|
| `TRUNCATE` (default) | Deletes all rows before writing | ✅ Yes — safe to re-run |
| `APPEND` | Adds rows to existing table | ❌ No — re-runs duplicate data |

Use `TRUNCATE` for report pipelines. Use `APPEND` only for event streams where
duplication is acceptable or handled externally.

---

## Dead-letter sink

`DeadLetterSinkTransform` writes `FailedRecord` objects to GCS as JSON lines.
Each line contains: `payload`, `errorMessage`, `errorClass`, `attemptCount`, `failedAtUtc`.

Configure with `--deadLetterSink=gs://bucket/dlq/my-pipeline/`.

Files land at `gs://bucket/dlq/my-pipeline/XXXXX.json` and can be inspected with:
```bash
gsutil cat "gs://bucket/dlq/my-pipeline/*.json" | jq .
```

---

## JsonUtils

`JsonUtils.rowToJson(row)` converts a Beam `Row` to a JSON string with correct type handling:

| Beam type | JSON output |
|---|---|
| `STRING` | `"value"` (quoted, escaped) |
| `INT64`, `DOUBLE`, `BOOLEAN` | `42`, `3.14`, `true` (unquoted) |
| `null` | `null` |

Used by `GcsSinkTransform` and `PubSubSinkTransform`. Import it in your own sinks.
