# beam-io

Source and sink connectors for GCS, BigQuery, and Pub/Sub.
Also contains the `DeadLetterSinkTransform` for writing failed records.

---

## What lives here

```
io/source/
    SourceRouter              — two modes: routeByOptions (REPORT_PROCESSING) + routeFromConfig (DATA_SOURCE_DOWNLOAD)
    BigQuerySourceTransform   — reads from BQ table or SQL query
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
    DataSourceCheckpointAdapter         — interface: createCheckpoint(), updateStatus(), isCompleted(), getLatest()
    BigQueryDataSourceCheckpointAdapter — BQ DML impl; MAX(dataSourceId)+1 sequence, MAX(vsnNo)+1 per (srcName,PerId)

io/records/
    DataSourceRecordAdapter         — interface: countRecords(dataSourceId), sumField(dataSourceId, field)
    BigQueryDataSourceRecordAdapter — BQ query using JSON_VALUE(RowDSJsonTx, '$.field') for BnC validation

io/sink/
    DataSourceRecordSinkTransform   — Beam PTransform writing all rows as JSON blobs to data_source_records

io/util/
    JsonUtils                 — shared type-aware Row → JSON serializer

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
- **BigQuery** currently produces a single-field `_row_json STRING` schema.
  For column-level transforms, use `BigQuerySchemaUtils.fetchBeamSchema(tableRef)`
  from `beam-utils` to fetch the real schema and wire it into the source.

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
