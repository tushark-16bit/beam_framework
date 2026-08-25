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
    FileSourceAdapter         — pure file adapter: CSV (Commons CSV) + Excel (Apache POI). Each data row is
                                keyed by Excel-style column letters (A, B, ..., Z, AA, AB, ...) in file column
                                order — see columnLetter() — never by real header text. When the file has a
                                header row, the real names are captured separately as a header-legend content
                                object (letter → real name), wrapped via FileHeaderLegend.wrapLegend() for
                                transit rather than used as row keys; no legend is produced when the file has
                                no header row. parseCsv/parseExcel return a FileParseResult(rows, headerLegendJson).
                                FileSourceConfig.firstRow (1-based, default 1) skips leading rows before real
                                content starts — the row at that position becomes the header row (hasHeader=true)
                                or the first data row (hasHeader=false). Column width for both the header legend
                                and every data row is FileSourceConfig.lastColumn (explicit Excel-style letter)
                                when set, otherwise auto-detected as the widest row seen (header or data) — so a
                                data row with more columns than the header is never truncated. columnIndexFromLetter()
                                is the inverse of columnLetter(), used to resolve lastColumn to a column count.
                                Both parseCsv/parseExcel share this width resolution via the private
                                resolveColumnCount(config, headerWidth, maxDataWidth) helper.
    FileSourceTransform       — Beam wrapper for FileSourceAdapter (downloads GCS bytes, parses). Emits one
                                Row per data row, then (if present) one extra Row carrying the marker-wrapped
                                header-legend JSON — both under the same Schemas.RAW_JSON schema.
                                DataSourceRecordSinkTransform detects and unwraps the legend and handles it
                                specially (see io/sink/).

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
                                          stageFromDaRec uses CROSS JOIN UNNEST(FileHeaderLegend.dataArrayExpr(...)) to un-nest
                                          DaRec pages into individual source-row JSON objects in stage_da_json_tx — this handles
                                          both a plain array page and a FILE source's {"Data":[...],"DataHeaders":[...]} page.
                                          The header legend, when present, lives in the separate DataHeaders array, so it's
                                          never unnested and never staged into a report's input data.

io/records/
    DataSourceRecordAdapter         — interface: countRecords(daId), sumField(daId, field), deleteRecords(daId)
    BigQueryDataSourceRecordAdapter — row_da_json_tx per page is a flat JSON array, or (FILE source with a
                                      header) {"Data":[...],"DataHeaders":[...]}. countRecords unnests via
                                      FileHeaderLegend.dataArrayExpr(...) — which extracts just the row array
                                      either way — and COUNT(*)s individual rows (not SUM(JSON_ARRAY_LENGTH(...))
                                      of the raw column, which would count the wrapper/legend structure instead
                                      of real rows for a FILE page). sumField unnests the same way with
                                      CROSS JOIN UNNEST(FileHeaderLegend.dataArrayExpr(...)) AS row_json then
                                      SUM(CAST(JSON_VALUE(row_json, '$.field') AS FLOAT64)). The header legend,
                                      when present, lives in the separate DataHeaders array and is never
                                      unnested by either method — no explicit exclusion needed, and a no-op
                                      distinction for BQ/API sources, which never produce a legend.
                                      deleteRecords(daId) — DELETE FROM DaRec WHERE da_id=@daId; best-effort at
                                      this level (logs and swallows failures). Used directly for the
                                      --manualOverrun cleanup of an older, already-flushed run (safe to be
                                      best-effort there). For the data_transform_query replace of THIS run's
                                      just-streamed rows, beam-runner's PostDownloadFinalizeTransform does NOT
                                      use this method — it runs DELETE+INSERT as a single atomic BigQuery
                                      multi-statement transaction instead (see beam-runner/README.md), since
                                      those rows can still be in BigQuery's streaming buffer (DML-ineligible
                                      even though already SELECT-visible) and two separate unverified
                                      statements could leave a partial delete, or a successful delete with a
                                      failed insert and nothing to restore the originals.
                                      Has a String-tableRef constructor for in-worker use (DoFn @Setup).

io/sink/
    DataSourceRecordSinkTransform   — Collects ALL source rows, paginates at 250 rows/page, writes one
                                      DaRec row per page with row_da_json_tx = JSON array of that page.
                                      Uses streaming inserts (rows immediately queryable).
                                      Returns PCollection<Long> = total source rows (not page count),
                                      held until all inserts complete, for PostDownloadFinalizeTransform
                                      to Wait.on(). DaRec schema gains a page_no INT64 column.
                                      PaginateAndBuildDoFn detects a FILE-source header-legend element
                                      (marker-wrapped via FileHeaderLegend.wrapLegend) among the incoming
                                      rows, unwraps it, excludes it from totalRows and page-size accounting,
                                      and builds each page for that source as
                                      {"Data":[...rows...],"DataHeaders":[legend]} instead of a flat array —
                                      appending a copy of the legend to EVERY page so each is independently
                                      self-describing. Headerless FILE pages, and all BQ/API pages, keep the
                                      original flat [{...},{...},...] shape.

io/util/
    JsonUtils                 — shared type-aware Row → JSON serializer
    FileHeaderLegend           — helpers for the FILE-source column-letter storage convention: wrapLegend()/
                                 isMarkerWrapped()/unwrapLegend() thread a header-legend content object through
                                 the same PCollection<Row> as data rows without being mistaken for one;
                                 buildPage() builds a FILE-with-header DaRec page as
                                 {"Data":[rows...],"DataHeaders":[legend]}; dataArrayExpr() is the one SQL
                                 expression every DaRec reader uses to extract just the row array, whether a
                                 page is that shape or a plain array — no per-source-type branching needed.

io/config/
    BigQuerySourceConfigRepository — reads source connector config from parameter_store (parameters_val_json).
                                     Key: (parameter_group_name=parentId, parameter_data_source=subprocessName,
                                     parameter_name=datasourceName). Validates required fields from schema_of_json;
                                     parses parameters_val_json into SourceConfig. No separate source_config table.
                                     Also parses the optional bq_schema_json array into BqFetchConfig.schema
                                     (List<SourceSchemaField>) — an operator-declared column schema for BQ sources.
                                     Also parses data_transform_query / data_transform_min_row_count /
                                     data_transform_max_row_count into SourceConfig.dataTransformConfig
                                     (DataTransformConfig) — an optional post-storage SQL transform.
    BigQueryReportRepository       — reads report config nested JSON from parameter_store for REPORT_PROCESSING
                                     AND PIPELINE (same lookup, same ReportConfig — PIPELINE has no config of its
                                     own; see beam-runner/README.md's PipelineSequenceFactory section).
                                     Key: (parameter_group_name=parentId, parameter_data_source=reportSubprocess,
                                     parameter_name=reportName). Parses parameters_val_json into ReportConfig.
                                     Includes datasources, preprocessing, transforms, outputs, email arrays,
                                     and top-level output_bq_table / output_bq_input_alias for per-report BQ write.

io/email/
    ReportEmailAdapter        — interface: send(subject, body, to, cc, List<EmailAttachment>)
    EmailAttachment           — attachment model (InputStream + fileName + contentType)

io/report/
    BigQueryJobService        — BQ jobs: query→table, table→GCS export (CSV/JSON), countRows(tableRef)
                                (exact live SELECT COUNT(*), not table-metadata based), dropTableIfExists()
                                (best-effort cleanup). No-arg constructor holds only a plain BigQuery client
                                (no FrameworkOptions), so it's also safe to use inside a Beam worker DoFn —
                                PostDownloadFinalizeTransform.FinalizeDoFn does this to run data_transform_query.
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

## FILE-source storage convention — column letters, not header names

CSV/Excel rows are stored in `DaRec` keyed by Excel-style column letters (`A`, `B`, ..., `Z`,
`AA`, `AB`, ...) in file column order, never by the file's own header text.

**Page shape.** Every other source type (BQ, API) stores a `DaRec` page's `row_da_json_tx` as a
flat JSON array of row objects, exactly as before: `[{...},{...},...]`. A FILE source **with a
header row** (`file_has_header=true`) instead stores each page as a single JSON object with two
arrays — `Data` holding the page's rows, `DataHeaders` holding one object that maps each column
letter to its real header name:

```json
{
  "Data": [
    {"A": "T1001", "B": "500.00", "C": "2024-01-15"},
    {"A": "T1002", "B": "300.00", "C": "2024-01-16"}
  ],
  "DataHeaders": [
    {"A": "trade_id", "B": "amount", "C": "trade_date"}
  ]
}
```

The same `DataHeaders` entry is appended to **every** page for that source, so each page is
independently self-describing. A FILE source with **no** header row produces no legend at all —
there's no real name to record — and its pages keep the original flat `[{...},{...},...]` shape,
same as BQ/API.

**Why column letters instead of header text**: storage never depends on header text being
stable, unique, spelled consistently, or even present. The same file re-uploaded with a
reworded header still lands in the same letter slots.

**Where reading starts — `file_first_row`.** By default the very first row of the file is used.
Setting `file_first_row` (1-based) skips everything before it — useful when a file has leading
title/metadata rows before the real table begins. The row landing at `file_first_row` becomes the
header row when `file_has_header=true`, or the first data row otherwise.

**Column width — auto-detected, or fixed via `file_last_column`.** The header row and the data
rows are not required to have the same number of columns. If the header row is narrower than the
data (e.g. 5 header cells but data rows carrying 20 values), every data column is still stored —
column width is computed as the widest row seen (header or data), never just the header's width,
so a column with no header name simply gets a `null` entry in `DataHeaders` rather than having its
data silently dropped. Setting `file_last_column` (an Excel-style letter, e.g. `"T"`) overrides
auto-detection with a fixed width instead: columns beyond it are intentionally dropped from both
the legend and every data row, even if the file itself extends further.

**Consequence for config that references FILE-sourced fields**: `bnc_rules_json`,
`data_transform_query`, and `source_transforms_json` (`GROUP_BY`/`SORT_BY`/`LOOKUP`) written
against a FILE source's parsed fields must reference the **column letter** (`A`, `B`, ...), not
the original header name — look up the mapping via the `DataHeaders` object, or check the
source file's column order directly. Field paths are unaffected by the page-level wrapping:
`JSON_VALUE(row_json, '$.A')` still works exactly as before, since `row_json` (after unnesting)
is the same letter-keyed row object regardless of which page shape it came from.

**Reading these rows**: every `DaRec` reader that unnests `row_da_json_tx` into individual
rows — `countRecords()`, `sumField()`, the `data` CTE always prepended to `data_transform_query`,
REPORT_PROCESSING staging — extracts the row array with `FileHeaderLegend.dataArrayExpr()`, one SQL expression that
handles both page shapes (`IFNULL(JSON_EXTRACT_ARRAY(JSON_EXTRACT(row_da_json_tx, '$.Data')),
JSON_EXTRACT_ARRAY(row_da_json_tx))`). No caller needs to know the source type or exclude
anything explicitly — `DataHeaders` is structurally separate from `Data` and is simply never
reached by unnesting `Data`.

---

## JsonUtils

`JsonUtils.rowToJson(row)` converts a Beam `Row` to a JSON string with correct type handling:

| Beam type | JSON output |
|---|---|
| `STRING` | `"value"` (quoted, escaped) |
| `INT64`, `DOUBLE`, `BOOLEAN` | `42`, `3.14`, `true` (unquoted) |
| `null` | `null` |

Used by `GcsSinkTransform` and `PubSubSinkTransform`. Import it in your own sinks.

---

## Unit tests

`src/test/java` — JUnit 5, no BigQuery/GCS mocking required for these; they exercise pure logic
directly, the same scenarios that were previously hand-verified with throwaway scripts:

```
io/source/FileSourceAdapterTest.java   — columnLetter/columnIndexFromLetter round-trip at known
                                          Excel boundaries (Z→AA, AZ→BA, ZZ→AAA); parseCsv: data
                                          wider than header keeps every column, file_first_row
                                          skip (with and without a header), file_last_column
                                          truncation, empty-file edge case.
io/util/FileHeaderLegendTest.java      — wrapLegend/unwrapLegend round-trip, isMarkerWrapped
                                          true/false, buildPage's exact {"Data":...,"DataHeaders":...}
                                          shape, dataArrayExpr's SQL fragment text.
```

Run with `mvn -pl beam-io -am test`. This is the starting point, not full coverage — the rest of
`beam-io` (BigQuery/GCS-backed adapters) has none yet and would need Mockito (already a
`dependencyManagement` entry in the root `pom.xml`, unused until now).
