# beam-utils

Shared utility helpers for transforms, the runner, and external transform modules.
Contains no Beam pipeline graph code — no `PTransform`, no `DoFn`.

---

## Utilities

| Class | Purpose |
|---|---|
| `BigQuerySchemaUtils` | Fetch real BQ table schema at pipeline-assembly time (table metadata or an operator-declared `bq_schema_json` list) |
| `GcsUtils` | Pre-flight path checks, read/write small files, list objects, delete prefixes |
| `SecretManagerUtils` | Fetch secrets from GCP Secret Manager by secret ID (never by value) |
| `RowValidationUtils` | Stateless row-level validators: required fields, patterns, ranges, allowed values |
| `MetricsUtils` | Factory for consistently-named Beam counters, distributions, and gauges |
| `CalendarUtils` | Business calendar stubs: `isBusinessDay`, `nextBusinessDay`, `applyOffset`, etc. |
| `DateUtils` | Run date resolution, formatting (ISO/compact/display), partitioned paths, sharded BQ tables |
| `RunDateCalculator` | The one place run dates are decided for every source and report. `resolve(RunScheduleConfig, options)` → `RunDates`: no schedule → `fromOptions(options)` (the CLI flags, unchanged behaviour); schedule configured → `calculateRunDates(schedule, asOfDate)`, a **stub** to implement (per-source/report analog of `CalendarUtils`, resolved against a separate external calendar DB keyed by `calendarKey`) |
| `QueryParameterResolver` | Resolves `{periodStart}`/`{periodEnd}`/`{periodId}`/`{runDate}` standard tokens (also available as `%periodStart%`/`%periodEnd%`/`%periodId%`/`%runDate%` — a fixed percent-delimited alternative, same underlying values, for SQL dialects where curly braces collide with something else), then custom tokens merged from a step's `query_params_json` and `--customParamsJson` (CLI flag, wins on collision) in query templates for both `DATA_SOURCE_DOWNLOAD` and `REPORT_PROCESSING` |

There is no JDBC / relational-DB adapter in this module — the framework has no JDBC dependency
anywhere (see `CLAUDE.md` §12). All configuration lives in BigQuery, fetched via
`BigQuerySourceConfigRepository` (source configs) and `BigQueryReportRepository` (report configs)
in `beam-io`.

```java
// Pattern for fetching source config from BigQuery parameter_store:
BigQuerySourceConfigRepository repo = new BigQuerySourceConfigRepository(options);
// fetchSourceConfigs throws IllegalStateException if the row is missing — no separate check needed
List<SourceConfig> configs = repo.fetchSourceConfigs(
    options.getParentId(), options.getDatasourceName(),
    options.getSubprocessName(), options.getPeriodId());
```

---

## Unit tests

`src/test/java` — `RunDateCalculatorTest.java`: `checkRunWindow()` is in-window with no schedule or no
bounds, and reaches the freq/max-freq stubs when those are configured. `QueryParameterResolverTest.java`: standard-token resolution, step-level
`query_params_json` resolution, `--customParamsJson` resolution and its override of a
same-named step-level key, standard-token references inside a custom value, and malformed/
non-object `--customParamsJson` rejection. Run with `mvn -pl beam-utils -am test`.

**Required DB tables** (must be created before first run):

```sql
-- Source configuration (one row per datasource/period/subprocess):
CREATE TABLE source_config (
  -- Identity
  datasource_name         VARCHAR(100)  NOT NULL,
  period_id               VARCHAR(50)   NOT NULL,
  subprocess_name         VARCHAR(100)  NOT NULL,
  source_type             VARCHAR(20)   NOT NULL,  -- API | FILE | BQ

  -- API source
  api_endpoint            TEXT,
  api_auth_type           VARCHAR(20),             -- NONE | BEARER | BASIC | API_KEY
  api_auth_secret_id      TEXT,
  api_headers_json        TEXT,                    -- {"X-Custom": "value"}
  api_query_params_json   TEXT,                    -- {"format": "json"}
  api_pagination_enabled  BOOLEAN,
  api_pagination_strategy VARCHAR(20),             -- PAGE_NUMBER | CURSOR | OFFSET
  api_page_size           INT,
  api_next_page_field     VARCHAR(100),
  api_data_array_field    VARCHAR(100),

  -- FILE source
  file_type               VARCHAR(20),             -- CSV | EXCEL
  file_location           TEXT,
  file_prefix             TEXT,
  file_suffix             TEXT,
  file_delimiter          VARCHAR(5),
  file_has_header         BOOLEAN,
  file_sheet_index        INT,

  -- BQ source
  bq_project_id           VARCHAR(100),
  bq_dataset              VARCHAR(100),
  bq_table                VARCHAR(100),
  bq_query                TEXT,                    -- SQL template (may contain {periodStart} etc)

  -- Query parameter injection (applied to bq_query before execution)
  query_params_json       TEXT,                    -- {"startDate":"{periodStart}","exchange":"NYSE"}

  -- Per-source output destination
  output_type             VARCHAR(20),             -- BQ | GCS
  output_bq_project       VARCHAR(100),
  output_bq_dataset       VARCHAR(100),
  output_bq_table         VARCHAR(100),
  output_gcs_path         TEXT,
  output_write_mode       VARCHAR(20),             -- TRUNCATE | APPEND

  -- Per-source transform chain (ordered JSON array)
  source_transforms_json  TEXT,
  -- Example:
  -- [{"type":"LOOKUP","lookupSourceType":"BQ","lookupBqTableRef":"proj:ds.fx",
  --   "lookupKeyField":"ccy_code","dataKeyField":"currency"},
  --  {"type":"GROUP_BY","groupByFields":["currency","date"],
  --   "aggregations":[{"field":"amount","function":"SUM","outputField":"total_amount"}]}]

  -- Validation rules
  min_row_count           BIGINT,                  -- 0 = no check
  max_row_count           BIGINT,                  -- -1 = no check
  required_headers_json   TEXT,                    -- ["trade_id","amount","currency"]
  bnc_rules_json          TEXT,                    -- [{"field":"amount","expectedTotal":1000000,"tolerancePct":0.01}]

  PRIMARY KEY (datasource_name, period_id, subprocess_name)
);

-- Optional required-parameter guard:
CREATE TABLE required_parameters (
  datasource_name   VARCHAR(100) NOT NULL,
  period_id         VARCHAR(50)  NOT NULL,
  subprocess_name   VARCHAR(100) NOT NULL,
  parameter_key     VARCHAR(200) NOT NULL,
  PRIMARY KEY (datasource_name, period_id, subprocess_name, parameter_key)
);
```

---

## BigQuerySchemaUtils

Solves the schema problem in `BigQuerySourceTransform`. Call it at pipeline-assembly time
to get real column names so transforms can operate on actual fields:

```java
// In PipelineFactory or a custom source, before pipeline.run():
Schema schema = BigQuerySchemaUtils.fetchBeamSchema("my-project:my-dataset.orders");

// Now pass schema into BigQuerySourceTransform so it produces typed Rows:
// order_id STRING, customer_email STRING, amount DOUBLE, ...
// instead of the generic name-only fallback (see BigQuerySourceTransform's own
// SELECT * LIMIT 1 preview query, or Schemas.RAW_JSON as a last resort)
```

**Never call inside a DoFn** — each worker would make a BQ API call.

### toBeamSchema(declaredFields) — from an operator-declared schema, not BQ metadata

Builds a `Schema` from a `List<SourceSchemaField>` — the parsed `bq_schema_json` column list
on `BqFetchConfig.schema` — instead of querying BigQuery table metadata at all:

```java
Schema schema = BigQuerySchemaUtils.toBeamSchema(bqFetchConfig.schema);
```

No BQ API call, so no `bigquery.tables.get` permission needed — the schema is exactly what
the operator declared in `parameter_store`. Unlike `fetchBeamSchema()` (which defaults an
unrecognised BQ-reported type to STRING), an unrecognised `bqType` here throws
`IllegalArgumentException` immediately — a typo in a human-entered schema should fail loudly,
not silently produce wrong data. See `DataSourcePipelineFactory.fetchBqSchema()` for where
this is preferred over `fetchBeamSchema()` when a source declares a schema.

---

## SecretManagerUtils — secrets pattern

```
❌ BAD — secret in plaintext in Airflow DAG / pipeline options / parameter_store:
smtp_password = MyS3cr3tP@ss

✅ GOOD — only the secret ID travels; value fetched at runtime:
smtp_password_secret_id = projects/my-project/secrets/smtp-password/versions/latest
```

```java
// In PipelineFactory or PostDownloadFinalizeTransform's @Setup (driver JVM, not per-element):
String smtpPassword = SecretManagerUtils.fetchSecret(emailConfig.smtpPasswordSecretId);
// emailConfig is a SourceFailureEmailConfig (or similar). Pass smtpPassword directly to
// your code — never log it, never store it.
```

IAM requirement: grant `roles/secretmanager.secretAccessor` to the Dataflow and
Cloud Composer service accounts for each secret they need to access.

---

## RowValidationUtils

Use inside `@ProcessElement` for reusable validation logic:

```java
@ProcessElement
public void processElement(@Element Row row, MultiOutputReceiver out) {
    ValidationResult v = RowValidationUtils.requireFields(
        row, Set.of("order_id", "customer_email", "amount"));

    if (!v.isValid()) {
        out.get(DEAD_LETTER_TAG).output(FailedRecord.of(row,
            new IllegalArgumentException(v.errorSummary()), 0));
        return;
    }
    out.get(SUCCESS_TAG).output(row);
}
```

All methods return `ValidationResult` — they never throw. The caller decides
whether to drop, route to DLQ, or apply a default.

---

## CalendarUtils — stubs to implement

These methods are placeholders. Implement them by integrating with your calendar source:

```java
// Option A: a BigQuery table of holidays
// SELECT date FROM `my-project.config.holidays` WHERE calendar_name = @calendar

// Option B: an internal REST API
// GET https://calendar-service.internal/is-business-day?date=2024-01-15&cal=NYSE

// Option C: a Java library
// <dependency>
//     <groupId>org.threeten</groupId>
//     <artifactId>threeten-extra</artifactId>
// </dependency>
```

Once implemented, use them via `CalendarUtils.resolveEffectiveDate(options)` which
combines `--runDate`, `--businessDayOffset`, and `--calendarName` into a single date.

---

## RunDateCalculator — stub to implement

Every flow gets its dates from `RunDateCalculator.resolve()` and nothing else, so implementing
`calculateRunDates()` once changes sources, reports and PIPELINE consistently:

| Call site | Flow | What it does with the `RunDates` |
|---|---|---|
| `DataSourcePipelineFactory.assembleForConfigs()` | `DATA_SOURCE_DOWNLOAD`, datasource half of `PIPELINE` | once per source: `SourceConfig.periodId` (COMPLETED check, DaRefer `per_id`), BQ query tokens, FILE `{date}`/`{dateCompact}`/`{fileDate}`/`{periodId}` |
| `ReportPipelineFactory.execute(options)` | `REPORT_PROCESSING` | once per report: RptRefer/DaRefer `per_id`, preprocessing/transform query tokens, GCS output file names, email tokens |
| `PipelineSequenceFactory.execute()` | report half of `PIPELINE` | resolved in the driver JVM at submission, carried to the worker's report step as a DoFn field |

```java
RunDates dates = RunDateCalculator.resolve(sourceConfig.runScheduleConfig, options);
// no run_details_json → exactly --runDate/--periodStart/--periodEnd/--periodId
// run_details_json set → calculateRunDates(schedule, --runDate or today UTC)  ← implement this
```

`calculateRunDates()` must return a `RunDates` (see `beam-core/README.md` for each field's
meaning and format). Implement by combining, in order: `frequency` (which period contains the
reference date → `periodStart`/`periodEnd`/`periodId`), `dateType`/`freqRunDay`/`maxFreqRunDay`
(which date within that period is the business `runDate`, and which period the reference date is
still reporting on), or — for `DAILY` — `dayLag` counted back from the reference date instead. Any
`WD`/business-day offset resolves against whichever calendar `calendarKey` identifies — a separate
external calendar database, not the `CalendarUtils`/`--calendarName` stub above. All call sites run
in the driver JVM, so the implementation may call that database directly.

### Reports — `calculateLastPeriod()` (stub)

Reports call `RunDateCalculator.resolveForReport()` instead of `resolve()`. With a `run_details`
schedule it calls `calculateLastPeriod(schedule, asOfDate)`, which must return the **last closed
period** the report applies to — e.g. a `MONTHLY` report on 2024-02-02 → `periodStart=2024-01-01`,
`periodEnd=2024-01-31`, `periodId=202401`, `runDate=2024-02-02`.

### Data sources — run window (`calculateFreqRunDate()` / `calculateMaxFreqRunDate()`, stubs)

After resolving a source's dates, `DataSourcePipelineFactory` calls
`RunDateCalculator.checkRunWindow(schedule, dates)`. The comparison is implemented; the two
boundary dates are stubs:

| Stub | Meaning | Example (`MONTHLY`, January period) |
|---|---|---|
| `calculateFreqRunDate(schedule, dates)` | first day the period may be loaded (`freqRunDay`) | `WD+1` → first working day of February |
| `calculateMaxFreqRunDate(schedule, dates)` | last day the period may be loaded (`maxFreqRunDay`) | `5` → fifth working day of February |

`dates.runDate` before the first → `BEFORE_FREQ_RUN_DATE`; after the second →
`AFTER_MAX_FREQ_RUN_DATE`. Either way the source is **skipped** — logged, no DaRefer row, no
branch, not a failure. A bound is only checked when configured (`freqRunDay` set /
`maxFreqRunDay != -1`). This is why `runDate` must be the date the run executes as, and the
period's as-of date (`dateType`) goes in `periodEnd`.

A report and the datasources it reads must resolve to the same `periodId` — the report finds them
in `DaRefer` by its own. `PIPELINE` logs a warning at submission when they differ.

`QueryParameterResolver.resolve(template, params, options, dates)` takes the `RunDates` for its
standard tokens; the 3-arg overload uses `RunDateCalculator.fromOptions(options)`.

---

## DateUtils — common patterns

```java
// Resolve the run date (uses --runDate if set, today UTC otherwise)
LocalDate runDate = DateUtils.resolveRunDate(options);

// Date-partitioned GCS output path
String outputPath = DateUtils.partitionedPath(options.getGcsSinkPath(), runDate);
// e.g. "gs://bucket/reports/2024-01-15/"

// BigQuery sharded table
String table = DateUtils.shardedTable(options.getBqSinkTable(), runDate);
// e.g. "my-project:reports.daily_summary$20240115"

// Display in email subject
String subject = "Daily Report — " + DateUtils.toDisplayString(runDate);
// e.g. "Daily Report — 15 Jan 2024"
```

---

## MetricsUtils

Enforces consistent metric naming. Metrics appear in Dataflow UI and Cloud Monitoring:

```java
// In your DoFn (declare as fields, not local variables):
private final Counter rowsProcessed = MetricsUtils.transformCounter("my-transform", "rows_processed");
private final Counter rowsDropped   = MetricsUtils.transformCounter("my-transform", "rows_dropped");

// In @ProcessElement:
rowsProcessed.inc();
```

Standard namespace convention:
- `pipeline/rows_in` — total rows entering the pipeline
- `pipeline/rows_out` — total rows written to sink
- `pipeline/dlq_total` — total DLQ records across all transforms
- `transform.{name}/rows_dropped_*` — per-transform drops
