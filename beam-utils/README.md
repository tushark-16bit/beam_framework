# beam-utils

Shared utility helpers for transforms, the runner, and external transform modules.
Contains no Beam pipeline graph code — no `PTransform`, no `DoFn`.

---

## Utilities

| Class | Purpose |
|---|---|
| `BigQuerySchemaUtils` | Fetch real BQ table schema at pipeline-assembly time; table existence check; row count |
| `GcsUtils` | Pre-flight path checks, read/write small files, list objects, delete prefixes |
| `SecretManagerUtils` | Fetch secrets from GCP Secret Manager by secret ID (never by value) |
| `RowValidationUtils` | Stateless row-level validators: required fields, patterns, ranges, allowed values |
| `MetricsUtils` | Factory for consistently-named Beam counters, distributions, and gauges |
| `CalendarUtils` | Business calendar stubs: `isBusinessDay`, `nextBusinessDay`, `applyOffset`, etc. |
| `DateUtils` | Run date resolution, formatting (ISO/compact/display), partitioned paths, sharded BQ tables |

### DB adapter sub-package (`db/`)

| Class | Purpose |
|---|---|
| `DatabaseAdapter` | Interface for relational DB operations: `query`, `queryOne`, `update`, `close` |
| `JdbcDatabaseAdapter` | JDBC + HikariCP implementation. One pool per instance; always use try-with-resources |
| `DatabaseAdapterFactory` | Static factory: reads `--paramDb*` options, fetches password from Secret Manager |
| `ParameterRepository` | Business queries: validate required params, fetch `SourceConfig` list with full per-source config |
| `ReportRepository` | Report queries: fetch `ReportConfig` (all report tables), look up datasource output BQ table |
| `DatabaseException` | Unchecked wrapper for `SQLException` — callers don't need to declare checked exceptions |
| `QueryParameterResolver` | Resolves `{periodStart}`, `{periodEnd}`, `{periodId}`, `{runDate}` tokens in query templates |

```java
// Pattern for parameter DB access (always try-with-resources):
try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
    ParameterRepository repo = new ParameterRepository(db, options);
    if (!repo.allRequiredParametersExist(datasource, period, subprocess)) {
        List<String> missing = repo.getMissingParameters(datasource, period, subprocess);
        throw new RuntimeException("Missing: " + missing);
    }
    List<SourceConfig> configs = repo.fetchSourceConfigs(datasource, period, subprocess);
}
```

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
// instead of the generic _row_json STRING schema
```

**Never call inside a DoFn** — each worker would make a BQ API call.

---

## SecretManagerUtils — secrets pattern

```
❌ BAD — secret in plaintext in Airflow DAG / pipeline options:
--smtpPassword=MyS3cr3tP@ss

✅ GOOD — only the secret ID travels; value fetched at runtime:
--smtpPasswordSecretId=projects/my-project/secrets/smtp-password/versions/latest
```

```java
// In PipelineFactory (driver JVM, not in a DoFn):
String smtpPassword = SecretManagerUtils.fetchSecret(options.getSmtpPasswordSecretId());
// Pass smtpPassword directly to your code — never log it, never store it
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
