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
| `RunDateCalculator` | Finance Automation scheduling rules (port of BAU): WHEN may a source/report run on the Business Date, WHICH period does it process, WHAT date ends that period. `evaluateDataSource()`/`evaluateReport()` → `ScheduleDecision` (`ELIGIBLE` + `RunDates`, or `NOT_YET_ELIGIBLE`/`EXPIRED`/`NON_BUSINESS_DAY`/`NOT_EVALUABLE`). See its section below |
| `BusinessCalendar` / `BusinessCalendarProvider` | Calendar contract for `RunDateCalculator`: `forKey(calendarKey).isBusinessDay(date)`. **No implementation ships** — register yours via `META-INF/services/com.yourco.beam.utils.BusinessCalendarProvider` |
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

`src/test/java` — `RunDateCalculatorTest.java`: BAU parity tests with a fake calendar — one per BAU
example (WD+3..WD+5 window inclusive, expired, ManualForceRun bypass rules, WD+0/WD-1, DAILY
business-day gate, DAILY WD/CAL lag incl. the 2026-09-08 holiday example, non-DAILY prefix rule,
quarterly/annual periods, lastBusDayMonth vs lastDayMonth, report ignoring dateType, not-evaluable
configs). `QueryParameterResolverTest.java`: standard-token resolution, step-level
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

## RunDateCalculator — Finance Automation scheduling (BAU port)

Every flow gets its dates from here and nothing else. The rules are a port of the existing BAU
framework, kept **as is** — including its quirks (marked `BAU PARITY` in the code). Points the
BAU description didn't pin down are marked `OPEN QUESTION` in the code and listed at the end of
this section.

### Two dates, three questions

| Concept | Answers | From | In `RunDates` |
|---|---|---|---|
| **Business Date** | Should it run today? | `--runDate` (BAU `ManualRunDateId`), else today in `--businessTimeZone` | `runDate` |
| **Reporting Period** | Which period's data? | Business Date + `frequency` + `dayLag` | `periodStart`, `periodId` |
| **Period-end date** | What date represents the period downstream? | + `dateType` + `calendarKey` (data source, MONTHLY) | `periodEnd` |

| Attribute | WHEN? | WHICH? | Rule |
|---|:-:|:-:|---|
| `frequency` | ✓ | ✓ | `DAILY` (business-day gate, `yyyyMMdd`), `MONTHLY` (window, `yyyyMM`), `QUARTERLY` (window, `yyyy*10+q`), `ANNUALLY` (window, `yyyy`). Anything else, incl. `WEEKLY` → not evaluable |
| `freqRunDay` (source) / `freqDtl` (report) | ✓ | – | Window start. `WD+n` n-th business day, `WD-n` n-th-last, `WD+0` last calendar day of previous month. Ignored for DAILY |
| `maxFreqRunDay` | ✓ | – | Window end, inclusive, same `WD±n` syntax. Blank → no upper bound (eligible until the period rolls over). Ignored for DAILY |
| `dayLag` | – | ✓ | DAILY: `WD+n` n business days back, `CAL+n` n calendar days back. Non-DAILY: starts with `WD-`/`CAL-` → current period; anything else (blank, `WD+5`, …) → previous period — the number is ignored |
| `calendarKey` | ✓ | ✓ | Required for every scheduled item. Missing/unknown → not evaluable |
| `dateType` | – | ✓ | Data source MONTHLY only: `lastBusDayMonth` → last business day; anything else → last calendar day. Reports: not used |

### Decision flow (per item, every execution)

```
no run schedule                         → ELIGIBLE, dates = CLI flags (unchanged behaviour)
bad frequency / calendar / WD / lag     → NOT_EVALUABLE   skip + FailureNotifier; others continue
DAILY, Business Date not a business day → NON_BUSINESS_DAY skip; never caught up
non-DAILY, before freqRunDay date       → NOT_YET_ELIGIBLE skip; picked up by a later run
non-DAILY, after maxFreqRunDay date     → EXPIRED          skip — unless --manualOverrun
otherwise                               → ELIGIBLE → RunDates
   then (callers): item + period already COMPLETED? → skip, unless --manualOverrun
```

Nothing is persisted for a skip: every run re-evaluates from scratch, so a not-yet-eligible or
failed item is simply retried by a later run while its window is open (no retry limit).
`--manualOverrun` (BAU `ManualForceRun`) bypasses the max-window and COMPLETED checks only — never
the `freqRunDay` check or the DAILY business-day check. Non-DAILY items do **not** need the Business
Date itself to be a business day (a MONTHLY window covering a Saturday runs on Saturday).

Data sources and reports are scheduled **independently**: a skipped data source never skips its
report; the report checks its required data sources are COMPLETED when it runs, fails if not, and
is retried on a later eligible run.

| Call site | Flow |
|---|---|
| `DataSourcePipelineFactory.assembleForConfigs()` → `evaluateDataSource()` | every data source (`DATA_SOURCE_DOWNLOAD`, datasource half of `PIPELINE`) |
| `ReportPipelineFactory.execute(options)` → `evaluateReport()` | `REPORT_PROCESSING` |
| `PipelineSequenceFactory.decideReportRun()` → `evaluateReport()` | report half of `PIPELINE` (dates carried to the worker) |

### Calendar — the one thing you implement

```java
public final class CalendarDbProvider implements BusinessCalendarProvider {
    @Override public BusinessCalendar forKey(String calendarKey) {
        Set<LocalDate> holidays = /* load from calendar DB, cache per key */;
        return date -> date.getDayOfWeek() != SATURDAY && date.getDayOfWeek() != SUNDAY
                       && !holidays.contains(date);        // weekend rules are the calendar's call
    }
}
// META-INF/services/com.yourco.beam.utils.BusinessCalendarProvider:
//   com.yourorg.CalendarDbProvider
```

Without a registered provider, every **scheduled** item is `NOT_EVALUABLE` (skipped, reported);
unscheduled items don't touch the calendar.

### OPEN QUESTIONS — confirm against BAU

1. **QUARTERLY/ANNUALLY window month** — BAU "quarter/annual month lookup" wasn't described.
   Assumed: WD days counted in the first month of the Business Date's quarter / January.
2. **QUARTERLY periodId** — assumed `yyyy*10+q` (Q1 2026 → `20261`).
3. **`WD+n` beyond the month's business days** — assumed the count carries into the next month
   (and `WD-n` into the previous) rather than failing.
4. **DAILY blank `dayLag`** — assumed lag 0 (period = Business Date).
5. **DAILY `WD-n`/`CAL-n`** — BAU only describes `+n`; assumed the magnitude is counted back.
6. **Blank `freqRunDay`/`freqDtl` on a non-DAILY item** — assumed no lower bound.
7. **Case/whitespace** — `WD±n`, `WD-`/`CAL-` prefixes, `lastBusDayMonth` and frequencies are
   matched exactly (case-sensitive). `CD+n` (used in earlier docs here) is not a BAU form.
8. **Report `paramReplace[].dateType`** (`periodId` / `RunDate` / period end + `periodOffset`) —
   not implemented; its config shape and `periodOffset` unit weren't described.
9. **Report ↔ datasource period resolution** — a report looks its datasources up by its own
   `periodId`; a report whose datasources have a different frequency isn't handled.
10. **`--overrideDownload`** still bypasses only the COMPLETED check; only `--manualOverrun` maps
    to `ManualForceRun`.

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
