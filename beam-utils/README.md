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
| `RunDateCalculator` | Finance Automation scheduling rules (port of BAU): WHEN may a source/report run on the Business Date, WHICH period does it process, WHAT date ends that period. `evaluateDataSource()`/`evaluateReport()` → `ScheduleDecision` (`ELIGIBLE` + `RunDates`, or `NOT_YET_ELIGIBLE`/`EXPIRED`/`NON_BUSINESS_DAY`/`NOT_EVALUABLE`). `lookbackPeriodIds(dataSource, report, reportDates, from, to, calendar)` lists the period ids a report's per-datasource lookback must find `COMPLETED` (offset 0 = report's period, stepping in the data source's frequency; DAILY steps business days for `WD-`/blank lag, calendar days for `CAL-`; data source and report must share a frequency). See its section below |
| `BusinessCalendar` / `BusinessCalendarProvider` / `BigQueryBusinessCalendarProvider` | Calendar contract for `RunDateCalculator` (`forKey(calendarKey).isBusinessDay(date)`) and its default implementation, which reads the calendars from the parameter table (`FINACOE_Calendars`) — see "Calendar" below |
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

`src/test/java` — `RunDateCalculatorTest.java` (65 tests): the BAU examples plus ~50 further
scenarios with a fake calendar (weekends + holidays, incl. a month-end holiday): every day of a month
against a WD+3..WD+5 window, window rollover, WD-n month-end windows, WD+0, empty and single-day
windows, quarterly/annual windows, DAILY gate and lag across holidays / year end / leap day, the
non-DAILY prefix rule, previous/current periods across year boundaries, all four quarterly period
ids, every kind of month end for `lastBusDayMonth`, `dateType` ignored for non-monthly and reports,
independent report/data-source windows, every not-evaluable cause (incl. no schedule, no calendar, positive DAILY lag), `--periodId` ignored,
`--manualOverrun` not changing anything, and a sweep of 11 schedules × a year of days × both item
types asserting that every evaluable decision (skipped ones included) carries a consistent period.
`BigQueryBusinessCalendarProviderTest.java`: calendar JSON parsing (weekend/holiday forms, loud failures, caching, end-to-end through `RunDateCalculator`) without BigQuery. `DateUtilsTest.java`: Business Date from `--runDate` / `--businessTimeZone`.
`QueryParameterResolverTest.java`: standard-token resolution, step-level
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

> **The contract is [`DATE_SCHEDULING_RULES.md`](../DATE_SCHEDULING_RULES.md)** (repo root): the BAU
> description verbatim (Part 1), the owner's decisions for this framework (Part 2), and the
> assumptions still awaiting confirmation (Part 3). **Read it before changing anything here; never
> change behaviour away from it without the owner's approval; if a change or test contradicts it,
> ask the owner.** This README only describes how the code is organised.

Every flow gets its dates from here and nothing else. The rules are a port of the existing BAU
framework, kept **as is** — including its quirks (marked `BAU PARITY` in the code).

### What it answers

| Concept | Answers | From | In `RunDates` |
|---|---|---|---|
| **Business Date** | Should it run today? | `--runDate` (BAU `ManualRunDateId`), else today in `--businessTimeZone` | `runDate` |
| **Reporting Period** | Which period's data? | Business Date + `frequency` + `dayLag` | `periodStart`, `periodId` |
| **Period-end date** | What date represents the period downstream? | + `dateType` + `calendarKey` (data source, MONTHLY) | `periodEnd` |

| Attribute | WHEN? | WHICH? | Rule |
|---|:-:|:-:|---|
| `frequency` | ✓ | ✓ | `DAILY` (no window, id `yyyyMMdd`), `MONTHLY` (window, `yyyyMM`), `QUARTERLY` (window, `yyyyMMddqq`), `ANNUALLY` (window, `yyyy`). **All** frequencies also need the Business Date to be a business day (D6). Anything else, incl. `WEEKLY` → not evaluable |
| `freqRunDay` (source) / `freqDtl` (report) | ✓ | – | Window start. `WD+n` n-th business day, `WD-n` n-th-last, `WD+0` last calendar day of previous month. Ignored for DAILY |
| `maxFreqRunDay` | ✓ | – | Window end, inclusive, same `WD±n` syntax. Blank → no upper bound (eligible until the period rolls over). Ignored for DAILY |
| `dayLag` | – | ✓ | DAILY: `WD-n` n business days back, `CAL-n` n calendar days back (owner decision D4 — a positive DAILY lag is an error). Non-DAILY: starts with `WD-`/`CAL-` → current period; anything else (blank, `WD+5`, …) → previous period — the number is ignored |
| `calendarKey` | ✓ | ✓ | Required for every scheduled item. Missing/unknown → not evaluable |
| `dateType` | – | ✓ | Data source MONTHLY only: `lastBusDayMonth` → last business day; anything else → last calendar day. Reports: not used |

### Period id — calculated, not passed

`--periodId` is **not needed** and not used to run an item: `RunDates.periodId` is calculated from the
Business Date + `frequency` + `dayLag` and stored alongside the other dates; a `--periodId` passed anyway is
ignored (with a warning when it differs). Every item must have a run schedule **and** a calendar (D5) —
without them it is `NOT_EVALUABLE`, not processed. Encodings:

| Frequency | Period id | Example |
|---|---|---|
| DAILY | `yyyyMMdd` | `20260904` |
| MONTHLY | `yyyyMM` | `202608` |
| QUARTERLY | `yyyyMMddqq` — first date of the quarter + zero-padded quarter number | Q1 2026 → `2026010101`, Q3 2026 → `2026070103` |
| ANNUALLY | `yyyy` | `2025` |

### Decision flow (per item, every execution)

```
no schedule / no calendar / bad config
  / positive DAILY lag                  → NOT_EVALUABLE   not processed + FailureNotifier; others continue
Business Date not a business day (ALL
  frequencies, D6)                      → NON_BUSINESS_DAY skip; DAILY never caught up, non-DAILY runs
                                                           on the next business day inside its window
non-DAILY, before freqRunDay date       → NOT_YET_ELIGIBLE skip; picked up by a later run
non-DAILY, after maxFreqRunDay date     → EXPIRED          skip
otherwise                               → ELIGIBLE
   then (callers): item + period already COMPLETED? → skip, unless --manualOverrun
```

The period is calculated **first** and attached to every decision (`ScheduleDecision.dates`) — a
skipped item still shows the period it would have processed. Only a `NOT_EVALUABLE` decision may have
`dates == null`. Callers use the dates only when `shouldRun()`.

**`--manualOverrun` never changes eligibility or dates** (contract decision D1). It only bypasses the
COMPLETED check and makes the new run overwrite stored data. To force a re-run, pass the `--runDate` that
is eligible for the item; with `--manualOverrun` set, a date before `freqRunDay`, after `maxFreqRunDay`,
or a weekend/holiday is still skipped.

Nothing is persisted for a skip: every run re-evaluates from scratch, so a not-yet-eligible or failed
item is simply retried by a later run while its window is open (no retry limit). Every frequency needs the Business Date
to be a business day (owner decision D6, overriding Part 1 §6): a MONTHLY window covering a Saturday
does **not** run on the Saturday — it runs on the next business day still inside the window. Data sources and reports are scheduled **independently**: a skipped data source never
skips its report; the report checks its required data sources are COMPLETED when it runs, fails if not,
and is retried on a later eligible run.

| Call site | Flow |
|---|---|
| `DataSourcePipelineFactory.assembleForConfigs()` → `evaluateDataSource()` | every data source (`DATA_SOURCE_DOWNLOAD`, datasource half of `PIPELINE`) |
| `ReportPipelineFactory.execute(options)` → `evaluateReport()` | `REPORT_PROCESSING` |
| `PipelineSequenceFactory.decideReportRun()` → `evaluateReport()` | report half of `PIPELINE` (dates carried to the worker) |

### Calendar — read from the parameter table

`BigQueryBusinessCalendarProvider` (the default) fetches each calendar from the same table as the
source/report configs (`--paramBqProject` / `--paramBqDataset` / `--paramStoreTable`):

| Column | Value |
|---|---|
| `parameter_group_name` | `FINACOE_Calendars` (marks the row as a calendar) |
| `parameter_name` | the item's `calendarKey` |
| `parameters_val_json` | `[{"Calendar":{"holiday":"20260101,20270901","weekend":"saturday,sunday"}}]` |

- `holiday` — comma-separated `yyyyMMdd` dates (optional; blank = none). A malformed date fails the
  whole calendar rather than being dropped (a lost holiday would become a business day).
- `weekend` — comma-separated weekday names (`saturday,sunday`; case-insensitive, `sat`/`sun` accepted).
  **Required** — never assumed; may be blank for a calendar with no weekend.
- A date is a business day when it is not a weekend day and not a holiday.
- One query per distinct `calendarKey` per run, cached in memory; driver JVM only.
- No row, more than one row, bad JSON or a bad value → `forKey` throws → the item is `NOT_EVALUABLE`
  (not processed + failure notification), as a calendar must exist (D5).
- The three column names are constants at the top of the class (`COL_GROUP`, `COL_NAME`,
  `COL_VALUE`) — they match the columns the other repositories read from this table.
- To use a different source (external calendar service), register your own `BusinessCalendarProvider`
  via `META-INF/services/com.yourco.beam.utils.BusinessCalendarProvider`; it takes precedence.

### Open items

The assumptions where the BAU description is silent (quarter/annual window month, `WD+n` beyond the
month's business days, blank DAILY lag, case sensitivity, report `paramReplace[].dateType`, …) and the
list of assumptions is tracked in **Part 3 of
`DATE_SCHEDULING_RULES.md`** — the single list. Code comments labelled `OPEN QUESTION` point at them.

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
