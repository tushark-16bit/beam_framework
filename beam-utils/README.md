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
