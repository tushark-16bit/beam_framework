# Beam Pipeline Framework

A configurable, plug-and-play Apache Beam ETL pipeline framework for GCP Dataflow,
triggered by Apache Airflow (Cloud Composer). Supports BigQuery, GCS, and Pub/Sub.
Written in Java 17.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Apache Airflow (Cloud Composer)                            │
│  DataflowStartJobOperator  +  dag_run.conf JSON             │
│  "--sourceType=BQ --transformChain=filter-nulls,mask-pii"   │
└──────────────────────────────┬──────────────────────────────┘
                               │ submit fat JAR
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  GCP Dataflow                                               │
│                                                             │
│  Source ──► Transform Chain ──► Sink                        │
│  (BQ/GCS/   (filter-nulls,      (BQ/GCS/                   │
│  PubSub)     mask-pii, ...)      PubSub)                   │
│                    │                                        │
│                    └──► Dead-Letter Sink (GCS)              │
└─────────────────────────────────────────────────────────────┘
```

## Module structure

```
beam-pipeline-framework/
├── beam-core/        Pure contracts — options, transform SPI, retry, models
├── beam-io/          Source/sink connectors — BQ, GCS, Pub/Sub, DLQ
├── beam-utils/       Shared utilities — schema, calendar, date, secrets, metrics
├── beam-transforms/  Built-in transform library + extension point
└── beam-runner/      Entry point — wires everything, produces fat JAR
```

Dependency direction (one-way only):
```
beam-runner → beam-core, beam-io, beam-utils, beam-transforms
beam-transforms → beam-core, beam-utils
beam-io → beam-core
beam-utils → beam-core
beam-core → (nothing internal)
```

---

## Quick start

### Prerequisites
- Java 17+
- Maven 3.8+
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- GCP project with Dataflow, BigQuery, GCS APIs enabled

### Build

```bash
git clone <your-repo-url>
cd beam-pipeline-framework

# Build all modules and produce the deployable fat JAR
mvn package -pl beam-runner -am -DskipTests

# Output:
# beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar
```

### Run locally (DirectRunner — no GCP needed)

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --sourceType=GCS \
  --gcsSourcePath=gs://my-bucket/input/*.json \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=GCS \
  --gcsSinkPath=gs://my-bucket/output/ \
  --deadLetterSink=gs://my-bucket/dlq/ \
  --piiFields=email,phone,ssn
```

### Run on Dataflow (batch)

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DataflowRunner \
  --project=my-gcp-project \
  --region=us-central1 \
  --tempLocation=gs://my-bucket/temp \
  --sourceType=BQ \
  --bqSourceTable=my-project:my-dataset.raw_orders \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=BQ \
  --bqSinkTable=my-project:my-dataset.clean_orders \
  --writeDisposition=TRUNCATE \
  --retryPolicy=EXPONENTIAL \
  --maxRetries=3 \
  --deadLetterSink=gs://my-bucket/dlq/ \
  --runDate=2024-01-15 \
  --calendarName=NYSE \
  --businessEmail=reports@company.com \
  --devErrorEmail=oncall@company.com
```

### Run from Airflow

```python
from airflow.providers.google.cloud.operators.dataflow import DataflowStartJobOperator

dataflow_task = DataflowStartJobOperator(
    task_id="run_etl_pipeline",
    project_id="{{ var.value.gcp_project }}",
    location="us-central1",
    jar="gs://{{ var.value.jar_bucket }}/beam-runner-1.0.0-SNAPSHOT-bundled.jar",
    job_name="etl-pipeline-{{ ds_nodash }}",
    options={
        "--runner":           "DataflowRunner",
        "--project":          "{{ var.value.gcp_project }}",
        "--region":           "us-central1",
        "--tempLocation":     "gs://{{ var.value.temp_bucket }}/temp",
        "--sourceType":       "{{ dag_run.conf['sourceType'] }}",
        "--bqSourceTable":    "{{ dag_run.conf.get('bqSourceTable', '') }}",
        "--transformChain":   "{{ dag_run.conf['transformChain'] }}",
        "--sinkType":         "{{ dag_run.conf['sinkType'] }}",
        "--bqSinkTable":      "{{ dag_run.conf.get('bqSinkTable', '') }}",
        "--writeDisposition": "{{ dag_run.conf.get('writeDisposition', 'TRUNCATE') }}",
        "--retryPolicy":      "{{ dag_run.conf.get('retryPolicy', 'EXPONENTIAL') }}",
        "--maxRetries":       "{{ dag_run.conf.get('maxRetries', '3') }}",
        "--deadLetterSink":   "gs://{{ var.value.dlq_bucket }}/{{ dag_run.conf['jobName'] }}/",
        "--runDate":          "{{ ds }}",   # Airflow execution date
        "--calendarName":     "{{ dag_run.conf.get('calendarName', 'DEFAULT') }}",
        "--businessEmail":    "{{ var.value.business_email }}",
        "--devErrorEmail":    "{{ var.value.dev_error_email }}",
    }
)
```

Trigger with different configs per run:
```json
{
  "sourceType": "BQ",
  "bqSourceTable": "my-project:finance.daily_transactions",
  "transformChain": "filter-nulls,mask-pii",
  "sinkType": "BQ",
  "bqSinkTable": "my-project:finance.clean_transactions",
  "calendarName": "NYSE"
}
```

---

## How to configure behaviour (no code changes needed)

Everything the pipeline does is controlled by CLI flags passed from Airflow.
Change `dag_run.conf` JSON to change pipeline behaviour:

| What to change | Flag(s) to set |
|---|---|
| Read source | `--sourceType`, `--bqSourceTable`, `--gcsSourcePath`, `--pubSubSubscription` |
| Write destination | `--sinkType`, `--bqSinkTable`, `--gcsSinkPath`, `--pubSubTopic` |
| Which transforms run | `--transformChain=a,b,c` |
| Which fields to mask | `--piiFields=email,phone,tax_id` |
| Retry behaviour | `--retryPolicy`, `--maxRetries`, `--retryDelayMs` |
| Failed record destination | `--deadLetterSink` |
| Report date | `--runDate=2024-01-15` (ISO-8601) |
| Business calendar | `--calendarName=NYSE` |
| Notification email | `--businessEmail`, `--devErrorEmail` |
| BQ idempotency | `--writeDisposition=TRUNCATE` (safe re-run) or `APPEND` |

---

## How to add a new transform (without touching the framework)

1. Create a class implementing `BeamTransform` in any Maven module or separate project
2. Add one line to `META-INF/services/com.yourco.beam.transform.BeamTransform`
3. Include the JAR in the fat JAR (Maven dependency) or pass via `--customTransformJarPath`
4. Use the transform name in `--transformChain`

See `beam-transforms/README.md` for a complete step-by-step example with code.

---

## How to add a new source or sink

1. Create a class in `beam-io` extending `PTransform<PBegin, PCollection<Row>>`
2. Add a value to `SourceType` / `SinkType` enum in `beam-core`
3. Add a case in `SourceRouter` / `SinkRouter` (exhaustive switch — compiler enforces it)
4. Add required options to `FrameworkOptions`

---

## How to use calendar utilities in a report pipeline

The calendar utilities in `beam-utils` are stubs — implement them for your environment:

```java
// beam-utils/src/main/java/com/yourco/beam/utils/CalendarUtils.java
public static boolean isBusinessDay(LocalDate date, String calendarName) {
    // TODO: integrate with your holiday service / BQ holiday table
    throw new UnsupportedOperationException("Not yet implemented");
}
```

Once implemented, use them via `CalendarUtils.resolveEffectiveDate(options)` which
combines `--runDate`, `--businessDayOffset`, and `--calendarName` automatically.

---

## How to handle secrets

**Never pass secrets as pipeline options** — they appear in Dataflow job metadata and logs.

```
✅ Correct pattern:
  1. Store secret in GCP Secret Manager
  2. Pass only the secret ID: --smtpPasswordSecretId=projects/p/secrets/smtp/versions/latest
  3. Fetch at runtime: SecretManagerUtils.fetchSecret(options.getSmtpPasswordSecretId())
```

Grant `roles/secretmanager.secretAccessor` to the Dataflow + Cloud Composer service accounts.

---

## Agent / AI guide

[`CLAUDE.md`](CLAUDE.md) is the primary reference for any AI agent working in this repository.
It covers: file reading order, the mandatory README update rule, the complete file map, all
architecture rules, serialization rules, how to make every type of change, and what never to do.
It is written in plain language and is readable by any capable language model (Claude, GPT, Gemini, etc).

---

## Project layout deep-dive

Each module has its own `README.md` explaining its internals:

- [`beam-core/README.md`](beam-core/README.md) — options, SPI interface, serialization rules
- [`beam-io/README.md`](beam-io/README.md) — connectors, schema contract, write dispositions
- [`beam-transforms/README.md`](beam-transforms/README.md) — built-in transforms, extension guide
- [`beam-utils/README.md`](beam-utils/README.md) — utility API reference, calendar stubs, secrets
- [`beam-runner/README.md`](beam-runner/README.md) — build, run locally, run on Dataflow

---

## Setting up git and pushing to a remote

```bash
cd /Users/tushark/IdeaProjects/beam-pipeline-framework

# Initialise git
git init
git add .
git commit -m "Initial commit: beam-pipeline-framework"

# Add your remote (GitHub, GitLab, Bitbucket, Cloud Source Repositories, etc.)
git remote add origin https://github.com/YOUR_ORG/beam-pipeline-framework.git

# Push
git push -u origin main
```

### Using with Cloud Source Repositories (GCP-native)

```bash
# Create the repo in GCP
gcloud source repos create beam-pipeline-framework --project=my-gcp-project

# Add as remote
git remote add google \
  https://source.developers.google.com/p/my-gcp-project/r/beam-pipeline-framework

git push google main
```

### Using elsewhere (another machine or CI)

```bash
# Clone
git clone https://github.com/YOUR_ORG/beam-pipeline-framework.git
cd beam-pipeline-framework

# Build
mvn package -pl beam-runner -am -DskipTests

# Upload JAR to GCS so Dataflow can access it
gsutil cp beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  gs://my-bucket/jars/beam-runner-1.0.0-SNAPSHOT-bundled.jar
```

### CI/CD pipeline (Cloud Build example)

```yaml
# cloudbuild.yaml
steps:
  - name: 'maven:3.9-eclipse-temurin-17'
    entrypoint: mvn
    args: ['package', '-pl', 'beam-runner', '-am', '-DskipTests']

  - name: 'gcr.io/cloud-builders/gsutil'
    args: ['cp',
           'beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar',
           'gs://${_JAR_BUCKET}/jars/beam-runner-${SHORT_SHA}.jar']

substitutions:
  _JAR_BUCKET: my-artifact-bucket
```

---

## Process types

| `--processType` | What runs | Source config from |
|---|---|---|
| `DATA_SOURCE_DOWNLOAD` | Fetches raw data from external sources and persists it per-source | Parameter DB (`source_config` table) |
| `REPORT_PROCESSING` (DB-configured) | Checks datasource availability, runs BQ transformation chain, exports files, sends email | Parameter DB (`report_config` + related tables) |
| `REPORT_PROCESSING` (legacy) | Source → transform chain → sink Beam pipeline | `--sourceType` CLI flag (leave `--reportName` blank) |

The two process types are designed to be scheduled as **separate, sequential Airflow DAGs**:
first `DATA_SOURCE_DOWNLOAD`, then `REPORT_PROCESSING` once all sources are `COMPLETED`.

## DATA_SOURCE_DOWNLOAD — per-source independent pipelines

Sources are **never merged**. Each source in `source_config` produces its own independent
Beam branch: read → transform chain → per-source output table. Adding a new datasource
requires only a DB row — no code change.

### Supported source types

| Source type | What it reads | Key config fields |
|---|---|---|
| `API` | REST API with pagination | `api_endpoint`, `api_auth_type`, `api_auth_secret_id`, `api_pagination_strategy` |
| `FILE` | CSV or Excel on GCS | `file_type`, `file_location`, `file_prefix`, `file_suffix` (support `{date}`, `{periodId}` placeholders) |
| `BQ` | BigQuery table or SQL query | `bq_project_id`, `bq_dataset`, `bq_table`, `bq_query` (may contain `{periodStart}`, `{periodEnd}`, `{periodId}` tokens) |

### Query parameter injection

Pass `--periodStart=2024-01-01` and `--periodEnd=2024-01-31` as pipeline options. These are
injected into the `bq_query` template at runtime via `QueryParameterResolver`:

```sql
-- In source_config.bq_query:
SELECT * FROM trades WHERE trade_date BETWEEN '{periodStart}' AND '{periodEnd}'
```

Additional named params go in `source_config.query_params_json`:
```json
{"startDate": "{periodStart}", "exchange": "NYSE"}
```

### Per-source transform chain

Each source can have an ordered list of transforms stored in `source_transforms_json`:

| Transform type | What it does |
|---|---|
| `LOOKUP` | Left-joins rows with a lookup table (from BQ or param JDBC DB). Config: lookup source, key fields, which output fields to merge. |
| `GROUP_BY` | Groups rows by specified fields and applies aggregations: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`. |
| `SORT_BY` | Sorts rows per-bundle by specified fields. For global ordering, use a BQ view with `ORDER BY` instead. |

### Per-source validation

After the pipeline writes data, the driver JVM validates against the output table:

| Check | Configuration | Result on failure |
|---|---|---|
| Header check | `required_headers_json` | Logged at pipeline-assembly time |
| Row count | `min_row_count`, `max_row_count` | `VALIDATION_FAILED` status |
| Balance & Control (BnC) | `bnc_rules_json` — field + expected sum + tolerance% | `VALIDATION_FAILED` status |

### Process status tracking

Every fetched source writes a row to the `process_status` BQ table
(configured via `--processStatusBqDataset`, `--processStatusBqTable`):

| Status | Written when |
|---|---|
| `PENDING` | Before `pipeline.run()` |
| `COMPLETED` | Pipeline succeeded + all validation checks passed |
| `VALIDATION_FAILED` | Pipeline succeeded but row count or BnC check failed |
| `FAILED` | Pipeline threw an exception |

Create the table before first run:
```sql
CREATE TABLE pipeline_metadata.process_status (
  job_run_id STRING, process_type STRING, datasource_name STRING,
  subprocess_name STRING, period_id STRING, period_start STRING, period_end STRING,
  status STRING, row_count INT64, error_message STRING, validation_details STRING,
  started_at TIMESTAMP, completed_at TIMESTAMP
) PARTITION BY DATE(started_at);
```

## REPORT_PROCESSING — DB-configured reports

When `--reportName` is set alongside `--processType=REPORT_PROCESSING`, the
`ReportPipelineFactory` runs entirely in the driver JVM (no Dataflow job submission).

### Execution flow

```
1. Fetch ReportConfig from parameter DB  (report_config + related tables)
2. Write PENDING status                  (process_status BQ table)
3. Run preprocessing steps               (BQ jobs — BQ_QUERY or API_ENRICHMENT)
4. Check datasource availability         (all required DSes must be COMPLETED)
5. Build alias registry                  (alias → BQ output table of each datasource)
6. Run transformation chain              (BQ jobs; each step materialises to a BQ table)
7. Export outputs to GCS                 (BQ extract jobs → CSV or JSON files)
8. Send email                            (SMTP; GCS files attached as InputStream)
9. Write COMPLETED / FAILED status
```

### DB tables required (create before first run)

```sql
CREATE TABLE report_config (
  report_name       VARCHAR(100) NOT NULL,
  report_subprocess VARCHAR(100) NOT NULL DEFAULT 'default',
  period_id         VARCHAR(50)  NOT NULL,
  override_key      BOOLEAN      DEFAULT FALSE,
  PRIMARY KEY (report_name, report_subprocess, period_id)
);

CREATE TABLE report_datasource_ref (
  report_name           VARCHAR(100) NOT NULL,
  report_subprocess     VARCHAR(100) NOT NULL DEFAULT 'default',
  period_id             VARCHAR(50)  NOT NULL,
  datasource_name       VARCHAR(100) NOT NULL,
  datasource_subprocess VARCHAR(100) NOT NULL DEFAULT 'default',
  transform_alias       VARCHAR(100) NOT NULL,  -- used as {alias} in query templates
  is_required           BOOLEAN      DEFAULT TRUE,
  PRIMARY KEY (report_name, report_subprocess, period_id, datasource_name, datasource_subprocess)
);

CREATE TABLE report_preprocessing_config (
  report_name       VARCHAR(100) NOT NULL,
  report_subprocess VARCHAR(100) NOT NULL DEFAULT 'default',
  period_id         VARCHAR(50)  NOT NULL,
  step_order        INT NOT NULL,
  step_type         VARCHAR(30)  NOT NULL,  -- BQ_QUERY | API_ENRICHMENT
  step_name         VARCHAR(200),
  bq_query          TEXT,
  bq_output_table   VARCHAR(500),
  api_endpoint      TEXT,
  api_params_json   TEXT,
  PRIMARY KEY (report_name, report_subprocess, period_id, step_order)
);

CREATE TABLE report_transformation_config (
  report_name       VARCHAR(100) NOT NULL,
  report_subprocess VARCHAR(100) NOT NULL DEFAULT 'default',
  period_id         VARCHAR(50)  NOT NULL,
  step_order        INT NOT NULL,
  step_name         VARCHAR(200),
  input_alias       VARCHAR(100) NOT NULL,
  output_alias      VARCHAR(100) NOT NULL,
  query_template    TEXT,         -- SQL; {alias} → BQ table ref, {periodStart} etc.
  output_bq_table   VARCHAR(500), -- project.dataset.table
  PRIMARY KEY (report_name, report_subprocess, period_id, step_order)
);

CREATE TABLE report_output_config (
  report_name       VARCHAR(100) NOT NULL,
  report_subprocess VARCHAR(100) NOT NULL DEFAULT 'default',
  period_id         VARCHAR(50)  NOT NULL,
  output_order      INT NOT NULL,
  input_alias       VARCHAR(100) NOT NULL,
  output_format     VARCHAR(20)  NOT NULL,  -- CSV | JSON
  gcs_path          TEXT         NOT NULL,
  file_prefix       VARCHAR(200),
  file_suffix       VARCHAR(200),
  include_header    BOOLEAN      DEFAULT TRUE,
  PRIMARY KEY (report_name, report_subprocess, period_id, output_order)
);

CREATE TABLE report_email_config (
  report_name       VARCHAR(100) NOT NULL,
  report_subprocess VARCHAR(100) NOT NULL DEFAULT 'default',
  period_id         VARCHAR(50)  NOT NULL,
  to_list           TEXT         NOT NULL,  -- comma-separated or JSON array
  cc_list           TEXT,
  subject_template  TEXT,  -- tokens: {reportName} {reportSubprocess} {periodId} {periodStart} {periodEnd} {runDate}
  body_template     TEXT,
  PRIMARY KEY (report_name, report_subprocess, period_id)
);
```

### Query template example

```sql
-- report_transformation_config.query_template
SELECT
  t.trade_id,
  t.amount * f.rate AS amount_usd,
  t.trade_date
FROM {trades} t
JOIN {fx_rates} f ON t.currency = f.currency_code
WHERE t.trade_date BETWEEN '{periodStart}' AND '{periodEnd}'
```

`{trades}` and `{fx_rates}` are `transform_alias` values from `report_datasource_ref`.
They resolve to `` `project.dataset.table` `` BQ standard SQL references at runtime.

### Trigger from Airflow

```python
options={
    "--processType":       "REPORT_PROCESSING",
    "--reportName":        "daily_trades_report",
    "--reportSubprocess":  "eod",
    "--periodId":          "2024-01",
    "--periodStart":       "2024-01-01",
    "--periodEnd":         "2024-01-31",
    "--runDate":           "{{ ds }}",
    "--paramDbUrl":        "jdbc:postgresql://...",
    # ... other standard options
}
```

## Built-in transforms reference

| Token | Options | Description |
|---|---|---|
| `filter-nulls` | none | Drops rows with any null field; routes to DLQ with metrics |
| `mask-pii` | `--piiFields=email,phone,...` | SHA-256 hashes listed fields |
| `enrich-from-api` | (sample only) | Demonstrates `@Setup`/`@Teardown` lifecycle for HTTP clients |

## Side-effect transforms (parallel pipeline branches)

| Class | Produces | Use for |
|---|---|---|
| `SideEffectEmailTransform` | `PDone` | SMTP notifications (success/failure summary emails) |
| `SideEffectDbWriteTransform` | `PDone` | Writing audit logs or status updates back to the parameter DB |
