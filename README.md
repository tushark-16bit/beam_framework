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
| `DATA_SOURCE_DOWNLOAD` | Fetches raw data; stores every row as JSON in `DaRec`; tracks run lifecycle in `DaRefer` | BQ `source_config` table (keyed by `parent_id`, `subprocess_name`, `datasource_name`, `period_id`) |
| `REPORT_PROCESSING` (DB-configured) | Checks `DaRefer` availability, runs BQ transform chain, routes output to GCS/BQ/API, writes `COM_CmnRptDtl`, sends email | BQ `report_config` + 5 related tables |
| `REPORT_PROCESSING` (legacy) | Source → transform chain → sink Beam pipeline | `--sourceType` CLI flag (leave `--reportName` blank) |

The two process types are designed to be scheduled as **separate, sequential Airflow DAGs**:
first `DATA_SOURCE_DOWNLOAD`, then `REPORT_PROCESSING` once all sources are `COMPLETED`.

## DATA_SOURCE_DOWNLOAD — per-source independent pipelines

Sources are **never merged**. Each source in `source_config` produces its own independent
Beam branch: read → transform chain → rows written as JSON blobs to `DaRec` (keyed by `DaId` from `DaRefer`).
Adding a new datasource requires only a BQ row in `source_config` — no code change.

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
| `LOOKUP` | Left-joins rows with a BQ lookup table. Config: BQ table ref, key fields, which output fields to merge into each row. |
| `GROUP_BY` | Groups rows by specified fields and applies aggregations: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`. |
| `SORT_BY` | Sorts rows per-bundle by specified fields. For global ordering, use a BQ view with `ORDER BY` instead. |

### Per-source validation

After the pipeline writes rows to `DaRec`, the driver JVM validates:

| Check | Configuration | Result on failure |
|---|---|---|
| Header check | `required_headers_json` | Logged at pipeline-assembly time |
| Row count | `min_row_count`, `max_row_count` | `DaRefer.StaCd = FAILED_BNC` |
| Balance & Control (BnC) | `bnc_rules_json` — field + expected sum + tolerance% | `DaRefer.StaCd = FAILED_BNC` |

### Run tracking (DaRefer)

Every run writes one row to `DaRefer` (configured via `--daReferTable`, default `DaRefer`):

| `StaCd` | Written when |
|---|---|
| `LOADING` | Before `pipeline.run()` — always |
| `COMPLETED` | Pipeline succeeded + all row-count and BnC checks passed |
| `FAILED_BNC` | Pipeline succeeded but row count outside bounds or BnC SUM exceeded tolerance |
| `FAILED` | Pipeline threw an exception |

All source rows are written to `DaRec` (configured via `--daRecTable`, default `DaRec`),
keyed by `DaId` from the `DaRefer` row. See `EXAMPLE.md` for full DDL.

## REPORT_PROCESSING — DB-configured reports

When `--reportName` is set alongside `--processType=REPORT_PROCESSING`, the
`ReportPipelineFactory` runs entirely in the driver JVM (no Dataflow job submission).

### Execution flow

```
 1. Resolve MSTR_Per for --periodId            (validate period exists)
 2. Fetch ReportConfig from BQ                 (report_config + 5 related tables)
 3. Insert DaRefer row StaCd=LOADING
 4. Run preprocessing steps                    (BQ jobs — BQ_QUERY or API_ENRICHMENT)
 5. Check datasource availability              (all required DSes must have DaRefer StaCd=COMPLETED)
 6. Build alias registry                       (alias → SELECT RowDaJsonTx FROM DaRec WHERE DaId=X)
 7. Run transformation chain                   (BQ jobs; each step materialises to a BQ table)
 8. Route each output via ReportOutputSinkRouter:
      GCS  → BQ extract job → CSV or JSON file
      BQ   → SELECT * INTO destination table (WRITE_TRUNCATE)
      API  → POST JSON array to external endpoint (auth via Secret Manager)
 9. Insert COM_CmnRptDtl row per output        (all sink types)
10. Send email                                 (GCS outputs as attachments; if configured)
11. Update DaRefer StaCd → COMPLETED / FAILED
```

### BQ config tables required

All report config tables live in the BQ dataset specified by `--paramBqProject` and `--paramBqDataset`.
See `EXAMPLE.md` for full DDL. Summary:

```sql
-- report_config: one row per (report_name, report_subprocess, period_id)
-- report_datasource_ref: which DaRec datasources are needed and under what alias
-- report_preprocessing_config: optional BQ/API enrichment before transforms
-- report_transformation_config: ordered BQ jobs; each materialises to a BQ table
-- report_email_config: SMTP recipients and message templates

-- report_output_config: one row per output — sink_type drives where the result goes
CREATE TABLE IF NOT EXISTS `my-project.pipeline_config.report_output_config` (
  report_name        STRING NOT NULL,
  report_subprocess  STRING NOT NULL,
  period_id          STRING NOT NULL,
  output_order       INT64  NOT NULL,
  input_alias        STRING NOT NULL,   -- alias whose BQ table to export
  sink_type          STRING,            -- GCS (default) | BQ | API
  -- GCS sink
  output_format      STRING,            -- CSV | JSON
  gcs_path           STRING,            -- gs://bucket/reports/
  file_prefix        STRING,
  file_suffix        STRING,
  include_header     BOOL,
  -- BQ sink
  bq_sink_table      STRING,            -- project.dataset.table to WRITE_TRUNCATE into
  -- API sink
  api_endpoint       STRING,            -- target URL
  api_method         STRING,            -- POST | PUT (default POST)
  api_auth_secret_id STRING,            -- Secret Manager ID for Bearer token
  api_headers_json   STRING             -- {"X-Custom-Header": "value"}
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
    "--processType":          "REPORT_PROCESSING",
    "--parentId":             "TRADING",          # → ParameterGroupName in parameter_store
    "--reportName":           "daily_trades_summary",
    "--reportSubprocess":     "eod",
    "--periodId":             "202401",           # MONTHLY format YYYYMM (must exist in MSTR_Per)
    "--periodStart":          "2024-01-01",
    "--periodEnd":            "2024-01-31",
    "--runDate":              "{{ ds }}",
    "--paramBqProject":       "my-gcp-project",
    "--paramBqDataset":       "pipeline_config",
    "--checkpointBqDataset":  "pipeline_metadata",
    "--daReferTable":         "DaRefer",
    "--daRecTable":           "DaRec",
    "--cmnRptDtlTable":       "COM_CmnRptDtl",
    # --sinkType is NOT required — output routing comes from report_output_config.sink_type
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
