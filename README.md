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
| `DATA_SOURCE_DOWNLOAD` | Fetches raw data from external sources and persists it | Parameter DB (`source_config` table) |
| `REPORT_PROCESSING` | Reads downloaded data, applies transform chain, writes reports | `--sourceType` CLI flag |

The two types are designed to be scheduled as **separate, parallel Airflow DAGs** so a download
failure never blocks a report run, and vice versa.

## Supported source types in DATA_SOURCE_DOWNLOAD

Configuration for API and file sources is stored in the parameter DB and fetched at runtime.
No code change is needed to add a new datasource — just add a row to `source_config`.

| Source type | What it reads | Key config fields in DB |
|---|---|---|
| `API` | REST API with pagination | `api_endpoint`, `api_auth_type`, `api_auth_secret_id`, `api_pagination_strategy` |
| `FILE` | CSV or Excel on GCS | `file_type`, `file_location`, `file_prefix`, `file_suffix` (support `{date}`, `{periodId}` placeholders) |
| `BQ` | BigQuery table or SQL query | `bq_project_id`, `bq_dataset`, `bq_table`, `bq_query` |

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
