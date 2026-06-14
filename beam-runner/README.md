# beam-runner

Entry point only. Wires all other modules together and produces the deployable fat JAR.
You should rarely need to edit this module.

---

## What lives here

| Class | Purpose |
|---|---|
| `Main` | Parses CLI args, routes by `--processType` and `--reportName`, delegates to the right factory |
| `DataSourcePipelineFactory` | `DATA_SOURCE_DOWNLOAD`: validates params, fetches configs, assembles per-source Beam branches |
| `ReportPipelineFactory` | `REPORT_PROCESSING` (DB-configured): orchestrates BQ jobs + email in driver JVM; no Beam pipeline submitted |
| `SmtpReportEmailAdapter` | SMTP implementation of `ReportEmailAdapter`; used by `ReportPipelineFactory` |
| `PipelineFactory` | `REPORT_PROCESSING` (legacy): assembles generic source → transform chain → sink Beam pipeline |

---

## DataSourcePipelineFactory — DATA_SOURCE_DOWNLOAD

Sources are **never merged**. Each `SourceConfig` is an independent Beam DAG branch
that reads, transforms, validates, and writes to its own output table.

```
DataSourcePipelineFactory.assemble(options)
    │
    ├─ 1. DatabaseAdapterFactory.create()  JDBC + Secret Manager (open → fetch → close)
    ├─ 2. ParameterRepository.validate()  fail fast if source config missing
    ├─ 3. ParameterRepository.fetchSourceConfigs()
    │       Each SourceConfig now also carries: outputConfig, queryConfig,
    │       sourceTransforms (LOOKUP/GROUP_BY/SORT_BY), validationConfig
    │       DB closed here.
    │
    ├─ 4. BigQueryCheckpointAdapter.isDownloadComplete()  skip finished sources
    │
    ├─ 5. Write STARTED checkpoints + PENDING process_status rows → BQ
    │
    ├─ 6. For each SourceConfig independently (no merge!):
    │       a. SourceRouter.routeFromConfig()         read raw data
    │       b. QueryParameterResolver                 inject {periodStart}/{periodEnd}
    │       c. SourceTransformChainAssembler.assemble()
    │              ├─ LOOKUP: load lookup table (JDBC/BQ) → side input → LookupEnrichTransform
    │              ├─ GROUP_BY:  GroupByTransform
    │              └─ SORT_BY:   SortByTransform (per-bundle, not global)
    │       d. Per-source sink (BigQueryIO.writeTableRows() or GcsSinkTransform)
    │
    └─ 7. Return pipeline (run() called by Main)

After pipeline.run() + waitUntilFinish():
    DataSourcePipelineFactory.runPostPipelineSteps()
    ├─ On success: for each source
    │    ├─ Query row count from output BQ table (COUNT(*))
    │    ├─ Query sums for each BnC rule (SUM(field))
    │    ├─ Compare against ValidationConfig bounds
    │    └─ Write COMPLETED or VALIDATION_FAILED → process_status + checkpoint BQ tables
    └─ On failure: write FAILED → process_status + checkpoint BQ tables
```

## SourceTransformChainAssembler

| Transform | What it does | Beam mechanism |
|---|---|---|
| `LOOKUP` | Left-join rows with a lookup table from BQ or JDBC param DB | Side input (`PCollectionView<Map<String,String>>`) |
| `GROUP_BY` | Group by fields + aggregate (SUM, COUNT, AVG, MIN, MAX) | `GroupByKey` + `ParDo(AggregateDoFn)` |
| `SORT_BY` | Sort within each Beam bundle (per-bundle, not global) | Buffer + sort in `@FinishBundle` |

For global ordering, use an `ORDER BY` clause in the downstream BQ view instead of `SORT_BY`.

## PipelineFactory — REPORT_PROCESSING

```
PipelineFactory.assemble(options)
    ├─ 1. SourceRouter.route()          reads --sourceType
    ├─ 2. TransformRegistry + chain loop
    ├─ 3. SinkRouter.route()
    └─ 4. Flatten DLQ → DeadLetterSinkTransform
```

No data moves during assembly — it only describes the computation graph.

---

## Building the fat JAR

```bash
# From project root — builds all modules and produces the deployable JAR
mvn package -pl beam-runner -am -DskipTests

# Output:
# beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar
```

The `maven-shade-plugin` in `beam-runner/pom.xml` does two critical things:
1. Bundles all dependencies into one JAR for Dataflow to execute
2. **`ServicesResourceTransformer`** merges all `META-INF/services/` files from all JARs
   so the SPI registry sees transforms from every module

---

## Running locally (DirectRunner)

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --sourceType=GCS \
  --gcsSourcePath=gs://my-bucket/input/*.json \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=GCS \
  --gcsSinkPath=gs://my-bucket/output/ \
  --deadLetterSink=gs://my-bucket/dlq/ \
  --piiFields=email,phone
```

---

## Running on Dataflow

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DataflowRunner \
  --project=my-gcp-project \
  --region=us-central1 \
  --tempLocation=gs://my-bucket/temp \
  --sourceType=BQ \
  --bqSourceTable=my-project:my-dataset.orders \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=BQ \
  --bqSinkTable=my-project:my-dataset.orders_clean \
  --writeDisposition=TRUNCATE \
  --retryPolicy=EXPONENTIAL \
  --maxRetries=3 \
  --deadLetterSink=gs://my-bucket/dlq/ \
  --runDate=2024-01-15 \
  --calendarName=NYSE \
  --businessEmail=reports@company.com \
  --devErrorEmail=oncall@company.com \
  --smtpPasswordSecretId=projects/my-project/secrets/smtp-pass/versions/latest
```

---

## Streaming mode (Pub/Sub)

```bash
java -jar beam-runner-bundled.jar \
  --runner=DataflowRunner \
  --sourceType=PUBSUB \
  --pubSubSubscription=projects/my-project/subscriptions/my-sub \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=PUBSUB \
  --pubSubTopic=projects/my-project/topics/clean-output \
  --deadLetterSink=gs://my-bucket/dlq/
```

For streaming jobs, `Main` does NOT call `waitUntilFinish()` — the job runs
indefinitely until cancelled in the Dataflow console or via:
```bash
gcloud dataflow jobs cancel JOB_ID --region=us-central1
```

---

## Adding beam-utils or other modules to the fat JAR

If you add a new module that `beam-runner` needs, add it to `beam-runner/pom.xml`:
```xml
<dependency>
    <groupId>com.yourco.beam</groupId>
    <artifactId>beam-utils</artifactId>
</dependency>
```

The shade plugin will include it automatically. No other changes needed.
