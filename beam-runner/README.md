# beam-runner

Entry point only. Wires all other modules together and produces the deployable fat JAR.
You should rarely need to edit this module.

---

## What lives here

| Class | Purpose |
|---|---|
| `Main` | Parses CLI args, routes by `--processType` and `--reportName`, delegates to the right factory |
| `DataSourcePipelineFactory` | `DATA_SOURCE_DOWNLOAD`: validates params, fetches configs, creates LOADING checkpoints, assembles per-source Beam branches |
| `PostDownloadFinalizeTransform` | Final pipeline step for each `DATA_SOURCE_DOWNLOAD` source: BnC validation + checkpoint update (COMPLETED/FAILED_BNC/FAILED) + failure email, all running in the Beam worker |
| `ReportPipelineFactory` | `REPORT_PROCESSING` (DB-configured): orchestrates BQ jobs + email in driver JVM; uses `ReportCheckpointAdapter` for RptRefer/RptDaMap/RptStageDa/RptOutput tracking; writes final result to per-report BQ table (`output_bq_table` from config) if set; no Beam pipeline submitted |
| `SmtpReportEmailAdapter` | SMTP implementation of `ReportEmailAdapter`; used by `ReportPipelineFactory` and `PostDownloadFinalizeTransform` |
| `PipelineFactory` | `REPORT_PROCESSING` (legacy): assembles generic source → transform chain → sink Beam pipeline |

---

## DataSourcePipelineFactory — DATA_SOURCE_DOWNLOAD

Sources are **never merged**. Each `SourceConfig` is an independent Beam DAG branch
that reads, transforms, validates, and writes to its own output table.

```
DataSourcePipelineFactory.assemble(options)
    │
    ├─ 1. BigQuerySourceConfigRepository.fetchSourceConfigs()  (throws if row missing)
    │       Each SourceConfig carries: queryConfig, sourceTransforms, validationConfig
    │
    ├─ 2. BigQueryDataSourceCheckpointAdapter.isCompleted()  skip COMPLETED sources
    │
    ├─ 3. BigQueryDataSourceCheckpointAdapter.createCheckpoint() → dataSourceId per source (LOADING row)
    │
    └─ 4. For each SourceConfig independently (no merge!):
            a. resolveQueryTokens()                   inject {periodStart}/{periodEnd} into BQ query
            b. fetchBqSchema()                        1. BqFetchConfig.schema (operator-declared bq_schema_json)
                                                          via BigQuerySchemaUtils.toBeamSchema() — no BQ call
                                                       2. else BigQuerySchemaUtils.fetchBeamSchema() (table metadata)
                                                       3. else null → BigQuerySourceTransform resolves column names
                                                          itself via a preview query (see beam-io/README.md)
            c. SourceRouter.routeFromConfig(schema)   read raw data (typed if schema non-null)
            d. SourceTransformChainAssembler.assemble()
                   ├─ LOOKUP: BQ side input → LookupEnrichTransform
                   ├─ GROUP_BY:  GroupByTransform
                   └─ SORT_BY:   SortByTransform (per-bundle, not global)
            e. DataSourceRecordSinkTransform(daId)    rows → streaming inserts → DaRec
                   → returns PCollection<Long> (count after all inserts commit)
            f. PostDownloadFinalizeTransform(daId)    [runs in Beam worker, not driver JVM]
                   ├─ BigQueryDataSourceRecordAdapter.countRecords(daId) → storedRowCount
                   ├─ row_count_mismatch check: storedRowCount == pipelineRowCount (always-on)
                   ├─ min/max row count bounds check (optional, from config)
                   ├─ BigQueryDataSourceRecordAdapter.sumField(daId, field) per BnC rule (optional)
                   ├─ updateStatus(daId, COMPLETED/FAILED_BNC/FAILED, bncJson)
                   └─ SmtpReportEmailAdapter.send() if SourceFailureEmailConfig.isPresent()
```

The terminal checkpoint update and failure email are part of the pipeline itself. When the job reaches DONE state in Dataflow, the checkpoint has already been updated. No post-pipeline driver-JVM step is needed.

## SourceTransformChainAssembler

| Transform | What it does | Beam mechanism |
|---|---|---|
| `LOOKUP` | Left-join rows with a lookup table from BQ | Side input (`PCollectionView<Map<String,String>>`) |
| `GROUP_BY` | Group by fields + aggregate (SUM, COUNT, AVG, MIN, MAX) | `GroupByKey` + `ParDo(AggregateDoFn)` |
| `SORT_BY` | Sort within each Beam bundle (per-bundle, not global) | Buffer + sort in `@FinishBundle` |

For global ordering, use an `ORDER BY` clause in the downstream BQ view instead of `SORT_BY`.

## PipelineFactory — REPORT_PROCESSING

```
PipelineFactory.assemble(options)
    ├─ 1. fetchBqSchema()               BQ table sources: BigQuerySchemaUtils.fetchBeamSchema()
    │       null for query-only or failed fetch → BigQuerySourceTransform resolves
    │       column names itself via a preview query (see beam-io/README.md)
    ├─ 2. SourceRouter.route(schema)    reads --sourceType; typed if schema non-null
    ├─ 3. TransformRegistry + chain loop
    ├─ 4. SinkRouter.route()
    └─ 5. Flatten DLQ → DeadLetterSinkTransform
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
