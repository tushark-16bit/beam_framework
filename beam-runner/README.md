# beam-runner

Entry point only. Wires all other modules together and produces the deployable fat JAR.
You should rarely need to edit this module.

---

## What lives here

| Class | Purpose |
|---|---|
| `Main` | Parses CLI args, routes by `--processType`, runs the pipeline, writes post-run checkpoints |
| `DataSourcePipelineFactory` | `DATA_SOURCE_DOWNLOAD`: validates params, fetches configs, assembles parallel sources |
| `PipelineFactory` | `REPORT_PROCESSING`: assembles transform chain pipeline (existing general-purpose factory) |

---

## DataSourcePipelineFactory — DATA_SOURCE_DOWNLOAD

```
DataSourcePipelineFactory.assemble(options)
    │
    ├─ 1. DatabaseAdapterFactory.create()     JDBC pool + Secret Manager password
    ├─ 2. ParameterRepository.validate()      fail fast if source config incomplete in DB
    ├─ 3. ParameterRepository.fetchSourceConfigs()  one SourceConfig per source
    │       DB closed here (no live connection needed during graph assembly)
    │
    ├─ 4. BigQueryCheckpointAdapter.isDownloadComplete()  filter already-done sources
    │       (skip unless --overrideDownload=true)
    │
    ├─ 5. Write STARTED checkpoints → BQ
    │
    ├─ 6. For each SourceConfig (parallel Beam branches):
    │       SourceRouter.routeFromConfig()  →  ApiSourceTransform | FileSourceTransform | BigQuerySourceTransform
    │
    ├─ 7. PCollectionList.of(branches).apply(Flatten)   merge all sources
    ├─ 8. SinkRouter.route()                             write to sink
    └─ 9. Optional DLQ branch

After pipeline.run() + waitUntilFinish():
    → Write FINISHED or FAILED checkpoints to BQ
```

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
