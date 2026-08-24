# beam-runner

Entry point only. Wires all other modules together and produces the deployable fat JAR.
You should rarely need to edit this module.

---

## What lives here

| Class | Purpose |
|---|---|
| `Main` | Parses CLI args, routes by `--processType` (and `--reportName` for REPORT_PROCESSING), delegates to the right factory |
| `DataSourcePipelineFactory` | `DATA_SOURCE_DOWNLOAD`: validates params, fetches configs, creates LOADING checkpoints, assembles per-source Beam branches. `assemble(options)` (single `--datasourceName`) delegates to public `assembleForConfigs(options, List<SourceConfig>)`, which `PipelineSequenceFactory` also calls directly with several explicitly-fetched configs to batch them into one job |
| `PostDownloadFinalizeTransform` | Final pipeline step for each `DATA_SOURCE_DOWNLOAD` source: row/BnC validation, optional `data_transform_query` (replaces stored rows once validated, via an atomic DELETE+INSERT transaction), checkpoint update (COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED), `--manualOverrun` cleanup of the superseded previous run's DaRec rows, and failure email — all running in the Beam worker |
| `ReportPipelineFactory` | `REPORT_PROCESSING` (DB-configured): orchestrates BQ jobs + email in driver JVM; uses `ReportCheckpointAdapter` for RptRefer/RptDaMap/RptStageDa/RptOutput tracking; writes final result to per-report BQ table (`output_bq_table` from config) if set; no Beam pipeline submitted |
| `SmtpReportEmailAdapter` | SMTP implementation of `ReportEmailAdapter`; used by `ReportPipelineFactory` and `PostDownloadFinalizeTransform` |
| `PipelineFactory` | `REPORT_PROCESSING` (legacy): assembles generic source → transform chain → sink Beam pipeline |
| `PipelineSequenceFactory` | `PIPELINE`: runs an ordered sequence of `DATA_SOURCE` steps (batched into one Dataflow job via `DataSourcePipelineFactory.assembleForConfigs`) followed by a terminal `REPORT` step (via the unchanged `ReportPipelineFactory`). Composes both existing factories — see its own section below |

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
    │       (bypassed entirely when --manualOverrun=true or --overrideDownload=true)
    │
    ├─ 2b. Under --manualOverrun only: fetchLatestCompletedDaId() per source, BEFORE the new
    │        checkpoint is created — captured so PostDownloadFinalizeTransform can delete this
    │        superseded run's DaRec rows once the new run reaches COMPLETED
    │
    ├─ 3. BigQueryDataSourceCheckpointAdapter.createCheckpoint() → dataSourceId per source (LOADING row)
    │       Always a fresh INSERT — DaRefer only ever gains new rows, never overwritten
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
                   ├─ data_transform_query (optional; only if the checks above passed):
                   │     BigQueryJobService.runQueryToTable() against a {data} → UNNEST(DaRec)
                   │     subquery; validates output row count; only then replaceStoredRows() runs
                   │     DELETE + INSERT (re-paginated at 250 rows/page) as ONE atomic BigQuery
                   │     multi-statement transaction (BEGIN TRANSACTION...COMMIT, ROLLBACK on
                   │     error) — retried as a whole with backoff (~30s) since this run's rows were
                   │     just streamed in and can still be in BigQuery's streaming buffer
                   │     (DML-ineligible despite being SELECT-visible); on any failure — query
                   │     error, bounds failure, or exhausted retries — the original rows are
                   │     completely untouched, never partially deleted or duplicated
                   ├─ BigQueryDataSourceRecordAdapter.sumField(daId, field) per BnC rule (optional;
                   │     runs against transformed rows if the transform above applied)
                   ├─ updateStatus(daId, COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED, bncJson)
                   ├─ manualOverrun cleanup (only on COMPLETED, only if this run superseded a
                   │     previous COMPLETED da_id): BigQueryDataSourceRecordAdapter.deleteRecords()
                   │     on the previous da_id's DaRec rows — DaRefer itself is untouched, it only
                   │     ever gains new rows
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

## PipelineSequenceFactory — PIPELINE

Composes `DataSourcePipelineFactory` and `ReportPipelineFactory` — it does not reimplement
either. Only decides which data sources to batch together and whether an incomplete one should
stop the sequence before the report runs.

```
PipelineSequenceFactory.execute(options)
    ├─ 1. BigQueryPipelineConfigRepository.fetchPipelineConfig()
    │       → ordered List<PipelineStepConfig> (DataSourceStep | ReportStep)
    │       PipelineConfig's compact constructor already guarantees the last step is a ReportStep
    │
    ├─ 2. For every DataSourceStep: BigQuerySourceConfigRepository.fetchSourceConfigs()
    │       (one call per named datasource — each returns exactly one SourceConfig)
    │
    ├─ 3. DataSourcePipelineFactory.assembleForConfigs(options, allFetchedConfigs)
    │       ONE Dataflow job for every pending step — never one job per step.
    │       Internally skips any step already COMPLETED, same as standalone
    │       DATA_SOURCE_DOWNLOAD (DaRefer skip-logic, unchanged).
    │       pipeline.run().waitUntilFinish()
    │
    ├─ 4. Re-check each DataSourceStep's checkpoint status (isCompleted()).
    │       Still incomplete + the terminal report's own
    │       ReportConfig.datasources[].required=true for that datasource
    │           → abort: PipelineSequenceFactory.PipelineAbortedException
    │             (report step never runs)
    │       Still incomplete + required=false, or not listed in the report's
    │       datasources[] at all
    │           → log and continue (not required by the report that would use it)
    │
    └─ 5. options.setReportName/setReportSubprocess(terminal step) →
            ReportPipelineFactory.execute(options)   [unchanged]
```

**Why no `optional` flag on `DataSourceStep` itself**: the terminal report already declares which
of its datasources are required, via the pre-existing `ReportDatasourceRef.required` — enforced
on every report run (`ReportPipelineFactory.checkDatasourceAvailability()`), pipeline or
standalone. A second, independently-set flag on the pipeline step could disagree with the first
about the same datasource; instead there is exactly one place that decision is declared, and
`PipelineSequenceFactory` looks it up rather than duplicating it.

**Why batch instead of one job per step**: sources are independent Beam branches (the "never
merged" rule still holds — no `Flatten.pCollections()` across sources), so submitting every
pending `DATA_SOURCE` step in this run as one Dataflow job is just `DataSourcePipelineFactory`'s
existing multi-source behavior, reused rather than reinvented.

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
