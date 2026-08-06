# beam-core

Pure contracts module. Contains no GCP-specific code and no pipeline graph code.
Every other module depends on this one — it defines the language the whole framework speaks.

---

## What lives here

| Package | Contents | Purpose |
|---|---|---|
| `options` | `FrameworkOptions`, `ProcessType`, `SourceType`, `SinkType`, `RetryPolicyType`, `WriteDispositionType` | Every CLI flag the framework understands |
| `transform` | `BeamTransform` (SPI interface), `TransformRegistry` | The extension point for adding new transforms |
| `retry` | `RetryPolicy`, `ExponentialRetryPolicy`, `FixedRetryPolicy`, `RetryingDoFn` | Retry logic and dead-letter routing |
| `model` | `FailedRecord`, `Schemas`, `SourceConfig`, `ApiSourceConfig`, `FileSourceConfig`, `BqFetchConfig`, `SourceSchemaField` | Shared data types — DATA_SOURCE_DOWNLOAD. `SourceSchemaField` is one column of the optional explicit schema declared via `bq_schema_json`, carried on `BqFetchConfig.schema` |
| `model` | `DataSourceCheckpoint`, `DataSourceRecord`, `QueryConfig`, `SourceTransformConfig`, `AggregationConfig`, `LookupConfig`, `ValidationConfig`, `BncRule` | Checkpoint/record models, per-source transform and validation config. `DataSourceCheckpoint` status codes: `LOADING`, `COMPLETED`, `FAILED_BNC`, `FAILED_TRANSFORM`, `FAILED` |
| `model` | `DataTransformConfig` | Optional post-storage SQL transform for one source's rows, run within the same `DATA_SOURCE_DOWNLOAD` run; carried on `SourceConfig.dataTransformConfig` |
| `model` | `SourceFailureEmailConfig` | Optional failure-notification email config carried on `SourceConfig`; populated from `failure_email_*` keys in `parameters_val_json` |
| `model` | `ReportConfig`, `ReportDatasourceRef`, `ReportPreprocessingStep`, `ReportTransformStep`, `ReportOutputConfig`, `ReportEmailConfig` | Report configuration assembled from the report DB tables |
| `model` | `ReportCheckpoint`, `RptDaMap`, `RptStageDa`, `RptOutput` | REPORT_PROCESSING tracking rows: RptRefer checkpoint, datasource map, staged data, output record |
| `model` | `PipelineRunConfig` | Per-datasource runtime config loaded from parameter_store. Replaces 21 CLI flags (source, sink, transforms, retry, calendar, email). Typed getters + generic `get(key)` for extensibility. |

---

## ProcessType — execution modes

| Value | CLI flag | Use case |
|---|---|---|
| `DATA_SOURCE_DOWNLOAD` | `--processType=DATA_SOURCE_DOWNLOAD` | Fetch raw data from APIs/files/BQ; creates LOADING checkpoint, runs workers, runs BnC post-pipeline |
| `REPORT_PROCESSING` | `--processType=REPORT_PROCESSING` | Transform downloaded data into reports |
| `POST_DOWNLOAD_VALIDATION` | `--processType=POST_DOWNLOAD_VALIDATION` | Run BnC validation + final checkpoint update for a Classic Template job that has already finished. Requires `--daId=<N>`. Used as a separate Airflow task after the DataflowJobStateSensor passes. |

```bash
# Download raw trades from an external API
java -jar beam-runner-bundled.jar \
  --processType=DATA_SOURCE_DOWNLOAD \
  --datasourceName=trades \
  --periodId=2024-01-15 \
  --subprocessName=eod \
  --sinkType=GCS \
  --gcsSinkPath=gs://bucket/raw/

# Run the report on the downloaded data
java -jar beam-runner-bundled.jar \
  --processType=REPORT_PROCESSING \
  --sourceType=GCS \
  --gcsSourcePath=gs://bucket/raw/*.json \
  --transformChain=filter-nulls,mask-pii \
  --sinkType=BQ \
  --bqSinkTable=project:dataset.report
```

---

## Key concept: FrameworkOptions

`FrameworkOptions` is the single source of truth for all CLI flags.
Every pipeline config — process type, source, sink, transforms, DB, checkpoints, retry, calendar, email — lives here.

### Process control
```
--processType=DATA_SOURCE_DOWNLOAD
--jobRunId=etl-trades-2024-01-15-run1
```

### Data source selection (DATA_SOURCE_DOWNLOAD only)
```
--datasourceName=trades
--periodId=2024-01-15
--subprocessName=eod
--overrideDownload=false    # legacy re-run bypass; prefer --manualOverrun
--manualOverrun=false       # explicit operator key: bypasses COMPLETED guard in DaRefer.
                            # DaRefer always gets a fresh row (never overwritten); once the new
                            # run reaches COMPLETED, the superseded run's DaRec rows are deleted.
```

### Parameter BigQuery store
```
--paramBqProject=my-gcp-project
--paramBqDataset=dw
--paramStoreTable=parameter_store
```

### Checkpoint storage
```
--checkpointBqProject=my-project
--checkpointBqDataset=pipeline_metadata
--checkpointBqTable=pipeline_checkpoints
```

### Run date (REPORT_PROCESSING)
```
--runDate=2024-01-15
--businessDayOffset=0
```

> **Source, sink, transform chain, retry/DLQ, calendar, and email settings** are no longer CLI flags.
> They are fetched per-datasource from `parameter_store` via `PipelineRunConfig`.
> Add a row with the appropriate keys (e.g. `source_type`, `sink_type`, `transform_chain`, etc.) to `parameter_store`.

### Adding a new flag

1. Add a getter + setter pair in `FrameworkOptions.java` with `@Description`
2. Read it in your transform via `options.getMyNewFlag()`
3. Pass it from Airflow: `"--myNewFlag": "{{ dag_run.conf['myNewFlag'] }}"`

---

## Key concept: BeamTransform SPI

`BeamTransform` is the interface all transforms implement. The `name()` string is
what you put in `--transformChain`. `toComposite()` returns the Beam `PTransform`
that processes the data.

```java
public final class MyTransform implements BeamTransform {

    @Override
    public String name() { return "my-transform"; }

    @Override
    public PTransform<PCollection<Row>, PCollectionTuple> toComposite(FrameworkOptions options, PipelineRunConfig runConfig) {
        return new MyComposite(runConfig.get("my_config_key", "default"));
    }

    public static final class MyComposite
            extends PTransform<PCollection<Row>, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollection<Row> input) {
            // ... your logic ...
            // Must output to SUCCESS_TAG and DEAD_LETTER_TAG
        }
    }
}
```

Register in `META-INF/services/com.yourco.beam.transform.BeamTransform`:
```
com.myco.transforms.MyTransform
```

Then use: `--transformChain=filter-nulls,my-transform,mask-pii`

---

## Serialization rules — READ BEFORE WRITING A DoFn

Beam serializes `DoFn` instances and ships them to remote workers. Violations cause
cryptic failures at runtime, not compile time. Follow these rules:

| Rule | Why |
|---|---|
| DoFns must be **named static inner classes** | Anonymous classes capture outer `this`, which may not be serializable |
| All DoFn fields must be `Serializable` | Fields are part of what gets serialized |
| Non-serializable resources (HTTP clients, DB connections) must be `transient` | They cannot be serialized; use `@Setup` to recreate them on workers |
| `TupleTag` instances must be `static final` | Beam uses object identity for tags — recreating them breaks routing |
| Use `SerializableFunction<>`, not `java.util.function.Function<>` | `Function<>` is not `Serializable` |

---

## Key concept: RetryPolicy

`RetryPolicy` controls per-element retry behaviour inside `RetryingDoFn`.
The delay is capped at **200ms** — longer delays block Beam worker threads.
For longer back-off, use a retry-topic pattern (write failed records to Pub/Sub,
re-process on a scheduled interval).

```java
// Used inside transforms that wrap operations in RetryingDoFn:
RetryPolicy policy = new ExponentialRetryPolicy(3, 100); // 3 retries, 100ms base
```

---

## Dependency direction

```
beam-core ← (no internal dependencies)
beam-io   → beam-core
beam-utils → beam-core
beam-transforms → beam-core, beam-utils
beam-runner → beam-core, beam-io, beam-transforms, beam-utils
```

`beam-core` must never depend on any sibling module.
