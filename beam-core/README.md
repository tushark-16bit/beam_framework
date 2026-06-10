# beam-core

Pure contracts module. Contains no GCP-specific code and no pipeline graph code.
Every other module depends on this one — it defines the language the whole framework speaks.

---

## What lives here

| Package | Contents | Purpose |
|---|---|---|
| `options` | `FrameworkOptions`, `SourceType`, `SinkType`, `RetryPolicyType`, `WriteDispositionType` | Every CLI flag the framework understands |
| `transform` | `BeamTransform` (SPI interface), `TransformRegistry` | The extension point for adding new transforms |
| `retry` | `RetryPolicy`, `ExponentialRetryPolicy`, `FixedRetryPolicy`, `RetryingDoFn` | Retry logic and dead-letter routing |
| `model` | `FailedRecord`, `Schemas` | Shared data types used across all modules |

---

## Key concept: FrameworkOptions

`FrameworkOptions` is the single source of truth for all CLI flags.
Every pipeline config — source, sink, transforms, retry, calendar, email — lives here.

```
--sourceType=BQ
--bqSourceTable=my-project:my-dataset.orders
--transformChain=filter-nulls,mask-pii
--sinkType=GCS
--gcsSinkPath=gs://bucket/output/
--writeDisposition=TRUNCATE
--retryPolicy=EXPONENTIAL
--maxRetries=3
--deadLetterSink=gs://bucket/dlq/
--runDate=2024-01-15
--calendarName=NYSE
--businessDayOffset=-1
--businessEmail=reports@company.com
--devErrorEmail=oncall@company.com
```

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
    public PTransform<PCollection<Row>, PCollectionTuple> toComposite(FrameworkOptions options) {
        return new MyComposite(options.getSomeFlag());
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
