# beam-transforms

Built-in pluggable transform library. This is the primary extension point —
add new transforms here (or in your own separate JAR) without modifying any
other module.

---

## Built-in transforms

| Name (use in `--transformChain`) | Class | What it does |
|---|---|---|
| `filter-nulls` | `FilterNullsTransform` | Drops rows with any null field; routes dropped rows to DLQ with a counter metric |
| `mask-pii` | `MaskPiiTransform` | SHA-256 hashes fields listed in `--piiFields`; configurable at runtime |
| `enrich-from-api` | `EnrichFromExternalApiTransform` | **Sample** showing the `@Setup`/`@Teardown` lifecycle pattern for HTTP clients |

## Per-source data transforms (`source/` sub-package)

These transforms are assembled into a per-source chain by `SourceTransformChainAssembler`
in beam-runner. They run after the data is fetched from the source and before the data
is written to the per-source output table.

| Class | Transform type in DB | What it does |
|---|---|---|
| `GroupByTransform` | `GROUP_BY` | Groups rows by fields; applies SUM/COUNT/AVG/MIN/MAX aggregations. Output schema = group key fields + aggregated fields. |
| `SortByTransform` | `SORT_BY` | Per-bundle sort. **Not global** — use a BQ view with `ORDER BY` for global ordering. Logs a warning when used. |
| `LookupEnrichTransform` | `LOOKUP` | Left-joins rows with a pre-built side input. Accepts `PCollectionView<Map<String,String>>` (key → JSON of lookup row). Name collisions prefixed with `lookup_`. |

The side input for `LookupEnrichTransform` is built by `SourceTransformChainAssembler`:
- `JDBC` lookup: loaded in driver JVM via `DatabaseAdapterFactory`, wrapped in `Create.of()` + `View.asMap()`
- `BQ` lookup: read via `BigQueryIO.readTableRows()` as part of the pipeline graph

## Side-effect transforms

Side effects branch off the main pipeline and run concurrently — they produce `PDone`
(no data output) and are used for notifications, audit logging, and status updates.

| Class | Input Row schema | What it does |
|---|---|---|
| `side/SideEffectEmailTransform` | `to`, `subject`, `body`, `cc` (optional) | Sends SMTP email per row; credentials from Secret Manager via `--smtpPasswordSecretId` |
| `side/SideEffectDbWriteTransform` | any Row (columns mapped to DB columns) | Inserts each row into a JDBC table; credentials from `--paramDb*` options |

```java
// Wire a side effect into any pipeline factory:
PCollection<Row> notifications = successRows.apply("BuildNotification", myTransform);
notifications.apply("SendEmail", new SideEffectEmailTransform(options));
```

## SideInputFactory — sharing data with transforms

`SideInputFactory` creates `PCollectionView` instances from driver-JVM data
(lists, maps, singletons) so transforms can access them as side inputs:

```java
// Create a view from a list fetched in driver JVM
PCollectionView<List<SourceConfig>> configView =
    SideInputFactory.asList(pipeline, "SourceConfigs", configs,
                            SerializableCoder.of(SourceConfig.class));

// Pass to any DoFn that needs per-element config access
rows.apply("Enrich", ParDo.of(new MyFn(configView)).withSideInputs(configView));
```

Use `asSingleton()`, `asList()`, or `asMap()` depending on the access pattern.

---

## How transforms are discovered (SPI)

The framework uses Java's `ServiceLoader` to discover transforms at startup.
Every transform registered in this file is automatically available:

```
src/main/resources/META-INF/services/com.yourco.beam.transform.BeamTransform
```

Current contents:
```
com.yourco.beam.transforms.FilterNullsTransform
com.yourco.beam.transforms.MaskPiiTransform
com.yourco.beam.transforms.EnrichFromExternalApiTransform
```

---

## Adding a new built-in transform

### Step 1 — Create the class

```java
// src/main/java/com/yourco/beam/transforms/MyNewTransform.java
public final class MyNewTransform implements BeamTransform {

    private static final long serialVersionUID = 1L;

    @Override
    public String name() { return "my-new-transform"; }   // used in --transformChain

    @Override
    public PTransform<PCollection<Row>, PCollectionTuple> toComposite(FrameworkOptions options) {
        String myFlag = options.getMyFlag();               // read your own options here
        return new MyComposite(myFlag);
    }

    // Named static inner class — required for Beam serialization
    public static final class MyComposite
            extends PTransform<PCollection<Row>, PCollectionTuple> {

        private final String config;

        public MyComposite(String config) { this.config = config; }

        @Override
        public PCollectionTuple expand(PCollection<Row> input) {
            PCollectionTuple result = input.apply("MyStep",
                ParDo.of(new MyDoFn(config))
                     .withOutputTags(SUCCESS_TAG, TupleTagList.of(DEAD_LETTER_TAG)));
            result.get(SUCCESS_TAG).setRowSchema(input.getSchema());
            return result;
        }
    }

    // Named static DoFn — required for Beam serialization
    public static final class MyDoFn extends DoFn<Row, Row> {
        private final String config;
        public MyDoFn(String config) { this.config = config; }

        @ProcessElement
        public void processElement(@Element Row row, MultiOutputReceiver out) {
            try {
                // ... your logic ...
                out.get(SUCCESS_TAG).output(row);
            } catch (Exception e) {
                out.get(DEAD_LETTER_TAG).output(FailedRecord.of(row, e, 1));
            }
        }
    }
}
```

### Step 2 — Register it

Add one line to `META-INF/services/com.yourco.beam.transform.BeamTransform`:
```
com.yourco.beam.transforms.MyNewTransform
```

### Step 3 — Use it

```bash
--transformChain=filter-nulls,my-new-transform,mask-pii
```

That's the entire change required. No framework code touched.

---

## Adding a transform in a completely separate project

You don't need to modify this module at all. Create your own Maven project:

```
my-custom-transforms/
├── pom.xml   (depends on beam-core only)
└── src/main/
    ├── java/com/myco/MyTransform.java
    └── resources/META-INF/services/com.yourco.beam.transform.BeamTransform
```

Then either:
- **Include at build time**: Add your JAR as a dependency in `beam-runner/pom.xml`.
  `maven-shade-plugin`'s `ServicesResourceTransformer` merges your `META-INF/services`
  file automatically.
- **Include at runtime**: Upload your JAR to GCS and pass
  `--customTransformJarPath=gs://bucket/jars/my-transforms.jar`

---

## The @Setup / @Teardown lifecycle (EnrichFromExternalApiTransform)

For transforms that need non-serializable resources (HTTP clients, DB connections):

```
serialized DoFn → shipped to worker → deserialized
                                              ↓
                                         @Setup (create HttpClient, JDBC connection, etc.)
                                              ↓
                                    @ProcessElement × N (use the resource)
                                              ↓
                                        @Teardown (close/cleanup)
```

Key rule: **declare the resource as `transient`** so it's excluded from serialization.
`@Setup` recreates it on each worker. See `EnrichFromExternalApiTransform` for a
complete annotated example.

---

## Output contract

Every transform MUST output to both tags or Beam will throw at runtime:
- `BeamTransform.SUCCESS_TAG` — successfully processed rows
- `BeamTransform.DEAD_LETTER_TAG` — failed rows (as `FailedRecord`)

If your transform cannot produce failures, output an empty PCollection for the
dead-letter tag:
```java
PCollection<FailedRecord> emptyDlq = input.getPipeline()
    .apply(Create.empty(SerializableCoder.of(FailedRecord.class)));
return PCollectionTuple.of(SUCCESS_TAG, ...).and(DEAD_LETTER_TAG, emptyDlq);
```
