# Beam Pipeline Framework — Code Walkthrough

A detailed guide to how the framework works, illustrated with UML diagrams.
Intended for engineers onboarding to the codebase or AI agents that need deep architectural understanding.

---

## 1. Module Architecture

The project is split into five Maven modules with a strict one-way dependency rule.

```mermaid
flowchart LR
    core["**beam-core**\nOptions · SPI interface\nModels · Retry logic"]
    utils["**beam-utils**\nDB adapter · Secret Manager\nGCS · BQ schema\nDate · Calendar"]
    io["**beam-io**\nSource connectors\nSink connectors\nStatus · Checkpoint\nEmail · BQ jobs"]
    transforms["**beam-transforms**\nBuilt-in transforms\nSide effects\nSource transforms"]
    runner["**beam-runner**\nMain entry point\nDataSourcePipelineFactory\nReportPipelineFactory\nFat JAR"]

    core --> utils
    core --> io
    core --> transforms
    utils --> transforms
    utils --> runner
    io --> runner
    transforms --> runner

    style core fill:#e8f4f8,stroke:#2196F3
    style utils fill:#e8f5e9,stroke:#4CAF50
    style io fill:#fff3e0,stroke:#FF9800
    style transforms fill:#fce4ec,stroke:#E91E63
    style runner fill:#f3e5f5,stroke:#9C27B0
```

> **Rule**: arrows never point left. `beam-core` depends on nothing internal.
> `beam-io` depends only on `beam-core` — never on `beam-utils` or `beam-transforms`.

---

## 2. Entry Point — Process Type Routing

`Main.java` is the single entry point. It routes by `--processType` and `--reportName`.

```mermaid
flowchart TD
    A["java -jar beam-runner-bundled.jar\n--processType=X ..."] --> B["PipelineOptionsFactory\n.fromArgs(args)\n.as(FrameworkOptions.class)"]
    B --> C{processType?}

    C -->|DATA_SOURCE_DOWNLOAD| D["DataSourcePipelineFactory\n.assemble(options)\npipeline.run()\n.waitUntilFinish()\nrunPostPipelineSteps()"]

    C -->|REPORT_PROCESSING| E{reportName\nset?}
    E -->|yes| F["ReportPipelineFactory\n.execute(options)\ndriver-JVM only\nno Beam pipeline"]
    E -->|no legacy mode| G["PipelineFactory\n.assemble(options)\npipeline.run()\nwaitUntilFinish if batch"]

    D --> H[("process_status\nBQ table")]
    F --> H
    D --> I[("pipeline_checkpoints\nBQ table")]

    style F fill:#e8f5e9,stroke:#4CAF50
    style D fill:#e3f2fd,stroke:#2196F3
    style G fill:#fafafa,stroke:#999
```

---

## 3. DATA_SOURCE_DOWNLOAD — Full Sequence

This process type reads source configuration from the parameter DB, runs one independent Beam branch per source, validates output, and writes status.

```mermaid
sequenceDiagram
    autonumber
    participant Main
    participant DSF as DataSourcePipelineFactory
    participant DB as Parameter DB (JDBC)
    participant Checkpoint as BigQueryCheckpointAdapter
    participant Status as BigQueryProcessStatusAdapter
    participant BQ as BigQuery (output tables)
    participant Beam as Apache Beam / Dataflow

    Main->>DSF: assemble(options)
    DSF->>DB: DatabaseAdapterFactory.create(options)
    DB-->>DSF: DatabaseAdapter (HikariCP pool)
    DSF->>DB: ParameterRepository.allRequiredParametersExist()
    DB-->>DSF: true / false (fail fast if false)
    DSF->>DB: ParameterRepository.fetchSourceConfigs(datasource, period, subprocess)
    DB-->>DSF: List<SourceConfig> (one per DB row)
    DSF->>DB: close()

    loop for each SourceConfig
        DSF->>Checkpoint: getCheckpoint(jobRunId, source, period)
        Checkpoint-->>DSF: CheckpointRecord or null
        alt already FINISHED_ACCESSING and not overrideDownload
            DSF->>DSF: skip this source
        else
            DSF->>Checkpoint: write(STARTED_ACCESSING)
            DSF->>Status: write(PENDING status row)
            DSF->>Beam: SourceRouter.routeFromConfig() → PCollection<Row>
            DSF->>Beam: SourceTransformChainAssembler.assemble() → PCollection<Row>
            DSF->>Beam: wirePerSourceSink() → BigQueryIO.writeTableRows()
        end
    end

    DSF-->>Main: Pipeline (graph assembled, no data moved yet)

    Main->>Beam: pipeline.run()
    Beam->>BQ: reads source data, applies transforms, writes to output tables
    Beam-->>Main: PipelineResult

    Main->>Main: result.waitUntilFinish()
    Main->>DSF: runPostPipelineSteps(finalState, error)

    loop for each SourceConfig that ran
        alt pipeline DONE or UPDATED
            DSF->>BQ: ProcessStatusAdapter.queryRowCount(outputTable)
            BQ-->>DSF: actual row count
            DSF->>DSF: ValidationConfig checks (min/max rows, BnC)
            alt all checks pass
                DSF->>Status: write(COMPLETED, rowCount)
                DSF->>Checkpoint: write(FINISHED_ACCESSING)
            else validation failed
                DSF->>Status: write(VALIDATION_FAILED, details)
                DSF->>Checkpoint: write(FAILED_DOWNLOADING)
            end
        else pipeline FAILED
            DSF->>Status: write(FAILED, errorMessage)
            DSF->>Checkpoint: write(FAILED_DOWNLOADING)
        end
    end
```

---

## 4. Per-Source Beam Branch

Each `SourceConfig` produces one independent branch of the Beam DAG. Branches are **never merged**.

```mermaid
flowchart TD
    subgraph "Beam Pipeline Graph (per source)"
        direction TB
        A["SourceRouter.routeFromConfig()\n→ PCollection&lt;Row&gt;"]

        A -->|sourceType=API| B1["ApiSourceTransform\n@Setup: create HttpClient\n@ProcessElement: paginate + fetch\n@Teardown: close"]
        A -->|sourceType=FILE| B2["FileSourceTransform\n@Setup: nothing\n@ProcessElement: GCS download\n→ CSV or Excel parse"]
        A -->|sourceType=BQ| B3["BigQuerySourceTransform\nBigQueryIO.read()\nSQL with {period} tokens resolved"]

        B1 --> C["SourceTransformChainAssembler\n(ordered chain from source_transforms_json)"]
        B2 --> C
        B3 --> C

        C --> D1["LOOKUP transform\n(if configured)\nLookupEnrichTransform\nPCollectionView side input"]
        D1 --> D2["GROUP_BY transform\n(if configured)\nGroupByTransform\nMapElements → GroupByKey → AggregateDoFn"]
        D2 --> D3["SORT_BY transform\n(if configured)\nSortByTransform\nper-bundle sort only"]

        D3 --> E{output_type?}
        E -->|BQ| F["BigQueryIO.writeTableRows()\nto output_bq_project.output_bq_dataset.output_bq_table\nWRITE_TRUNCATE or APPEND"]
        E -->|GCS| G["GcsSinkTransform\nnewline-delimited JSON\nto output_gcs_path"]
    end

    subgraph "Driver JVM (before pipeline.run)"
        H["QueryParameterResolver\nresolves {periodStart} {periodEnd}\n{periodId} {runDate}\n+ custom query_params_json tokens"]
        H --> A
    end

    subgraph "Driver JVM (after waitUntilFinish)"
        F --> I["BigQueryProcessStatusAdapter\n.queryRowCount(outputTable)"]
        I --> J["ValidationConfig\nmin/max row count\nBnC SUM checks"]
        J --> K[("process_status\nCOMPLETED /\nVALIDATION_FAILED")]
    end
```

---

## 5. SourceTransformChainAssembler — Lookup Loading Detail

Lookup views are built differently depending on the lookup source type.

```mermaid
flowchart LR
    A["SourceConfig\nsource_transforms_json"] --> B["SourceTransformChainAssembler\n.assemble()"]

    B --> C{transform type?}

    C -->|GROUP_BY| D["GroupByTransform\nMapElements → KV<groupKey, Row>\nGroupByKey\nAggregateDoFn\nSUM / COUNT / AVG / MIN / MAX"]

    C -->|SORT_BY| E["SortByTransform\nBundleSortDoFn\n@StartBundle: init buffer\n@ProcessElement: buffer.add\n@FinishBundle: sort and emit\nWARNING: not global order"]

    C -->|LOOKUP| F{lookupSourceType?}

    F -->|JDBC| G["Driver JVM\nDatabaseAdapterFactory.create()\nopen → SELECT * FROM lookup_table → close\nserialize each row to JSON string\nCreate.of(KV pairs) in pipeline\n→ View.asMap()"]

    F -->|BQ| H["In-pipeline\nBigQueryIO.readTableRows()\nMapElements: TableRow → KV<key, jsonBlob>\n→ View.asMap()"]

    G --> I["PCollectionView\nMap<String, String>\nkey → JSON blob of lookup row"]
    H --> I

    I --> J["LookupEnrichTransform\nEnrichDoFn\n@ProcessElement:\nctx.sideInput(lookupView)\nparse JSON blob\nmerge fields into Row\nprefix 'lookup_' on collisions"]
```

---

## 6. REPORT_PROCESSING — Full Sequence

Report processing runs entirely in the **driver JVM** — no Dataflow job is submitted. It orchestrates BigQuery jobs and email sending directly.

```mermaid
sequenceDiagram
    autonumber
    participant Main
    participant RPF as ReportPipelineFactory
    participant DB as Parameter DB (JDBC)
    participant Status as BigQueryProcessStatusAdapter
    participant BQJob as BigQueryJobService
    participant BQ as BigQuery
    participant GCS as Cloud Storage
    participant SMTP as SMTP Server

    Main->>RPF: execute(options)
    RPF->>DB: DatabaseAdapterFactory.create()
    RPF->>DB: ReportRepository.fetchReportConfig(reportName, subprocess, periodId)
    Note over DB: Queries 6 tables:<br/>report_config<br/>report_datasource_ref<br/>report_preprocessing_config<br/>report_transformation_config<br/>report_output_config<br/>report_email_config
    DB-->>RPF: ReportConfig
    RPF->>DB: close()

    RPF->>Status: write(PENDING — processType=REPORT_PROCESSING)

    opt hasPreprocessing
        loop each ReportPreprocessingStep (by step_order)
            RPF->>BQJob: runQueryToTable(resolvedSQL, bqOutputTable)
            BQJob->>BQ: CREATE QueryJob (WRITE_TRUNCATE)
            BQ-->>BQJob: job completed
        end
    end

    loop each required ReportDatasourceRef
        RPF->>Status: getLatest(datasourceName, subprocess)
        Status->>BQ: SELECT ... FROM process_status WHERE ...
        BQ-->>Status: ProcessStatusRecord
        alt status != COMPLETED
            RPF->>Status: write(FAILED — datasource not ready)
            RPF-->>Main: throws RuntimeException
        end
    end

    RPF->>DB: ReportRepository.fetchDatasourceOutputTable() × N
    Note over RPF: Build alias registry:<br/>alias → project.dataset.table

    loop each ReportTransformStep (by step_order)
        RPF->>RPF: resolveAliasTokens({alias} → backtick table ref)
        RPF->>RPF: QueryParameterResolver.resolve(sql, step.queryParams, options)
        RPF->>BQJob: runQueryToTable(resolvedSQL, step.outputBqTable)
        BQJob->>BQ: CREATE QueryJob → materialise result
        BQ-->>BQJob: done
        RPF->>RPF: aliasRegistry.put(step.outputAlias, step.outputBqTable)
    end

    loop each ReportOutputConfig (by output_order)
        RPF->>RPF: look up inputAlias in aliasRegistry
        RPF->>RPF: build GCS URI (gcsPath + prefix + reportName_periodId_date + suffix)
        alt outputFormat=CSV
            RPF->>BQJob: exportToCsv(sourceTable, gcsUri, includeHeader)
        else outputFormat=JSON
            RPF->>BQJob: exportToJson(sourceTable, gcsUri)
        end
        BQJob->>BQ: CREATE ExtractJob → export to GCS
        BQ->>GCS: write file
    end

    opt hasEmail
        loop each exported file
            RPF->>GCS: GcsUtils.readBytes(gcsUri)
            GCS-->>RPF: byte[]
            RPF->>RPF: new EmailAttachment(ByteArrayInputStream(bytes), fileName, contentType)
        end
        RPF->>RPF: resolveEmailTokens(subject/body templates)
        RPF->>SMTP: SmtpReportEmailAdapter.send(subject, body, to, cc, attachments)
        SMTP-->>RPF: sent
    end

    RPF->>Status: write(COMPLETED, outputCount)
```

---

## 7. Query Token Resolution — Three Layers

Every SQL template in the framework goes through up to three resolution passes.

```mermaid
flowchart TD
    A["Raw query template in DB\n\nSELECT t.id, t.amount * f.rate AS usd\nFROM {trades} t\nJOIN {fx_rates} f ON t.ccy = f.ccy\nWHERE t.date BETWEEN '{periodStart}'\n  AND '{periodEnd}'\n  AND t.exchange = '{exchange}'\n  AND t.amount > {threshold}"]

    A --> B["Layer 1 — Alias tokens\nresolveAliasTokens(template, aliasRegistry)\n\n{trades}   → backtick proj.ds.trades_out backtick\n{fx_rates} → backtick proj.ds.fx_out backtick"]

    B --> C["Layer 2 — Standard tokens\nQueryParameterResolver (pass 1)\n\n{periodStart} → options.getPeriodStart()\n{periodEnd}   → options.getPeriodEnd()\n{periodId}    → options.getPeriodId()\n{runDate}     → DateUtils.resolveRunDate()"]

    C --> D["Layer 3 — Custom tokens\nQueryParameterResolver (pass 2)\n\nfrom query_params_json column:\n{exchange}  → NYSE\n{threshold} → 10000\n\nNote: param values may reference\nstandard tokens — resolved first"]

    D --> E["Fully resolved SQL ready for BigQueryJobService.runQueryToTable()"]

    style A fill:#fff9c4,stroke:#F9A825
    style B fill:#e3f2fd,stroke:#1976D2
    style C fill:#e8f5e9,stroke:#388E3C
    style D fill:#fce4ec,stroke:#C62828
    style E fill:#f3e5f5,stroke:#7B1FA2
```

---

## 8. Process Status State Machine

Both `DATA_SOURCE_DOWNLOAD` (per source) and `REPORT_PROCESSING` write to the same `process_status` BQ table, keyed by `processType`.

```mermaid
stateDiagram-v2
    [*] --> PENDING : written before pipeline.run() / before report execution

    PENDING --> COMPLETED : pipeline DONE + all validation checks passed\n(row count, BnC)\nOR report exported all outputs successfully

    PENDING --> VALIDATION_FAILED : pipeline DONE but row count outside bounds\nor BnC SUM exceeds tolerance %\n(DATA_SOURCE_DOWNLOAD only)

    PENDING --> FAILED : pipeline threw exception\nor required datasource not COMPLETED\nor any ReportPipelineFactory phase threw

    COMPLETED --> [*]
    VALIDATION_FAILED --> [*]
    FAILED --> [*]

    note right of PENDING
        Written by driver JVM
        before data moves.
        datasource_name = source name or report name
        subprocess_name = subprocess or report_subprocess
    end note

    note right of COMPLETED
        completed_at timestamp set.
        row_count filled for DATA_SOURCE_DOWNLOAD.
        row_count = output file count for REPORT_PROCESSING.
    end note
```

---

## 9. Key Model Relationships

```mermaid
classDiagram
    class SourceConfig {
        +String datasourceName
        +String periodId
        +String subprocessName
        +SourceType sourceType
        +ApiSourceConfig apiConfig
        +FileSourceConfig fileConfig
        +BqFetchConfig bqFetchConfig
        +OutputConfig outputConfig
        +QueryConfig queryConfig
        +List~SourceTransformConfig~ sourceTransforms
        +ValidationConfig validationConfig
        +Builder builder()
    }

    class OutputConfig {
        +String outputType
        +String bqTableRef()
        +String gcsPath
        +String writeMode
        +boolean isBq()
        +boolean isGcs()
        +boolean isTruncate()
    }

    class QueryConfig {
        +String queryTemplate
        +Map~String,String~ paramMappings
        +boolean hasTemplate()
        +static QueryConfig empty()
    }

    class SourceTransformConfig {
        +String transformType
        +List~String~ groupByFields
        +List~AggregationConfig~ aggregations
        +List~String~ sortByFields
        +LookupConfig lookupConfig
        +static groupBy()
        +static sortBy()
        +static lookup()
    }

    class ValidationConfig {
        +long minRowCount
        +long maxRowCount
        +List~String~ requiredHeaders
        +List~BncRule~ bncRules
        +boolean hasAnyCheck()
    }

    class ProcessStatusRecord {
        +String jobRunId
        +String processType
        +String datasourceName
        +String subprocessName
        +String periodId
        +String status
        +long rowCount
        +static pending()
        +static pendingReport()
        +static completed()
        +static failed()
        +static validationFailed()
        +Builder builder()
    }

    class ReportConfig {
        +String reportName
        +String reportSubprocess
        +String periodId
        +boolean overrideKey
        +List~ReportDatasourceRef~ datasources
        +List~ReportPreprocessingStep~ preprocessingSteps
        +List~ReportTransformStep~ transformSteps
        +List~ReportOutputConfig~ outputConfigs
        +ReportEmailConfig emailConfig
    }

    class ReportTransformStep {
        +int stepOrder
        +String inputAlias
        +String outputAlias
        +String queryTemplate
        +String outputBqTable
        +Map~String,String~ queryParams
    }

    class ReportDatasourceRef {
        +String datasourceName
        +String datasourceSubprocess
        +String transformAlias
        +boolean required
    }

    SourceConfig *-- OutputConfig
    SourceConfig *-- QueryConfig
    SourceConfig *-- ValidationConfig
    SourceConfig *-- SourceTransformConfig
    ReportConfig *-- ReportDatasourceRef
    ReportConfig *-- ReportTransformStep
    ReportConfig *-- ReportOutputConfig
    ReportConfig *-- ReportEmailConfig
    ReportConfig *-- ReportPreprocessingStep
```

---

## 10. Database Entity Relationship — Parameter DB

```mermaid
erDiagram
    source_config {
        varchar datasource_name PK
        varchar period_id PK
        varchar subprocess_name PK
        varchar source_type
        text bq_query
        text query_params_json
        varchar output_type
        varchar output_bq_project
        varchar output_bq_dataset
        varchar output_bq_table
        text source_transforms_json
        bigint min_row_count
        bigint max_row_count
        text required_headers_json
        text bnc_rules_json
    }

    report_config {
        varchar report_name PK
        varchar report_subprocess PK
        varchar period_id PK
        boolean override_key
    }

    report_datasource_ref {
        varchar report_name PK,FK
        varchar report_subprocess PK,FK
        varchar period_id PK,FK
        varchar datasource_name PK
        varchar datasource_subprocess PK
        varchar transform_alias
        boolean is_required
    }

    report_preprocessing_config {
        varchar report_name PK,FK
        varchar report_subprocess PK,FK
        varchar period_id PK,FK
        int step_order PK
        varchar step_type
        text bq_query
        varchar bq_output_table
        text query_params_json
        text api_endpoint
        text api_params_json
    }

    report_transformation_config {
        varchar report_name PK,FK
        varchar report_subprocess PK,FK
        varchar period_id PK,FK
        int step_order PK
        varchar input_alias
        varchar output_alias
        text query_template
        varchar output_bq_table
        text query_params_json
    }

    report_output_config {
        varchar report_name PK,FK
        varchar report_subprocess PK,FK
        varchar period_id PK,FK
        int output_order PK
        varchar input_alias
        varchar output_format
        text gcs_path
        varchar file_prefix
        varchar file_suffix
        boolean include_header
    }

    report_email_config {
        varchar report_name PK,FK
        varchar report_subprocess PK,FK
        varchar period_id PK,FK
        text to_list
        text cc_list
        text subject_template
        text body_template
    }

    report_config ||--o{ report_datasource_ref : "has"
    report_config ||--o{ report_preprocessing_config : "has"
    report_config ||--o{ report_transformation_config : "has"
    report_config ||--o{ report_output_config : "has"
    report_config ||--o| report_email_config : "has"
    report_datasource_ref }o--|| source_config : "references output of"
```

---

## 11. BigQuery Tables — Runtime State

These are BQ tables written to at runtime (not the parameter DB).

```mermaid
erDiagram
    process_status {
        string job_run_id
        string process_type
        string datasource_name
        string subprocess_name
        string period_id
        string period_start
        string period_end
        string status
        int64 row_count
        string error_message
        string validation_details
        timestamp started_at
        timestamp completed_at
    }

    pipeline_checkpoints {
        string job_run_id
        string datasource_name
        string period_id
        string subprocess_name
        string state
        string source_type
        timestamp created_at
        string error_message
        int64 records_processed
    }
```

`process_status.process_type` is either `DATA_SOURCE_DOWNLOAD` or `REPORT_PROCESSING`.
For `DATA_SOURCE_DOWNLOAD`, `datasource_name` = the source name.
For `REPORT_PROCESSING`, `datasource_name` = the report name.

---

## 12. Email Adapter — Class Structure

```mermaid
classDiagram
    class ReportEmailAdapter {
        <<interface>>
        +send(subject, body, to, cc, attachments) void
    }

    class SmtpReportEmailAdapter {
        -Session session
        -String fromAddress
        +SmtpReportEmailAdapter(FrameworkOptions options)
        +send(subject, body, to, cc, attachments) void
    }

    class EmailAttachment {
        +InputStream content
        +String fileName
        +String contentType
        +static csv(content, fileName) EmailAttachment
        +static json(content, fileName) EmailAttachment
    }

    class SideEffectEmailTransform {
        <<Beam PTransform>>
        note "Used inside the pipeline for\nper-row email notifications\nNo attachments"
    }

    ReportEmailAdapter <|.. SmtpReportEmailAdapter : implements
    SmtpReportEmailAdapter ..> EmailAttachment : uses
    ReportEmailAdapter ..> EmailAttachment : parameter

    note for SmtpReportEmailAdapter "Reads SMTP config from FrameworkOptions.\nFetches password from Secret Manager.\nUses jakarta.mail MimeMultipart\nfor file attachments."
```

---

## 13. DATA_SOURCE_DOWNLOAD — Airflow Configuration Example

```python
# Airflow DAG: download trades data for a monthly period
DataflowStartJobOperator(
    task_id="download_trades",
    jar="gs://bucket/jars/beam-runner-bundled.jar",
    options={
        "--processType":        "DATA_SOURCE_DOWNLOAD",
        "--datasourceName":     "trades",
        "--subprocessName":     "eod",
        "--periodId":           "2024-01",
        "--periodStart":        "2024-01-01",
        "--periodEnd":          "2024-01-31",
        "--runDate":            "{{ ds }}",
        "--paramDbUrl":         "jdbc:postgresql://db-host:5432/pipeline_params",
        "--paramDbUser":        "pipeline_user",
        "--paramDbCredentialSecretId": "projects/p/secrets/db-password/versions/latest",
        "--checkpointBqDataset": "pipeline_metadata",
        "--processStatusBqDataset": "pipeline_metadata",
    }
)
```

---

## 14. REPORT_PROCESSING — Airflow Configuration Example

```python
# Airflow DAG: generate daily trades report (runs after download completes)
DataflowStartJobOperator(
    task_id="run_trades_report",
    jar="gs://bucket/jars/beam-runner-bundled.jar",
    options={
        "--processType":        "REPORT_PROCESSING",
        "--reportName":         "daily_trades_report",
        "--reportSubprocess":   "eod",
        "--periodId":           "2024-01",
        "--periodStart":        "2024-01-01",
        "--periodEnd":          "2024-01-31",
        "--runDate":            "{{ ds }}",
        "--paramDbUrl":         "jdbc:postgresql://db-host:5432/pipeline_params",
        "--paramDbUser":        "pipeline_user",
        "--paramDbCredentialSecretId": "projects/p/secrets/db-password/versions/latest",
        "--processStatusBqDataset": "pipeline_metadata",
        "--emailSmtpHost":      "smtp.gmail.com",
        "--emailSmtpPort":      "587",
        "--smtpPasswordSecretId": "projects/p/secrets/smtp-password/versions/latest",
        "--devErrorEmail":      "reports@company.com",
        # --sinkType is NOT required for DB-configured REPORT_PROCESSING
    }
)
```

> **Note**: When `--reportName` is set, `--sinkType`, `--sourceType`, and `--transformChain` are not used.
> The entire pipeline is driven by the `report_*` DB tables.

---

## 15. Code Navigation Map

Where to find things in the source tree:

| Concept | File |
|---|---|
| All CLI flags | [`beam-core/.../options/FrameworkOptions.java`](beam-core/src/main/java/com/yourco/beam/options/FrameworkOptions.java) |
| Entry point | [`beam-runner/.../runner/Main.java`](beam-runner/src/main/java/com/yourco/beam/runner/Main.java) |
| DATA_SOURCE_DOWNLOAD orchestration | [`beam-runner/.../runner/DataSourcePipelineFactory.java`](beam-runner/src/main/java/com/yourco/beam/runner/DataSourcePipelineFactory.java) |
| REPORT_PROCESSING orchestration | [`beam-runner/.../runner/ReportPipelineFactory.java`](beam-runner/src/main/java/com/yourco/beam/runner/ReportPipelineFactory.java) |
| Source routing | [`beam-io/.../io/source/SourceRouter.java`](beam-io/src/main/java/com/yourco/beam/io/source/SourceRouter.java) |
| Per-source transform chain | [`beam-runner/.../runner/SourceTransformChainAssembler.java`](beam-runner/src/main/java/com/yourco/beam/runner/SourceTransformChainAssembler.java) |
| Lookup transform (side input) | [`beam-transforms/.../transforms/source/LookupEnrichTransform.java`](beam-transforms/src/main/java/com/yourco/beam/transforms/source/LookupEnrichTransform.java) |
| Group-by transform | [`beam-transforms/.../transforms/source/GroupByTransform.java`](beam-transforms/src/main/java/com/yourco/beam/transforms/source/GroupByTransform.java) |
| Query token resolution | [`beam-utils/.../utils/QueryParameterResolver.java`](beam-utils/src/main/java/com/yourco/beam/utils/QueryParameterResolver.java) |
| DB parameter loading (source) | [`beam-utils/.../utils/db/ParameterRepository.java`](beam-utils/src/main/java/com/yourco/beam/utils/db/ParameterRepository.java) |
| DB parameter loading (report) | [`beam-utils/.../utils/db/ReportRepository.java`](beam-utils/src/main/java/com/yourco/beam/utils/db/ReportRepository.java) |
| BQ job execution | [`beam-io/.../io/report/BigQueryJobService.java`](beam-io/src/main/java/com/yourco/beam/io/report/BigQueryJobService.java) |
| Process status tracking | [`beam-io/.../io/status/BigQueryProcessStatusAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/status/BigQueryProcessStatusAdapter.java) |
| Email interface | [`beam-io/.../io/email/ReportEmailAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/email/ReportEmailAdapter.java) |
| Email SMTP implementation | [`beam-runner/.../runner/SmtpReportEmailAdapter.java`](beam-runner/src/main/java/com/yourco/beam/runner/SmtpReportEmailAdapter.java) |
| Checkpoint adapter | [`beam-io/.../io/checkpoint/BigQueryCheckpointAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/checkpoint/BigQueryCheckpointAdapter.java) |
