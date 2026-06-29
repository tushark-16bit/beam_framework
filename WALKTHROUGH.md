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

    D --> H[("DaRefer\nBQ table")]
    F --> H
    D --> I[("DaRec\nBQ table")]

    style F fill:#e8f5e9,stroke:#4CAF50
    style D fill:#e3f2fd,stroke:#2196F3
    style G fill:#fafafa,stroke:#999
```

---

## 3. DATA_SOURCE_DOWNLOAD — Full Sequence

This process type reads source configuration from BigQuery, runs one independent Beam branch per source, validates output, and writes lifecycle state to `DaRefer`.

```mermaid
sequenceDiagram
    autonumber
    participant Main
    participant DSF as DataSourcePipelineFactory
    participant Per as BigQuery (MSTR_Per)
    participant BQCfg as BigQuery (source_config)
    participant Checkpoint as BigQueryDataSourceCheckpointAdapter (DaRefer)
    participant DaRec as BigQuery (DaRec)
    participant Beam as Apache Beam / Dataflow

    Main->>DSF: assemble(options)
    DSF->>Per: BigQueryPeriodRepository.fetchPeriod(periodId)
    Per-->>DSF: Period (per_dt, mo_no, yr_no, per_typ_cd)
    DSF->>BQCfg: BigQuerySourceConfigRepository.getMissingParameters(parentId, datasource, subprocess, period)
    BQCfg-->>DSF: [] or list of missing keys (fail fast if non-empty)
    DSF->>BQCfg: BigQuerySourceConfigRepository.fetchSourceConfigs(parentId, datasource, subprocess, period)
    BQCfg-->>DSF: List<SourceConfig>

    loop for each SourceConfig
        DSF->>Checkpoint: isCompleted(srce_nm, per_id)
        Checkpoint-->>DSF: true / false
        alt already COMPLETED and not overrideDownload
            DSF->>DSF: skip this source
        else
            DSF->>Checkpoint: createCheckpoint(srce_nm, per_id, fl_nm)
            Checkpoint-->>DSF: da_id (MAX(da_id)+1 across DaRefer)
            DSF->>Beam: SourceRouter.routeFromConfig() → PCollection<Row>
            DSF->>Beam: SourceTransformChainAssembler.assemble() → PCollection<Row>
            DSF->>Beam: DataSourceRecordSinkTransform(da_id)
        end
    end

    DSF-->>Main: Pipeline (graph assembled, no data moved yet)

    Main->>Beam: pipeline.run()
    Beam->>DaRec: streams rows as JSON blobs (rec_id, da_id, row_da_json_tx, load_dt)
    Beam-->>Main: PipelineResult

    Main->>Main: result.waitUntilFinish()
    Main->>DSF: runPostPipelineSteps(finalState, error)

    loop for each SourceConfig that ran
        alt pipeline DONE or UPDATED
            DSF->>DaRec: COUNT(*) WHERE da_id = X
            DaRec-->>DSF: rowCount
            DSF->>DaRec: SUM(JSON_VALUE(row_da_json_tx, @field)) WHERE da_id = X (BnC)
            DaRec-->>DSF: actual sum
            DSF->>DSF: ValidationConfig checks (min/max rows, BnC tolerance%)
            alt all checks pass
                DSF->>Checkpoint: updateStatus(da_id, COMPLETED, bncJson)
            else validation failed
                DSF->>Checkpoint: updateStatus(da_id, FAILED_BNC, bncJson)
            end
        else pipeline FAILED
            DSF->>Checkpoint: updateStatus(da_id, FAILED, null)
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

        D3 --> F["DataSourceRecordSinkTransform\nserialize Row → JSON (JsonUtils.rowToJson)\nset rec_id=UUID, da_id, load_dt\nBigQueryIO.writeTableRows() APPEND"]
        F --> RecTab[("DaRec\nrec_id, da_id\nrow_da_json_tx, load_dt")]
    end

    subgraph "Driver JVM (before pipeline.run)"
        H["QueryParameterResolver\nresolves {periodStart} {periodEnd}\n{periodId} {runDate}\n+ custom query_params_json tokens"]
        H --> A
    end

    subgraph "Driver JVM (after waitUntilFinish)"
        RecTab --> I["DataSourceRecordAdapter\n.countRecords(da_id)\n.sumField(da_id, field)"]
        I --> J["ValidationConfig\nmin/max row count\nBnC JSON_VALUE SUM checks"]
        J --> K[("DaRefer\nCOMPLETED / FAILED_BNC / FAILED\n+ bal_and_cntl_smry_tx JSON")]
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

    C -->|LOOKUP| F["In-pipeline BQ lookup\nBigQueryIO.readTableRows(from bqTableRef)\nMapElements: TableRow → KV&lt;key, jsonBlob&gt;\n→ View.asMap()"]

    F --> I["PCollectionView\nMap&lt;String, String&gt;\nkey → JSON blob of lookup row"]

    I --> J["LookupEnrichTransform\nEnrichDoFn\n@ProcessElement:\nctx.sideInput(lookupView)\nparse JSON blob\nmerge fields into Row\nprefix 'lookup_' on collisions"]
```

---

## 6. REPORT_PROCESSING — Full Sequence

Report processing runs entirely in the **driver JVM** — no Dataflow job is submitted.
All configuration is loaded from **BigQuery** (no JDBC). Two config patterns coexist:
- **Nested JSON** (`parameter_store` via `BigQueryReportRepository`) — used by `ReportPipelineFactory`
- **Flat key-value** (`parameter_store` via `BigQueryParameterAdapter`) — used by `ExampleWorkflow`

Both read the same `parameter_store` table; they differ only in how `parameters_val_json` is structured.

### 6a. ReportPipelineFactory — parameter_store nested JSON config

```mermaid
sequenceDiagram
    autonumber
    participant Main
    participant RPF as ReportPipelineFactory
    participant Per as BigQuery<br/>(MSTR_Per)
    participant BQRepo as BigQueryReportRepository
    participant CfgBQ as BigQuery<br/>(dw)
    participant Checkpoint as BigQueryDataSourceCheckpointAdapter<br/>(DaRefer)
    participant BQJob as BigQueryJobService
    participant DataBQ as BigQuery<br/>(data / report tables)
    participant DaRec as BigQuery<br/>(DaRec)
    participant CmnRpt as BigQueryCommonReportDetailAdapter<br/>(COM_CmnRptDtl)
    participant Router as ReportOutputSinkRouter
    participant GCS as Cloud Storage
    participant SMTP as SMTP Server

    Main->>RPF: execute(options)

    rect rgb(230, 240, 255)
        Note over RPF,Per: Phase 1 — Period + config load
        RPF->>Per: BigQueryPeriodRepository.fetchPeriod(periodId)
        Per-->>RPF: Period (per_dt, mo_no, yr_no, per_typ_cd)
        RPF->>BQRepo: fetchReportConfig(reportName, subprocess, periodId)
        BQRepo->>CfgBQ: SELECT parameters_val_json FROM parameter_store<br/>WHERE parameter_group_name=parentId AND parameter_data_source=subprocess<br/>AND parameter_name=reportName
        CfgBQ-->>BQRepo: parameters_val_json (nested JSON blob)
        BQRepo-->>RPF: ReportConfig (parsed from JSON)
    end

    RPF->>Checkpoint: createCheckpoint(srce_nm=reportName, per_id, fl_nm=reportName)
    Checkpoint-->>RPF: da_id (LOADING row inserted into DaRefer)

    rect rgb(255, 245, 220)
        Note over RPF,DataBQ: Phase 2 — Preprocessing (optional)
        opt hasPreprocessing
            loop each ReportPreprocessingStep (by step_order)
                RPF->>BQJob: runQueryToTable(resolvedSQL, bqOutputTable)
                BQJob->>DataBQ: CREATE QueryJob (WRITE_TRUNCATE)
            end
        end
    end

    rect rgb(255, 235, 235)
        Note over RPF,Checkpoint: Phase 3 — Datasource availability check
        loop each required ReportDatasourceRef
            RPF->>Checkpoint: isCompleted(srce_nm=datasourceName, per_id)
            Checkpoint->>DaRec: SELECT da_id FROM DaRefer WHERE srce_nm=? AND per_id=? AND sta_cd='COMPLETED'
            DaRec-->>Checkpoint: da_id or empty
            alt no COMPLETED row
                RPF->>Checkpoint: updateStatus(da_id, FAILED, null)
                RPF-->>Main: throws RuntimeException
            end
        end
    end

    rect rgb(230, 255, 235)
        Note over RPF,DaRec: Phase 4 — Build alias registry
        loop each ReportDatasourceRef
            RPF->>Checkpoint: fetchLatestCompletedDaId(datasourceName, periodId)
            Checkpoint->>DaRec: SELECT da_id FROM DaRefer WHERE srce_nm=? AND per_id=? AND sta_cd='COMPLETED' ORDER BY lst_updt_ts DESC LIMIT 1
            DaRec-->>Checkpoint: da_id
            Checkpoint-->>RPF: da_id
            RPF->>RPF: aliasRegistry.put(alias, "SELECT row_da_json_tx FROM DaRec WHERE da_id=X")
        end
    end

    rect rgb(240, 230, 255)
        Note over RPF,DataBQ: Phase 5 — Transformation chain
        loop each ReportTransformStep (by step_order)
            RPF->>RPF: resolveAliasTokens({alias} → DaRec subquery or prior output table)
            RPF->>BQJob: runQueryToTable(resolvedSQL, step.outputBqTable)
            BQJob->>DataBQ: CREATE QueryJob → materialise to outputBqTable
            RPF->>RPF: aliasRegistry.put(step.outputAlias, step.outputBqTable)
        end
    end

    rect rgb(255, 250, 220)
        Note over RPF,GCS: Phase 6 — Output sink routing (per ReportOutputConfig)
        loop each ReportOutputConfig (by output_order)
            RPF->>RPF: aliasRegistry.get(inputAlias) → sourceTable
            RPF->>Router: route(outputConfig, sourceTable, config, options)
            alt sinkType = GCS
                Router->>BQJob: exportToCsv / exportToJson(sourceTable, gcsUri)
                BQJob->>GCS: write file
                Router-->>RPF: OutputResult(GCS, gcsUri, fileName, hasAttachment=true)
            else sinkType = BQ
                Router->>BQJob: copyTable(sourceTable, bqSinkTable)
                Router-->>RPF: OutputResult(BQ, bqSinkTable, hasAttachment=false)
            else sinkType = API
                Router->>DataBQ: SELECT TO_JSON_STRING(t) FROM sourceTable
                Router->>Router: POST JSON array (auth from Secret Manager)
                Router-->>RPF: OutputResult(API, endpoint, rowCount, hasAttachment=false)
            end
            RPF->>CmnRpt: insertDetail(srce_sys_nm=reportName, fl_nm, rec_ct, userId)
        end
    end

    rect rgb(220, 245, 255)
        Note over RPF,SMTP: Phase 7 — Email (GCS outputs only, optional)
        opt hasEmail
            loop each OutputResult where hasAttachment=true
                RPF->>GCS: GcsUtils.readBytes(gcsUri)
                GCS-->>RPF: byte[]
            end
            RPF->>SMTP: SmtpReportEmailAdapter.send(subject, body, to, cc, gcsAttachments)
        end
    end

    RPF->>Checkpoint: updateStatus(da_id, COMPLETED, null) or (FAILED, null)
```

### 6b. ExampleWorkflow — key-value BigQueryParameterAdapter pattern

An alternative to the 6-table structured config. All job config lives as key-value rows
in `parameter_store`. The framework discovers which keys are needed from `required_parameters_index`
at runtime — no key names are hard-coded in Java.

```mermaid
sequenceDiagram
    autonumber
    participant EW as ExampleWorkflow
    participant Adapter as BigQueryParameterAdapterImpl
    participant CfgBQ as BigQuery<br/>(dw dataset)
    participant BQJob as BigQueryJobService
    participant DataBQ as BigQuery<br/>(data / report tables)
    participant GCS as Cloud Storage

    EW->>Adapter: fetchRequiredParameters(parameterGroupName, parameterDataSource, parameterName)

    rect rgb(230, 240, 255)
        Note over Adapter,CfgBQ: Step 1 — Fetch the parameter_store row (single BQ query)
        Adapter->>CfgBQ: SELECT parameters_val_json, schema_of_json<br/>FROM parameter_store<br/>WHERE parameter_group_name=@groupName<br/>AND parameter_data_source=@dataSource<br/>AND parameter_name=@paramName LIMIT 1
        CfgBQ-->>Adapter: one row
    end

    rect rgb(230, 255, 235)
        Note over Adapter,Adapter: Step 2 — Parse and validate in driver JVM
        Adapter->>Adapter: parse schema_of_json → find fields where "required"=true<br/>[source_bq_table, transform_query, transform_output_table,<br/>output_gcs_path, output_file_name]
        Adapter->>Adapter: parse parameters_val_json →<br/>{source_bq_table: "proj.raw.trades",<br/>transform_query: "SELECT ...",<br/>transform_output_table: "proj.reports.summary",<br/>output_gcs_path: "gs://bucket/reports/",<br/>output_file_name: "report_{periodId}.csv"}
        Adapter->>Adapter: validate all required fields non-null (throws if any missing)
        Adapter-->>EW: Map<String, String> params
    end

    rect rgb(255, 245, 220)
        Note over EW,EW: Step 3 — Token resolution
        EW->>EW: replace {periodStart}, {periodEnd}, {periodId}, {runDate} in transform_query
    end

    rect rgb(240, 230, 255)
        Note over EW,DataBQ: Step 4 — Run transform query → BQ table
        EW->>BQJob: runQueryToTable(resolvedQuery, params["transform_output_table"])
        BQJob->>DataBQ: CREATE QueryJob (WRITE_TRUNCATE)
        DataBQ-->>BQJob: completed
    end

    rect rgb(255, 250, 220)
        Note over EW,GCS: Step 5 — Export to GCS CSV
        EW->>BQJob: exportToCsv(outputTable, gcsPath + fileName, includeHeader=true)
        BQJob->>DataBQ: CREATE ExtractJob
        DataBQ->>GCS: write CSV file
        EW->>EW: log "output at gs://bucket/reports/report_2024_01.csv"
    end
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

## 8. DaRefer State Machine

Both `DATA_SOURCE_DOWNLOAD` (per source) and `REPORT_PROCESSING` write to `DaRefer`.
Each run creates one row (`sta_cd=LOADING`), then updates it to a terminal state.

```mermaid
stateDiagram-v2
    [*] --> LOADING : createCheckpoint() before pipeline.run() / report.execute()

    LOADING --> COMPLETED : DATA_SOURCE_DOWNLOAD: pipeline DONE + row-count and BnC checks passed\nREPORT_PROCESSING: all outputs routed and COM_CmnRptDtl written

    LOADING --> FAILED_BNC : DATA_SOURCE_DOWNLOAD only:\npipeline DONE but row count outside min/max\nor BnC SUM exceeds tolerance %

    LOADING --> FAILED : pipeline threw exception\nor required datasource has no COMPLETED DaRefer row\nor any ReportPipelineFactory phase threw

    COMPLETED --> [*]
    FAILED_BNC --> [*]
    FAILED --> [*]

    note right of LOADING
        createCheckpoint() inserts into DaRefer.
        da_id = MAX(da_id)+1 across all DaRefer rows.
        vsn_no = MAX(vsn_no)+1 per (srce_nm, per_id).
        All DaRec rows for this run share the same da_id.
    end note

    note right of COMPLETED
        updateStatus() sets sta_cd and bal_and_cntl_smry_tx.
        bal_and_cntl_smry_tx JSON: {status, srcCount, dstCount,
        srcAmount_X, dstAmount_X} per BnC field.
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
        +QueryConfig queryConfig
        +List~SourceTransformConfig~ sourceTransforms
        +ValidationConfig validationConfig
        +Builder builder()
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

    class DataSourceCheckpoint {
        +long daId
        +String srceNm
        +long vsnNo
        +String perId
        +String flNm
        +String balAndCntlSmryTx
        +String staCd
        +Instant createdTs
        +Instant lstUpdtTs
        +static STA_LOADING
        +static STA_COMPLETED
        +static STA_FAILED_BNC
        +static STA_FAILED
        +static loading(daId, vsnNo, srceNm, perId, flNm)
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

## 10. BigQuery Config Tables — Entity Relationship

All configuration lives in BigQuery (`--paramBqProject.--paramBqDataset`). No JDBC.
A single `parameter_store` table holds all configuration for both pipeline types:

- **Source configs** (DATA_SOURCE_DOWNLOAD) — flat JSON in `parameters_val_json`, read by `BigQuerySourceConfigRepository`
- **Report configs** (REPORT_PROCESSING) — nested JSON blob in `parameters_val_json`, read by `BigQueryReportRepository`

The lookup key is always `(parameter_group_name, parameter_data_source, parameter_name)`.
`periodId` is never a lookup key — configs are period-agnostic.

```mermaid
erDiagram
    parameter_store {
        STRING parameter_name PK
        STRING parameter_group_name PK
        STRING parameter_data_source PK
        STRING schema_of_json
        STRING parameters_val_json
        STRING edit_grp_nm
        TIMESTAMP last_updt_ts
        STRING lst_update_user_id
    }

    MSTR_Per {
        STRING per_id PK
        DATE per_dt
        INT64 mo_no
        STRING yr_no
        STRING per_typ_cd
        TIMESTAMP lst_updt_ts
    }

    parameter_store ||--|| MSTR_Per : "per_id referenced at runtime"
```

### parameters_val_json: source config (flat JSON)
```json
{"source_type": "BQ", "bq_query": "SELECT ...", "min_row_count": "1", ...}
```

### parameters_val_json: report config (nested JSON)
```json
{
  "override_key": false,
  "datasources":  [{"datasource_name": "trades", "transform_alias": "raw_trades", "is_required": true, ...}],
  "preprocessing": [],
  "transforms":   [{"step_order": 1, "input_alias": "raw_trades", "output_alias": "summary", "query_template": "...", ...}],
  "outputs":      [{"output_order": 1, "input_alias": "summary", "sink_type": "GCS", "output_format": "CSV", ...}],
  "email":        {"to_list": ["analyst@example.com"], "subject_template": "Report {periodId}", ...}
}
```

---

## 11. BigQuery Tables — Runtime State

These tables are written at runtime (in `--checkpointBqDataset`, default `pipeline_metadata`).
Both process types use `DaRefer`. `DATA_SOURCE_DOWNLOAD` writes `DaRec`. `REPORT_PROCESSING` writes `COM_CmnRptDtl`.

```mermaid
erDiagram
    DaRefer {
        INT64 da_id PK
        STRING srce_nm
        INT64 vsn_no
        STRING per_id
        STRING fl_nm
        STRING bal_and_cntl_smry_tx
        STRING sta_cd
        TIMESTAMP created_ts
        TIMESTAMP lst_updt_ts
    }

    DaRec {
        STRING rec_id PK
        INT64 da_id FK
        STRING row_da_json_tx
        DATE load_dt
        TIMESTAMP lst_updt_ts
    }

    COM_CmnRptDtl {
        STRING srce_sys_nm
        STRING fl_nm
        TIMESTAMP srce_fl_create_ts
        STRING fl_da_json_tx
        INT64 rec_ct
        TIMESTAMP creat_ts
        STRING create_user_id
        TIMESTAMP lst_updt_ts
        STRING lst_updt_user_id
    }

    DaRefer ||--o{ DaRec : "da_id (DATA_SOURCE_DOWNLOAD rows)"
    DaRefer ||--o{ COM_CmnRptDtl : "srce_nm = srce_sys_nm (REPORT_PROCESSING outputs)"
```

`sta_cd` values: `LOADING` | `COMPLETED` | `FAILED_BNC` | `FAILED`.

For `DATA_SOURCE_DOWNLOAD`: `srce_nm` = datasource name, `fl_nm` = BQ table ref / file path / API endpoint.
For `REPORT_PROCESSING`: `srce_nm` = report name, `fl_nm` = report name.

`vsn_no` increments each time the same `(srce_nm, per_id)` is re-run (e.g. after `--overrideDownload=true`).

`bal_and_cntl_smry_tx` (BnC summary JSON, DATA_SOURCE_DOWNLOAD only):
```json
{ "status": "Matched", "srcCount": 1000, "srcAmount": 5000000.00, "dstCount": 1000, "dstAmount": 5000000.00 }
```

`COM_CmnRptDtl` — one row per output step, written by `REPORT_PROCESSING` for all sink types (GCS, BQ, API).
`fl_nm` = GCS file name, destination BQ table, or API endpoint. `rec_ct` = row count written to that sink.

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
        "--processType":         "DATA_SOURCE_DOWNLOAD",
        "--parentId":            "TRADING",      # → parent_id in source_config
        "--datasourceName":      "trades",
        "--subprocessName":      "eod",
        "--periodId":            "202401",        # MONTHLY YYYYMM — must exist in MSTR_Per
        "--periodStart":         "2024-01-01",
        "--periodEnd":           "2024-01-31",
        "--runDate":             "{{ ds }}",
        "--paramBqProject":      "my-gcp-project",
        "--paramBqDataset":      "dw",
        "--checkpointBqProject": "my-gcp-project",
        "--checkpointBqDataset": "pipeline_metadata",
        "--daReferTable":        "DaRefer",
        "--daRecTable":          "DaRec",
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
        "--processType":         "REPORT_PROCESSING",
        "--parentId":            "TRADING",      # → parameter_group_name in parameter_store
        "--reportName":          "daily_trades_summary",
        "--reportSubprocess":    "eod",
        "--periodId":            "202401",        # MONTHLY YYYYMM — must exist in MSTR_Per
        "--periodStart":         "2024-01-01",
        "--periodEnd":           "2024-01-31",
        "--runDate":             "{{ ds }}",
        "--paramBqProject":      "my-gcp-project",
        "--paramBqDataset":      "dw",
        "--checkpointBqProject": "my-gcp-project",
        "--checkpointBqDataset": "pipeline_metadata",
        "--daReferTable":        "DaRefer",
        "--daRecTable":          "DaRec",
        "--cmnRptDtlTable":      "COM_CmnRptDtl",
        "--emailSmtpHost":       "smtp.gmail.com",
        "--emailSmtpPort":       "587",
        "--smtpPasswordSecretId": "projects/p/secrets/smtp-password/versions/latest",
        "--devErrorEmail":       "reports@company.com",
        # --sinkType / --sourceType / --transformChain are NOT used here;
        # all output routing comes from parameter_store outputs[].sink_type (GCS | BQ | API)
    }
)
```

> **Note**: When `--reportName` is set, `--sinkType`, `--sourceType`, and `--transformChain` are not used.
> All config (including output sink type per output step) is loaded from BigQuery.

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
| Source config loading (DATA_SOURCE_DOWNLOAD, BQ) | [`beam-io/.../io/config/BigQuerySourceConfigRepository.java`](beam-io/src/main/java/com/yourco/beam/io/config/BigQuerySourceConfigRepository.java) |
| Report config loading (REPORT_PROCESSING, BQ) | [`beam-io/.../io/config/BigQueryReportRepository.java`](beam-io/src/main/java/com/yourco/beam/io/config/BigQueryReportRepository.java) |
| Key-value BQ parameter store | [`beam-io/.../io/params/BigQueryParameterAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/params/BigQueryParameterAdapter.java) |
| BQ job execution | [`beam-io/.../io/report/BigQueryJobService.java`](beam-io/src/main/java/com/yourco/beam/io/report/BigQueryJobService.java) |
| End-to-end BQ param example | [`beam-runner/.../runner/example/ExampleWorkflow.java`](beam-runner/src/main/java/com/yourco/beam/runner/example/ExampleWorkflow.java) |
| Checkpoint lifecycle (LOADING→COMPLETED/FAILED) | [`beam-io/.../io/checkpoint/BigQueryDataSourceCheckpointAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/checkpoint/BigQueryDataSourceCheckpointAdapter.java) |
| Record table sink (all sources → JSON blobs) | [`beam-io/.../io/sink/DataSourceRecordSinkTransform.java`](beam-io/src/main/java/com/yourco/beam/io/sink/DataSourceRecordSinkTransform.java) |
| Record validation (BnC via JSON_VALUE) | [`beam-io/.../io/records/BigQueryDataSourceRecordAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/records/BigQueryDataSourceRecordAdapter.java) |
| Email interface | [`beam-io/.../io/email/ReportEmailAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/email/ReportEmailAdapter.java) |
| Email SMTP implementation | [`beam-runner/.../runner/SmtpReportEmailAdapter.java`](beam-runner/src/main/java/com/yourco/beam/runner/SmtpReportEmailAdapter.java) |
