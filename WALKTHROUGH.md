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

    C -->|DATA_SOURCE_DOWNLOAD| D["DataSourcePipelineFactory\n.assemble(options)\npipeline.run(), then RETURNS\nIMMEDIATELY — no poll loop, no\nwaitUntilFinish(). Required by the\nFlex Template launch contract."]

    C -->|REPORT_PROCESSING| E{reportName\nset?}
    E -->|yes| F["ReportPipelineFactory\n.execute(options)\ndriver-JVM only\nno Beam pipeline"]
    E -->|no legacy mode| G["PipelineFactory\n.assemble(options)\npipeline.run()\nwaitUntilFinish if batch\n(known gap — no checkpoint\nto poll instead)"]

    C -->|PIPELINE| P["PipelineSequenceFactory\n.execute(options)\nsame reportName/reportSubprocess\nas REPORT_PROCESSING — wires the\nreport step onto the SAME batched\ndatasource job via\nReportFinalizeTransform.wire()\n(gated on Wait.on()), submits ONCE,\nRETURNS IMMEDIATELY"]
    P -.assembles via.-> D
    P -.wires report onto same pipeline.-> RFT["ReportFinalizeTransform\n(worker DoFn: Wait.on() gate →\nverify required datasources →\nReportPipelineFactory.execute(options,config)\n→ FailureNotifier on any exception)"]
    RFT -.calls internally.-> F

    C -->|STATUS_CHECK| S["DataSourceStatusChecker\ncheckSingle() / checkPipeline()\nfast DB-only poll of DaRefer\nnever blocks — one BQ read.\nOptional diagnostic only"]

    D --> H[("DaRefer\nBQ table")]
    F --> H
    D --> I[("DaRec\nBQ table")]
    S --> H

    style F fill:#e8f5e9,stroke:#4CAF50
    style D fill:#e3f2fd,stroke:#2196F3
    style G fill:#fafafa,stroke:#999
    style P fill:#fff3e0,stroke:#FB8C00
    style S fill:#fce4ec,stroke:#E91E63
    style RFT fill:#fff3e0,stroke:#FB8C00
```

`DATA_SOURCE_DOWNLOAD` and `PIPELINE` both submit and return immediately — nothing in `main()`
blocks. This deployment launches via a Dataflow Flex Template, whose launch contract requires
`main()` to build the pipeline, submit it, and exit promptly (the launch is considered complete
once the launcher process exits, not once the job finishes); a poll loop in `main()` violates that
contract exactly as much as `waitUntilFinish()` would, and was traced to a real incident (Airflow
timing out at the graph level while the Dataflow job was still — or already — running). Everything
that used to block the driver JVM now happens worker-side, gated by `Wait.on()` instead of a sleep
loop: `PostDownloadFinalizeTransform` for datasource finalization, `ReportFinalizeTransform` for
`PIPELINE`'s report step. `STATUS_CHECK` is the only branch here that ever talks to `Main`
synchronously about readiness — an optional, non-blocking single-check diagnostic, not required
for either of the two paths above.

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
    DSF->>BQCfg: BigQuerySourceConfigRepository.fetchSourceConfigs(parentId, datasource, subprocess, period)
    BQCfg-->>DSF: List<SourceConfig>  (throws IllegalStateException if row missing)

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

    Note over Main: Main does NOT call waitUntilFinish(), and does NOT poll either — it<br/>RETURNS IMMEDIATELY after pipeline.run(). This deployment launches via a Dataflow<br/>Flex Template, whose launch contract requires main() to submit and exit promptly;<br/>any blocking call here (poll loop or waitUntilFinish() alike) breaks the launch<br/>itself. Everything below runs inside the Beam worker (PostDownloadFinalizeTransform)<br/>with nothing in the driver JVM watching for it anymore.

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
        Note over DSF: Always emits a Long signal element for this da_id, success or failure —<br/>read by a downstream Wait.on() when this branch is part of a batched PIPELINE run.
    end

    Note over Checkpoint: This DaRefer row is now the only record of the outcome — no driver-JVM<br/>caller is watching it for a standalone run. --processType=STATUS_CHECK can read it later as an<br/>optional, non-blocking diagnostic. A finalize failure's email (if SourceFailureEmailConfig is set)<br/>was already sent from inside the worker DoFn itself, above.
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

    subgraph "Still inside the Beam worker (PostDownloadFinalizeTransform — main() has already returned; nothing in the driver JVM is waiting)"
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
    participant BQRepo as BigQueryReportRepository
    participant CfgBQ as BigQuery<br/>(parameter_store)
    participant RptAdapter as BigQueryReportCheckpointAdapter<br/>(RptRefer / RptDaMap / RptStageDa / RptOutput)
    participant DsAdapter as BigQueryDataSourceCheckpointAdapter<br/>(DaRefer)
    participant BQJob as BigQueryJobService
    participant DataBQ as BigQuery<br/>(data / report tables)
    participant DaRec as BigQuery<br/>(DaRec)
    participant GCS as Cloud Storage
    participant EmailUtil as EmailSendUtility<br/>(SPI-discovered or injected)

    Main->>RPF: execute(options)

    rect rgb(230, 240, 255)
        Note over RPF,CfgBQ: Phase 1 — Config load
        RPF->>BQRepo: fetchReportConfig(reportName, subprocess, periodId)
        BQRepo->>CfgBQ: SELECT parameters_val_json FROM parameter_store<br/>WHERE parameter_group_name=parentId AND parameter_data_source=subprocess<br/>AND parameter_name=reportName
        CfgBQ-->>BQRepo: parameters_val_json (nested JSON blob)
        BQRepo-->>RPF: ReportConfig (parsed from JSON)
    end

    RPF->>RPF: RunDateCalculator.resolve(config.runScheduleConfig, options) → RunDates
    Note over RPF: every date below (perId, query tokens, file names,<br/>email tokens) comes from RunDates — CLI flags<br/>when no run_details schedule is configured
    RPF->>RptAdapter: createCheckpoint(rptNm=reportName, perId=dates.periodId, rptDs=reportName)
    RptAdapter-->>RPF: rpt_id (LOADING row inserted into RptRefer)

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
        Note over RPF,DsAdapter: Phase 3 — Datasource availability check
        loop each required ReportDatasourceRef
            RPF->>DsAdapter: isCompleted(srce_nm=datasourceName, per_id)
            DsAdapter->>DataBQ: SELECT sta_cd FROM DaRefer WHERE srce_nm=? AND per_id=? AND sta_cd='COMPLETED'
            DataBQ-->>DsAdapter: row or empty
            alt no COMPLETED row
                RPF->>RptAdapter: updateStatus(rpt_id, FAILED)
                RPF-->>Main: throws ReportProcessingException(DATASOURCE_UNAVAILABLE)
            end
        end
    end

    rect rgb(230, 255, 235)
        Note over RPF,DaRec: Phase 4 — Map datasources + stage data
        loop each ReportDatasourceRef
            RPF->>DsAdapter: fetchLatestCompletedDaId(datasourceName, periodId)
            DsAdapter-->>RPF: da_id
            RPF->>RptAdapter: addDaMapping(rpt_id, da_id)
            RptAdapter-->>RPF: map_id (row inserted into RptDaMap)
            RPF->>RptAdapter: stageFromDaRec(map_id, da_id)
            RptAdapter->>DaRec: INSERT INTO RptStageDa SELECT ... FROM DaRec WHERE da_id=? (page copy, one RptStageDa row per DaRec page)
            RPF->>RPF: aliasRegistry.put(alias, stagedDataSubquery(map_id)) — subquery un-nests RptStageDa's pages back into individual records
        end
    end

    rect rgb(240, 230, 255)
        Note over RPF,DataBQ: Phase 5 — Transformation chain
        loop each ReportTransformStep (by step_order)
            RPF->>RPF: resolveAliasTokens({alias} → RptStageDa subquery or prior output table)
            RPF->>BQJob: runQueryToTable(resolvedSQL, step.outputBqTable)
            BQJob->>DataBQ: CREATE QueryJob → materialise to outputBqTable
            RPF->>RPF: aliasRegistry.put(step.outputAlias, step.outputBqTable)
        end
    end

    rect rgb(255, 250, 220)
        Note over RPF,GCS: Phase 6 — Export outputs
        loop each ReportOutputConfig (by output_order)
            RPF->>RPF: aliasRegistry.get(inputAlias) → sourceTable
            alt outputFormat = CSV
                RPF->>BQJob: exportToCsv(sourceTable, gcsUri, includeHeader)
                BQJob->>GCS: write CSV file
            else outputFormat = JSON
                RPF->>BQJob: exportToJson(sourceTable, gcsUri)
                BQJob->>GCS: write JSON file
            end
        end
    end

    rect rgb(255, 235, 210)
        Note over RPF,RptAdapter: Phase 7 — Write RptOutput + clear staged data
        loop each ReportOutputConfig
            RPF->>RptAdapter: writeOutput(rpt_id, outptCd, outputDs, lineReferCd, schedTx, balAm, rptTypeCd)
            RptAdapter->>DataBQ: INSERT INTO RptOutput (vsn_no = MAX(vsn_no)+1)
        end
        RPF->>RptAdapter: clearStagedData(rpt_id)
        RptAdapter->>DataBQ: DELETE FROM RptStageDa WHERE map_id IN (SELECT map_id FROM RptDaMap WHERE rpt_id=?)
    end

    rect rgb(220, 245, 255)
        Note over RPF,EmailUtil: Phase 8 — Email (optional; skipped with a warning if no EmailSendUtility is available)
        opt hasEmail and emailUtility != null
            loop each exported GCS file
                RPF->>EmailUtil: FetchFileFromGcs(gcsUri)
                EmailUtil->>GCS: read object
                GCS-->>EmailUtil: bytes
                EmailUtil-->>RPF: InputStream (wrapped as model.EmailAttachment)
            end
            RPF->>EmailUtil: SetEmailParams(fromAddress, subject, toList, ccList, encrypted)
            EmailUtil-->>RPF: EmailParams
            RPF->>EmailUtil: CreateEmailRequest(emailParams, body, attachments)
        end
    end

    RPF->>RptAdapter: updateStatus(rpt_id, COMPLETED) or updateStatus(rpt_id, FAILED)
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

## 8. Checkpoint State Machines

### DaRefer — DATA_SOURCE_DOWNLOAD

`DATA_SOURCE_DOWNLOAD` writes one `DaRefer` row per source per run.

```mermaid
stateDiagram-v2
    [*] --> LOADING : createCheckpoint() before pipeline.run()

    LOADING --> COMPLETED : pipeline DONE + row-count and BnC checks passed

    LOADING --> FAILED_BNC : pipeline DONE but row count outside min/max\nor BnC SUM exceeds tolerance %

    LOADING --> FAILED : pipeline threw exception

    COMPLETED --> [*]
    FAILED_BNC --> [*]
    FAILED --> [*]

    note right of LOADING
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

### RptRefer — REPORT_PROCESSING

`REPORT_PROCESSING` writes one `RptRefer` row per report run.

```mermaid
stateDiagram-v2
    [*] --> LOADING : createCheckpoint() before execute()

    LOADING --> COMPLETED : all phases complete (transforms + exports + email)

    LOADING --> FAILED : any phase threw (datasource unavailable, BQ job error, etc.)

    COMPLETED --> [*]
    FAILED --> [*]

    note right of LOADING
        rpt_id = MAX(rpt_id)+1 across all RptRefer rows.
        RptDaMap rows added after LOADING (one per datasource).
        RptStageDa rows populated from DaRec; cleared after export.
        RptOutput rows written per output step.
    end note
```

---

## 9. Key Model Relationships

```mermaid
classDiagram
    class SourceConfig {
        +String datasourceName
        +int periodId
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
`DATA_SOURCE_DOWNLOAD` uses `DaRefer` + `DaRec`. `REPORT_PROCESSING` uses `DaRefer` (read-only, availability check) + `RptRefer` / `RptDaMap` / `RptStageDa` / `RptOutput`.

```mermaid
erDiagram
    DaRefer {
        INT64 da_id PK
        STRING srce_nm
        INT64 vsn_no
        INT64 per_id
        STRING fl_nm
        STRING bal_and_cntl_smry_tx
        STRING sta_cd
        DATETIME created_ts
        DATETIME lst_updt_ts
    }

    DaRec {
        STRING rec_id PK
        INT64 da_id FK
        STRING row_da_json_tx
        DATE load_dt
        DATETIME lst_updt_ts
    }

    RptRefer {
        INT64 rpt_id PK
        STRING rpt_nm
        INT64 per_id
        STRING rpt_ds
        STRING sta_cd
        DATETIME creat_ts
        DATETIME lst_updt_ts
    }

    RptDaMap {
        INT64 map_id PK
        INT64 rpt_id FK
        INT64 da_id FK
        DATETIME lst_updt_ts
    }

    RptStageDa {
        INT64 stage_id PK
        INT64 map_id FK
        STRING stage_ds_json_tx
        STRING query_config_tx
        DATE load_dt
        DATETIME lst_updt_ts
    }

    RptOutput {
        STRING outpt_cd
        DATETIME rpt_dt
        INT64 vsn_no
        STRING output_ds
        STRING line_refer_cd
        STRING sched_tx
        FLOAT64 bal_am
        STRING rpt_type_cd
        INT64 rpt_id FK
        DATETIME lst_updt_ts
    }

    DaRefer ||--o{ DaRec : "da_id (DATA_SOURCE_DOWNLOAD rows)"
    RptRefer ||--o{ RptDaMap : "rpt_id"
    RptDaMap ||--o{ RptStageDa : "map_id"
    RptRefer ||--o{ RptOutput : "rpt_id"
    DaRefer ||--o{ RptDaMap : "da_id (read from DaRefer by REPORT_PROCESSING)"
```

**DaRefer** — `sta_cd` values: `LOADING` | `COMPLETED` | `FAILED_BNC` | `FAILED`. Written by `DATA_SOURCE_DOWNLOAD` only; read by `REPORT_PROCESSING` to check datasource availability.

`vsn_no` increments each time the same `(srce_nm, per_id)` is re-run.

`bal_and_cntl_smry_tx` (BnC summary JSON, DATA_SOURCE_DOWNLOAD only):
```json
{ "status": "Matched", "srcCount": 1000, "srcAmount": 5000000.00, "dstCount": 1000, "dstAmount": 5000000.00 }
```

**RptRefer** — `sta_cd` values: `LOADING` | `COMPLETED` | `FAILED`. Written and updated by `REPORT_PROCESSING` only.

**RptStageDa** — transient. Rows are copied from `DaRec` before the transform chain runs and deleted after all outputs are exported. They exist only for the duration of one report execution. Batched like `DaRec`: one `RptStageDa` row per `DaRec` page (≤250 records), not one row per source record — `stagedDataSubquery()` un-nests those pages back into individual records when a transform's `{alias}` resolves, so this batching is invisible to every `query_template`.

---

## 12. Email — Two Separate Contracts, Two Different Callers

```mermaid
classDiagram
    class ReportEmailAdapter {
        <<interface>>
        +send(subject, body, to, cc, attachments) void
    }

    class SmtpReportEmailAdapter {
        -Session session
        -String fromAddress
        +SmtpReportEmailAdapter(String smtpHost, int smtpPort, String smtpPasswordSecretId, String fromAddress)
        +send(subject, body, to, cc, attachments) void
    }

    class IoEmailAttachment["EmailAttachment (io/email)"] {
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
    SmtpReportEmailAdapter ..> IoEmailAttachment : uses
    ReportEmailAdapter ..> IoEmailAttachment : parameter

    note for SmtpReportEmailAdapter "Constructor args come straight from\nSourceFailureEmailConfig — used only by\nPostDownloadFinalizeTransform's\nDATA_SOURCE_DOWNLOAD failure email.\nFetches password from Secret Manager.\nUses jakarta.mail MimeMultipart\nfor file attachments."

    class EmailSendUtility {
        <<interface>>
        +SetEmailParams(fromAddress, subject, toList, ccList, encryptedOrNot) EmailParams
        +CreateEmailRequest(EmailParams, emailBodyHtml, emailAttachments) void
        +FetchFileFromGcs(fileLocation) InputStream
    }

    class EmailParams {
        +String fromEmailAddress
        +String subject
        +List~String~ toList
        +List~String~ ccList
        +boolean encryptedOrNot
    }

    class ModelEmailAttachment["EmailAttachment (model)"] {
        +String fileName
        +InputStream content
        +String type
    }

    EmailSendUtility ..> EmailParams : returns / consumes
    EmailSendUtility ..> ModelEmailAttachment : parameter

    note for EmailSendUtility "No implementation ships in this repo.\nReportPipelineFactory discovers one via\nServiceLoader SPI, or accepts one via\nconstructor injection. Used only for\nREPORT_PROCESSING/PIPELINE\nreport-completion email — if none is\navailable, sending is skipped with a\nwarning, not a failure."
```

`ReportEmailAdapter`/`SmtpReportEmailAdapter` and `EmailSendUtility` are unrelated interfaces for
two different callers — `ReportPipelineFactory` no longer touches `SmtpReportEmailAdapter` at
all (its one call site there had a real constructor-signature bug and has been replaced).

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
        "--periodId":            "202401",        # integer period id, e.g. YYYYMM or YYYYMMDD
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
        "--periodId":            "202401",        # integer period id, e.g. YYYYMM or YYYYMMDD
        "--periodStart":         "2024-01-01",
        "--periodEnd":           "2024-01-31",
        "--runDate":             "{{ ds }}",
        "--paramBqProject":      "my-gcp-project",
        "--paramBqDataset":      "dw",
        "--checkpointBqProject": "my-gcp-project",
        "--checkpointBqDataset": "pipeline_metadata",
        "--daReferTable":        "DaRefer",
        "--daRecTable":          "DaRec",
        "--rptReferTable":       "RptRefer",
        "--rptDaMapTable":       "RptDaMap",
        "--rptStageDaTable":     "RptStageDa",
        "--rptOutputTable":      "RptOutput",
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
| PIPELINE orchestration (batches DATA_SOURCE_DOWNLOAD's job + a Wait.on()-gated report step into ONE pipeline, reuses ReportConfig.datasources[]) | [`beam-runner/.../runner/PipelineSequenceFactory.java`](beam-runner/src/main/java/com/yourco/beam/runner/PipelineSequenceFactory.java) |
| Worker-side report step for PIPELINE (Wait.on() gate → verify required datasources → run the report → FailureNotifier) | [`beam-runner/.../runner/ReportFinalizeTransform.java`](beam-runner/src/main/java/com/yourco/beam/runner/ReportFinalizeTransform.java) |
| Pipeline + per-source finalize signals holder, returned by DataSourcePipelineFactory.assembleForConfigs() | [`beam-runner/.../runner/DataSourceAssembly.java`](beam-runner/src/main/java/com/yourco/beam/runner/DataSourceAssembly.java) |
| STATUS_CHECK readiness poll — optional, non-blocking diagnostic only (DATA_SOURCE_DOWNLOAD/PIPELINE no longer poll) | [`beam-runner/.../runner/DataSourceStatusChecker.java`](beam-runner/src/main/java/com/yourco/beam/runner/DataSourceStatusChecker.java) |
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
| Email interface (DATA_SOURCE_DOWNLOAD failure email) | [`beam-io/.../io/email/ReportEmailAdapter.java`](beam-io/src/main/java/com/yourco/beam/io/email/ReportEmailAdapter.java) |
| Email SMTP implementation | [`beam-runner/.../runner/SmtpReportEmailAdapter.java`](beam-runner/src/main/java/com/yourco/beam/runner/SmtpReportEmailAdapter.java) |
| Email interface (REPORT_PROCESSING/PIPELINE completion email) | [`beam-io/.../io/email/EmailSendUtility.java`](beam-io/src/main/java/com/yourco/beam/io/email/EmailSendUtility.java) |
| Exception hierarchy (one per process type) | [`beam-core/.../exception/`](beam-core/src/main/java/com/yourco/beam/exception/) |
| Failure notification entry point | [`beam-runner/.../runner/FailureNotifier.java`](beam-runner/src/main/java/com/yourco/beam/runner/FailureNotifier.java) |

---

## 16. PIPELINE — One Call: Submit and Return; the Report Runs on a Worker

Composes section 3 (`DATA_SOURCE_DOWNLOAD`), section 6 (the report), and a `Wait.on()` gate — all
as ONE Dataflow job, submitted once, with nothing blocking the driver JVM. There is no separate
pipeline config: `PipelineSequenceFactory` takes the exact same `--reportName`/`--reportSubprocess`
as `REPORT_PROCESSING`, reads that report's own `ReportConfig.datasources[]` (already declaring
which datasources feed it and which are mandatory via `is_required`), batches whichever aren't
`COMPLETED` into the same Dataflow job as a `ReportFinalizeTransform`-wired report step, and
submits once.

### 16a. Why the wait moved onto the worker — the Flex Template launch contract

This deployment launches via a Dataflow Flex Template. Its launch contract requires the launcher
process (`main()`) to build the pipeline, call `pipeline.run()`, and exit promptly — the Dataflow
*launch* operation is considered complete once the launcher exits, not once the submitted job
itself finishes. The previous design (`DataSourceStatusChecker.awaitSingle()`/`awaitPipeline()`, a
plain `Thread.sleep` poll loop in the driver JVM) violated that contract just as much as
`PipelineResult.waitUntilFinish()` would have, and was traced to a real incident: Airflow observed
the launch never completing and timed out at the graph level, independent of whether the Dataflow
job itself succeeded or had already finished.

The fix moves the wait — and the report, and its completion/failure email — onto a worker, gated
by `Wait.on()` (`org.apache.beam.sdk.transforms.Wait`), a Beam data-dependency barrier with no
sleep, no timeout, and no re-reading `DaRefer` from outside the pipeline: it simply guarantees a
downstream step doesn't run until every signal `PCollection` it names has produced an element.

```mermaid
sequenceDiagram
    participant Main
    participant PSF as PipelineSequenceFactory
    participant RR as BigQueryReportRepository
    participant SCR as BigQuerySourceConfigRepository
    participant DSF as DataSourcePipelineFactory
    participant RFT as ReportFinalizeTransform
    participant Beam as Apache Beam / Dataflow

    Main->>PSF: execute(options)
    PSF->>RR: fetchReportConfig(reportName, reportSubprocess, periodId)
    RR-->>PSF: ReportConfig.datasources[] (List<ReportDatasourceRef>)
    PSF->>PSF: RunDateCalculator.resolve(reportConfig.runScheduleConfig, options) → reportDates
    Note over PSF: resolved at submission in the driver JVM,<br/>carried to the worker as a DoFn field.<br/>Each datasource resolves its own RunDates<br/>inside assembleForConfigs().

    loop each declared datasource
        PSF->>SCR: fetchSourceConfigs(parent, dsName, subprocess, periodId)
        SCR-->>PSF: SourceConfig
    end

    PSF->>DSF: assembleForConfigs(options, allFetchedConfigs)
    Note over DSF: skips any datasource already COMPLETED —<br/>same DaRefer skip-logic as standalone<br/>DATA_SOURCE_DOWNLOAD. Throws DataSourceDownloadException<br/>directly on a config/assembly failure.
    DSF-->>PSF: DataSourceAssembly { pipeline, finalizeSignals: List<PCollection<Long>> }

    PSF->>RFT: wire(pipeline, finalizeSignals, reportConfig, reportDates, options)
    Note over RFT: adds Create.of(1) → Wait.on(finalizeSignals) →<br/>ParDo(ReportRunDoFn) to the SAME pipeline —<br/>no separate job, no extra pipeline.run() call.

    PSF->>Beam: pipeline.run()   [ONE submit — datasources + report step, same job]
    PSF-->>Main: returns immediately — nothing here blocks
```

Everything downstream of submission now runs inside `ReportRunDoFn`, on a worker, only after
`Wait.on()` confirms every batched datasource branch reached a terminal state:

```mermaid
sequenceDiagram
    participant Beam as Apache Beam worker
    participant RRD as ReportRunDoFn
    participant CKA as BigQueryDataSourceCheckpointAdapter (DaRefer)
    participant RPF as ReportPipelineFactory
    participant FN as FailureNotifier

    Beam->>RRD: @ProcessElement(trigger, PipelineOptions)
    Note over RRD: FrameworkOptions options = pipelineOptions.as(FrameworkOptions.class) —<br/>reconstructs the worker's options view via Beam's own DoFn-parameter<br/>injection, not a serialized field (FrameworkOptions isn't Serializable).
    RRD->>CKA: isCompleted(dsName, periodId)   [once per required datasource — one-shot, no retry/timeout]
    alt any required datasource not COMPLETED
        RRD->>RRD: throw PipelineException(ABORTED_REQUIRED_DATASOURCE)
        RRD->>FN: notify(options, e) — caught here, NOT rethrown
    else all required datasources COMPLETED
        RRD->>RPF: execute(options, config, dates)   [pre-fetched ReportConfig — no BigQueryReportRepository call on the worker]
        alt report succeeds
            RPF-->>RRD: RptRefer COMPLETED (report's own completion email already sent)
        else report throws ReportProcessingException
            RPF-->>RRD: RptRefer already marked FAILED by ReportPipelineFactory itself
            RRD->>FN: notify(options, e) — caught here, NOT rethrown
        end
    end
```

`ReportRunDoFn` never rethrows: this is the pipeline's last step, and rethrowing would only trigger
Beam bundle retries with no benefit — both `ReportPipelineFactory` and
`PostDownloadFinalizeTransform` have already written their own terminal checkpoint status (and, for
the datasource branches, their own failure email) before any exception reaches here.

**Why no separate required/optional flag anywhere else**: the terminal report already declares
required datasources via `ReportDatasourceRef.required`, enforced both by
`ReportRunDoFn.verifyRequiredDatasources()` (the one-shot check above) and by
`ReportPipelineFactory.checkDatasourceAvailability()` (a belt-and-suspenders re-check when the
report actually runs — section 6). A second, independently-set flag anywhere in a
pipeline-specific config could disagree with the first about the same datasource — there is
exactly one place "is this datasource required" is declared.

**Why one batched job instead of one job per datasource**: sources are independent Beam branches
— the "never merged" rule from section 4 still holds, no `Flatten.pCollections()` across sources
— so submitting every declared datasource plus the report step as one Dataflow job is just
`DataSourcePipelineFactory`'s existing multi-source behavior (`assembleForConfigs`), extended with
one more `Wait.on()`-gated step, not reinvented.

**`--processType=STATUS_CHECK`** (`DataSourceStatusChecker.checkSingle()`/`checkPipeline()`) still
exists as an optional, non-blocking, single BQ-read diagnostic — useful for an ops dashboard or a
manual look — but it is no longer part of either `DATA_SOURCE_DOWNLOAD` or `PIPELINE`'s own flow;
neither one polls, and neither one needs an external caller to invoke it.

---

## 17. Exception Hierarchy — Class Structure

```mermaid
classDiagram
    class DataSourceDownloadException {
        <<RuntimeException>>
        +Reason reason
        +String datasourceName
        +String subprocessName
        +int periodId
        +static wrap(reason, datasourceName, subprocessName, periodId, cause) DataSourceDownloadException
    }
    class DataSourceDownloadException_Reason["Reason"] {
        <<enumeration>>
        FILE_NOT_FOUND
        INVALID_INPUT
        CONNECTIVITY_FAILURE
        JOB_FAILURE
        UNKNOWN
    }

    class ReportProcessingException {
        <<RuntimeException>>
        +Reason reason
        +String reportName
        +String reportSubprocess
        +int periodId
        +static wrap(reason, reportName, reportSubprocess, periodId, cause) ReportProcessingException
    }
    class ReportProcessingException_Reason["Reason"] {
        <<enumeration>>
        CONFIG_NOT_FOUND
        PREPROCESSING_FAILURE
        DATASOURCE_UNAVAILABLE
        STAGING_FAILURE
        TRANSFORM_FAILURE
        OUTPUT_FAILURE
        EMAIL_FAILURE
        UNKNOWN
    }

    class PipelineException {
        <<RuntimeException>>
        +Reason reason
        +String reportName
        +String reportSubprocess
        +int periodId
        +static wrap(reason, reportName, reportSubprocess, periodId, cause) PipelineException
    }
    class PipelineException_Reason["Reason"] {
        <<enumeration>>
        CONFIGURATION_ERROR
        CONFIG_NOT_FOUND
        ABORTED_REQUIRED_DATASOURCE
        DATASOURCE_PHASE_FAILURE
        REPORT_PHASE_FAILURE
        UNKNOWN
    }

    DataSourceDownloadException *-- DataSourceDownloadException_Reason
    ReportProcessingException *-- ReportProcessingException_Reason
    PipelineException *-- PipelineException_Reason

    class DataSourceStatusChecker {
        <<beam-runner, package-private>>
        +checkSingle(options) Outcome
        +checkPipeline(options) Outcome
        note "One non-blocking DaRefer read (or a\nhandful) — STATUS_CHECK's entire\nimplementation. No blocking wrappers\nanymore: DATA_SOURCE_DOWNLOAD/PIPELINE\nno longer poll from the driver JVM —\nsee ReportFinalizeTransform for how\nPIPELINE waits instead (Wait.on(),\nworker-side)."
    }

    class ReportFinalizeTransform {
        <<beam-runner, package-private>>
        +static wire(pipeline, finalizeSignals, config, dates, options) void
        note "Adds Create.of(1) -> Wait.on(finalizeSignals)\n-> ParDo(ReportRunDoFn) to the pipeline.\nReportRunDoFn: verifyRequiredDatasources()\n(one-shot, throws PipelineException) then\nReportPipelineFactory.execute(options,config);\ncatches any exception and calls\nFailureNotifier.notify() itself, worker-side."
    }

    class FailureNotifier {
        <<beam-runner, package-private>>
        +static notify(options, Throwable) void
        note "Called from Main's driver-JVM catch block\nAND from ReportFinalizeTransform's worker\nDoFn — a PIPELINE failure discovered only\nafter main() returns never reaches Main's\ncatch. Template by exception type, plus a\ndefault for anything else. Always logs;\nemails only if --opsFailureEmail is set and\nan EmailSendUtility is available."
    }

    ReportFinalizeTransform ..> PipelineException : throws (verifyRequiredDatasources)
    ReportFinalizeTransform ..> FailureNotifier : calls (worker-side)
    FailureNotifier ..> DataSourceDownloadException : templates
    FailureNotifier ..> ReportProcessingException : templates
    FailureNotifier ..> PipelineException : templates
```

**Thrown from:**

| Exception | Factory | Mechanism |
|---|---|---|
| `DataSourceDownloadException` | `DataSourcePipelineFactory.assemble()`/`assembleForConfigs()` | direct try/catch around config load, graph assembly, and submission — synchronous, driver JVM |
| `DataSourceDownloadException` | `DataSourceStatusChecker.checkSingle()` | maps an observed terminal `sta_cd` in `DaRefer` to `JOB_FAILURE` — the optional `STATUS_CHECK` diagnostic only |
| `ReportProcessingException` | `ReportPipelineFactory.execute()`/`execute(options, config, dates)` | a `currentReason` local, updated before each of the 7 phases runs |
| `PipelineException` | `PipelineSequenceFactory.execute()` | wraps anything that isn't already `DataSourceDownloadException` during assembly/submission — synchronous, driver JVM |
| `PipelineException` (`ABORTED_REQUIRED_DATASOURCE`) | `ReportFinalizeTransform`'s `ReportRunDoFn.verifyRequiredDatasources()` | worker-side, one-shot (no retry/timeout) — runs only after `Wait.on()` confirms every batched datasource branch reached a terminal state |

**Caught in:** `Main.main()` — one `catch (Exception e)` around the whole process-type dispatch,
calling `FailureNotifier.notify(options, e)` then rethrowing `e` unchanged — but this only covers
synchronous, driver-JVM failures (before `pipeline.run()` returns). A failure discovered only on a
worker, after `main()` has already returned (a datasource finalize failure, a required-datasource
check failing, or the report itself failing as PIPELINE's report step), never reaches this catch
block: `PostDownloadFinalizeTransform` and `ReportFinalizeTransform` each call
`FailureNotifier.notify()` directly from inside their own worker DoFn instead. See section 12's
pattern (two separate contracts for two different callers) — this is the same idea one layer up:
three separate exception types for three different process types, converging on one handler that
can now be called from either the driver JVM or a worker.
