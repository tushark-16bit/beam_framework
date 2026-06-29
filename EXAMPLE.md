# End-to-End Example: Parameter Store → DaRefer → Sink

This walkthrough shows the full lifecycle for both pipeline types.

Two ways to run a report:

| Path | Class | DaRefer tracking? | Use when |
|------|-------|-------------------|----------|
| **ExampleWorkflow** | `ExampleWorkflow` | No | Smoke-test parameter-store plumbing directly |
| **Full pipeline** | `Main` via `ReportPipelineFactory` | Yes | Production run with LOADING → COMPLETED tracking |

---

## 1. BQ table DDL (run once in your GCP project)

### Config tables (pre-populated externally, read-only at runtime)

```sql
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.pipeline_config`;

-- Parameter store: one row per named parameter group.
-- Three-identifier key: ParameterGroupName (parent) / ParameterDataSource (child) / ParameterName (name).
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_config.parameter_store` (
  ParameterName       STRING    NOT NULL,   -- report or parameter name
  ParameterGroupName  STRING    NOT NULL,   -- top-level business group (--parentId)
  ParameterDataSource STRING    NOT NULL,   -- subprocess / variant (--reportSubprocess)
  SchemaOfJson        STRING,               -- {"field": {"required": true, "type": "string"}}
  ParametersValJson   STRING,               -- {"field": "value", ...}
  EditGrpNm           STRING,
  LastUpdtTs          TIMESTAMP,
  LstUpdateUserId     STRING
);

-- Period master: one row per period. Pre-populated; framework reads only.
-- PerId encoding:
--   YYYYMM       → MONTHLY    (203012 = Dec 2030)
--   YYYYMMDD     → DAILY      (20301112 = 12 Nov 2030)
--   YYYYMMDDQQ   → QUARTERLY  (2030111201 = 12 Nov 2030 Q1)
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_config.MSTR_Per` (
  PerId      STRING    NOT NULL,
  PerDt      DATE      NOT NULL,   -- the specific date for this period
  MoNo       INT64     NOT NULL,   -- calendar month (1–12)
  YrNo       STRING    NOT NULL,   -- fiscal year label e.g. "25-26"
  PerTypeCd  STRING    NOT NULL,   -- MONTHLY | DAILY | ANNUALLY | QUARTERLY
  LstUpdtTs  TIMESTAMP NOT NULL
);

-- Raw trades source data (example source for DATA_SOURCE_DOWNLOAD)
CREATE TABLE IF NOT EXISTS `my-gcp-project.raw_data.trades` (
  trade_id    STRING,
  currency    STRING,
  amount      FLOAT64,
  trade_date  DATE,
  desk        STRING
);

-- Output BQ table — used as a BQ sink target or materialised transform step
CREATE TABLE IF NOT EXISTS `my-gcp-project.reports.daily_trades_summary` (
  currency     STRING,
  total_amount FLOAT64,
  trade_count  INT64
);
```

### Runtime tables (framework-managed)

```sql
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.pipeline_metadata`;

-- DaRefer: one row per pipeline run (DATA_SOURCE_DOWNLOAD or REPORT_PROCESSING).
-- Created LOADING before the run; updated to COMPLETED / FAILED_BNC / FAILED after.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.DaRefer` (
  DaId              INT64     NOT NULL,   -- surrogate PK: MAX(DaId)+1 per run
  SrceNm            STRING    NOT NULL,   -- data source name or report name
  VsnNo             INT64     NOT NULL,   -- rerun counter per (SrceNm, PerId): 1, 2, 3 …
  PerId             STRING    NOT NULL,   -- period identifier (from MSTR_Per)
  FlNm              STRING,               -- source location: BQ table, GCS path, or API endpoint
  BalAndCntlSmryTx  STRING,               -- JSON BnC summary: {status, srcCount, dstCount, …}
  StaCd             STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED
  CreatedTs         TIMESTAMP NOT NULL,
  LstUpdtTs         TIMESTAMP NOT NULL
);

-- DaRec: all data rows from every DATA_SOURCE_DOWNLOAD run, stored as JSON blobs.
-- Filter by DaId (FK → DaRefer) to retrieve all rows for one run.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.DaRec` (
  RecId        STRING    NOT NULL,   -- UUID per row
  DaId         INT64     NOT NULL,   -- FK → DaRefer.DaId
  RowDaJsonTx  STRING,               -- source row serialised as JSON after transforms
  LoadDt       DATE      NOT NULL,   -- partition column; set once per run
  LstUpdtTs    TIMESTAMP NOT NULL
)
PARTITION BY LoadDt;

-- COM_CmnRptDtl: one row per output file written by REPORT_PROCESSING.
-- Written for every sink type (GCS, BQ, API) after the output step completes.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.COM_CmnRptDtl` (
  SrceSysNm      STRING    NOT NULL,   -- report name (matches SrceNm in DaRefer)
  FlNm           STRING    NOT NULL,   -- output file name, BQ table ref, or API endpoint
  SrceFlCreateTs TIMESTAMP NOT NULL,   -- when this output was generated
  FlDaJsonTx     STRING,               -- output data as JSON (populated by future enhancements)
  RecCt          INT64,                -- row count written to this output
  CreatTs        TIMESTAMP NOT NULL,
  CreateUserId   STRING,
  LstUpdtTs      TIMESTAMP NOT NULL,
  LstUpdtUserId  STRING
);
```

---

## 2. Seed sample data

```sql
-- Period master rows
INSERT INTO `my-gcp-project.pipeline_config.MSTR_Per`
  (PerId, PerDt, MoNo, YrNo, PerTypeCd, LstUpdtTs)
VALUES
  ('202401',   DATE '2024-01-31', 1,  '23-24', 'MONTHLY',   CURRENT_TIMESTAMP()),
  ('20240115', DATE '2024-01-15', 1,  '23-24', 'DAILY',     CURRENT_TIMESTAMP()),
  ('20240101', DATE '2024-01-01', 1,  '23-24', 'DAILY',     CURRENT_TIMESTAMP());

-- Raw trades
INSERT INTO `my-gcp-project.raw_data.trades` VALUES
  ('T001', 'USD', 150000.00, DATE '2024-01-05', 'FX'),
  ('T002', 'EUR',  80000.00, DATE '2024-01-07', 'FX'),
  ('T003', 'USD',  95000.00, DATE '2024-01-10', 'RATES'),
  ('T004', 'GBP',  60000.00, DATE '2024-01-12', 'FX'),
  ('T005', 'EUR',  40000.00, DATE '2024-01-15', 'RATES'),
  ('T006', 'USD', 210000.00, DATE '2024-01-20', 'FX'),
  ('T007', 'JPY', 500000.00, DATE '2024-01-22', 'FX'),
  ('T008', 'GBP',  35000.00, DATE '2024-01-28', 'RATES');

-- Parameter store — three-identifier key: TRADING / eod / daily_trades_summary
INSERT INTO `my-gcp-project.pipeline_config.parameter_store`
  (ParameterName, ParameterGroupName, ParameterDataSource,
   SchemaOfJson, ParametersValJson, EditGrpNm, LastUpdtTs, LstUpdateUserId)
VALUES (
  'daily_trades_summary',
  'TRADING',          -- ← --parentId
  'eod',              -- ← --reportSubprocess
  JSON '{
    "source_bq_table":        {"required": true, "type": "string"},
    "transform_query":        {"required": true, "type": "string"},
    "transform_output_table": {"required": true, "type": "string"},
    "output_gcs_path":        {"required": true, "type": "string"},
    "output_file_name":       {"required": true, "type": "string"}
  }',
  JSON '{
    "source_bq_table":        "my-gcp-project.raw_data.trades",
    "transform_query":        "SELECT currency, SUM(amount) AS total_amount, COUNT(*) AS trade_count FROM `my-gcp-project.raw_data.trades` WHERE trade_date BETWEEN DATE \"{periodStart}\" AND DATE \"{periodEnd}\" GROUP BY currency ORDER BY total_amount DESC",
    "transform_output_table": "my-gcp-project.reports.daily_trades_summary",
    "output_gcs_path":        "gs://my-bucket/reports/daily_trades/",
    "output_file_name":       "daily_trades_summary_{periodId}.csv"
  }',
  'TRADING', CURRENT_TIMESTAMP(), 'setup_script'
);

-- Source config for DATA_SOURCE_DOWNLOAD — stored in parameter_store alongside report params.
-- Key: ParameterGroupName=parentId, ParameterDataSource=subprocessName, ParameterName=datasourceName.
-- Period-specific filtering is handled by {periodStart}/{periodEnd} tokens inside bq_query.
INSERT INTO `my-gcp-project.pipeline_config.parameter_store`
  (ParameterName, ParameterGroupName, ParameterDataSource,
   SchemaOfJson, ParametersValJson, EditGrpNm, LastUpdtTs, LstUpdateUserId)
VALUES (
  'trades',           -- ← --datasourceName
  'TRADING',          -- ← --parentId
  'eod',              -- ← --subprocessName
  JSON '{
    "source_type": {"required": true,  "type": "string"},
    "bq_query":    {"required": true,  "type": "string"}
  }',
  JSON '{
    "source_type":    "BQ",
    "bq_project_id":  "my-gcp-project",
    "bq_dataset":     "raw_data",
    "bq_table":       "trades",
    "bq_query":       "SELECT trade_id, currency, amount, trade_date, desk FROM `my-gcp-project.raw_data.trades` WHERE trade_date BETWEEN DATE \"{periodStart}\" AND DATE \"{periodEnd}\"",
    "min_row_count":  "1",
    "bnc_rules_json": "[{\"field\":\"amount\",\"expectedTotal\":635000,\"tolerancePct\":0.01}]"
  }',
  'TRADING', CURRENT_TIMESTAMP(), 'setup_script'
);

-- report_output_config — one row per output step.
-- sink_type drives where the result goes after the transform completes.
-- Example A: GCS output (default)
INSERT INTO `my-gcp-project.pipeline_config.report_output_config`
  (report_name, report_subprocess, period_id, output_order, input_alias,
   sink_type, output_format, gcs_path, file_prefix, file_suffix, include_header)
VALUES (
  'daily_trades_summary', 'eod', '202401', 1, 'transform_result',
  'GCS', 'CSV', 'gs://my-bucket/reports/daily_trades/', '', '', true
);

-- Example B: BQ output — copies result to a downstream analytics dataset
INSERT INTO `my-gcp-project.pipeline_config.report_output_config`
  (report_name, report_subprocess, period_id, output_order, input_alias,
   sink_type, bq_sink_table)
VALUES (
  'daily_trades_summary', 'eod', '202401', 2, 'transform_result',
  'BQ', 'my-analytics-project.shared_reports.daily_trades_summary'
);

-- Example C: API output — POSTs result rows as JSON to an external service
INSERT INTO `my-gcp-project.pipeline_config.report_output_config`
  (report_name, report_subprocess, period_id, output_order, input_alias,
   sink_type, api_endpoint, api_method, api_auth_secret_id, api_headers_json)
VALUES (
  'daily_trades_summary', 'eod', '202401', 3, 'transform_result',
  'API',
  'https://api.downstream.example.com/v1/reports/trades',
  'POST',
  'projects/my-gcp-project/secrets/downstream-api-key/versions/latest',
  '{"X-Report-Source": "pipeline-framework"}'
);
```

---

## 3. Verify setup

```sql
-- Check period master
SELECT PerId, PerDt, MoNo, YrNo, PerTypeCd
FROM `my-gcp-project.pipeline_config.MSTR_Per`
WHERE PerId = '202401';

-- Check parameter store row
SELECT ParameterGroupName, ParameterDataSource, ParameterName,
       JSON_QUERY(SchemaOfJson, '$')      AS schema,
       JSON_QUERY(ParametersValJson, '$') AS params
FROM `my-gcp-project.pipeline_config.parameter_store`
WHERE ParameterGroupName  = 'TRADING'
  AND ParameterDataSource = 'eod'
  AND ParameterName       = 'daily_trades_summary';

-- Check output config rows
SELECT output_order, input_alias, sink_type,
       gcs_path, bq_sink_table, api_endpoint
FROM `my-gcp-project.pipeline_config.report_output_config`
WHERE report_name = 'daily_trades_summary'
ORDER BY output_order;
```

---

## 4a. Run with ExampleWorkflow (no DaRefer tracking)

ExampleWorkflow bypasses the DaRefer lifecycle — it directly fetches parameters,
runs the BQ transform, and exports to GCS. Useful for smoke-testing the
parameter-store setup without needing the runtime tables.

```bash
mvn -pl beam-runner exec:java \
  -Dexec.mainClass=com.yourco.beam.runner.example.ExampleWorkflow \
  -Dexec.args="
    --project=my-gcp-project
    --parentId=TRADING
    --paramBqProject=my-gcp-project
    --paramBqDataset=pipeline_config
    --paramStoreTable=parameter_store
    --reportName=daily_trades_summary
    --reportSubprocess=eod
    --periodId=202401
    --periodStart=2024-01-01
    --periodEnd=2024-01-31
    --processType=REPORT_PROCESSING"
```

### What ExampleWorkflow does step-by-step

| Step | Action | Where |
|------|--------|-------|
| 1 | `SELECT ParametersValJson, SchemaOfJson FROM parameter_store WHERE ParameterGroupName='TRADING' AND ...` | BigQuery |
| 2 | Parse `SchemaOfJson` → required fields; parse `ParametersValJson` → `Map<String,String>` | Driver JVM |
| 3 | Validate all required fields present — throws if any missing | Driver JVM |
| 4 | Token substitution: `{periodStart}` → `2024-01-01`, `{source_bq_table}` → actual table | Driver JVM |
| 5 | BQ query job: aggregation SQL → `reports.daily_trades_summary` (WRITE_TRUNCATE) | BigQuery |
| 6 | BQ extract job: `reports.daily_trades_summary` → GCS CSV | BigQuery |

---

## 4b. Run with Main (full lifecycle — DaRefer + DaRec + COM_CmnRptDtl)

`Main` drives `ReportPipelineFactory`, which adds the full run lifecycle.

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --processType=REPORT_PROCESSING \
  --project=my-gcp-project \
  --parentId=TRADING \
  --reportName=daily_trades_summary \
  --reportSubprocess=eod \
  --periodId=202401 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=pipeline_config \
  --paramStoreTable=parameter_store \
  --checkpointBqProject=my-gcp-project \
  --checkpointBqDataset=pipeline_metadata \
  --daReferTable=DaRefer \
  --daRecTable=DaRec \
  --cmnRptDtlTable=COM_CmnRptDtl
```

### What ReportPipelineFactory does step-by-step

| Step | Action | DaRefer state |
|------|--------|---------------|
| 1 | Look up `MSTR_Per` for PerId=`202401` → resolve PerDt, MoNo, YrNo | — |
| 2 | Load `ReportConfig` from `report_config` + related tables | — |
| 3 | `createCheckpoint('daily_trades_summary', '202401', ...)` → inserts DaRefer row | → **LOADING** |
| 4 | Check all required datasources have `StaCd = COMPLETED` in DaRefer for this PerId | — |
| 5 | Build alias registry: datasource alias → `(SELECT RowDaJsonTx FROM DaRec WHERE DaId = X)` | — |
| 6 | Run transform chain: BQ SQL → intermediate BQ tables | — |
| 7 | Route each output via `ReportOutputSinkRouter` (GCS / BQ / API) | — |
| 8 | Insert one row into `COM_CmnRptDtl` per output step | — |
| 9 | Send email with GCS outputs as attachments (if email configured) | — |
| 10a | Success → `updateStatus(DaId, COMPLETED, null)` | → **COMPLETED** |
| 10b | Any failure → `updateStatus(DaId, FAILED, null)` | → **FAILED** |

### Output routing (step 7 detail)

| `sink_type` | What happens | Email attachment? |
|-------------|-------------|-------------------|
| `GCS` | BQ extract job → `gs://.../{reportName}_{periodId}_{runDate}.csv` | Yes — file attached |
| `BQ` | `SELECT * FROM result_table` → destination `bq_sink_table` (WRITE_TRUNCATE) | No |
| `API` | Query rows via `TO_JSON_STRING`, POST JSON array to `api_endpoint` | No |

---

## 5. Inspect runtime state

```sql
-- DaRefer: check run lifecycle for this report
SELECT DaId, SrceNm, VsnNo, PerId, FlNm, StaCd, BalAndCntlSmryTx, LstUpdtTs
FROM `my-gcp-project.pipeline_metadata.DaRefer`
WHERE SrceNm = 'daily_trades_summary'
ORDER BY LstUpdtTs DESC
LIMIT 5;

-- DaRec: count rows written by a specific run (replace 42 with actual DaId)
SELECT COUNT(*) AS row_count,
       MIN(LoadDt) AS load_dt
FROM `my-gcp-project.pipeline_metadata.DaRec`
WHERE DaId = 42;

-- DaRec: sample a few rows for a run
SELECT RecId, JSON_VALUE(RowDaJsonTx, '$.currency') AS currency,
       CAST(JSON_VALUE(RowDaJsonTx, '$.amount') AS FLOAT64) AS amount,
       LoadDt
FROM `my-gcp-project.pipeline_metadata.DaRec`
WHERE DaId = 42
LIMIT 10;

-- COM_CmnRptDtl: outputs written by this report run
SELECT SrceSysNm, FlNm, SrceFlCreateTs, RecCt, CreateUserId
FROM `my-gcp-project.pipeline_metadata.COM_CmnRptDtl`
WHERE SrceSysNm = 'daily_trades_summary'
ORDER BY SrceFlCreateTs DESC
LIMIT 10;
```

---

## 6. DATA_SOURCE_DOWNLOAD run (writes to DaRec)

For loading raw data — the Beam Dataflow pipeline path.

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --processType=DATA_SOURCE_DOWNLOAD \
  --project=my-gcp-project \
  --parentId=TRADING \
  --datasourceName=trades \
  --subprocessName=eod \
  --periodId=202401 \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=pipeline_config \
  --checkpointBqProject=my-gcp-project \
  --checkpointBqDataset=pipeline_metadata \
  --daReferTable=DaRefer \
  --daRecTable=DaRec \
  --runner=DataflowRunner \
  --region=us-central1
```

### What DataSourcePipelineFactory does step-by-step

| Step | Action | DaRefer state |
|------|--------|---------------|
| 1 | Look up `MSTR_Per` for PerId=`202401` | — |
| 2 | Fetch `source_config` row for (TRADING, trades, eod, 202401) | — |
| 3 | Validate required parameters present in BQ | — |
| 4 | Check DaRefer — skip if `StaCd=COMPLETED` already exists (unless `--overrideDownload`) | — |
| 5 | `createCheckpoint('trades', '202401', '<bq-table-ref>')` → DaRefer row | → **LOADING** |
| 6 | Dataflow: read source → apply transforms → write rows to DaRec as `RowDaJsonTx` JSON | — |
| 7 | `waitUntilFinish()` | — |
| 8 | `COUNT(*) FROM DaRec WHERE DaId = X` + BnC SUM checks | — |
| 9a | All checks pass → `updateStatus(COMPLETED, bncJson)` | → **COMPLETED** |
| 9b | BnC mismatch → `updateStatus(FAILED_BNC, bncJson)` | → **FAILED_BNC** |
| 9c | Infrastructure error → `updateStatus(FAILED, errorJson)` | → **FAILED** |

---

## 7. Expected GCS output (daily_trades_summary_202401_2024-01-31.csv)

```
currency,total_amount,trade_count
USD,455000.0,3
JPY,500000.0,1
EUR,120000.0,2
GBP,95000.0,2
```

---

## 8. Adding a second report

```sql
INSERT INTO `my-gcp-project.pipeline_config.parameter_store`
  (ParameterName, ParameterGroupName, ParameterDataSource,
   SchemaOfJson, ParametersValJson, EditGrpNm, LastUpdtTs, LstUpdateUserId)
VALUES (
  'monthly_pnl_report',
  'TRADING',
  'monthly',
  JSON '{"pnl_source_table": {"required": true, "type": "string"}}',
  JSON '{"pnl_source_table": "my-gcp-project.raw_data.pnl"}',
  'TRADING', CURRENT_TIMESTAMP(), 'setup_script'
);

INSERT INTO `my-gcp-project.pipeline_config.MSTR_Per`
  (PerId, PerDt, MoNo, YrNo, PerTypeCd, LstUpdtTs)
VALUES ('202402', DATE '2024-02-29', 2, '23-24', 'MONTHLY', CURRENT_TIMESTAMP());
```

---

## 9. Key option flags reference

### Process control

| Option | Default | Purpose |
|--------|---------|---------|
| `--processType` | required | `DATA_SOURCE_DOWNLOAD` or `REPORT_PROCESSING` |
| `--parentId` | — | Business group. Maps to `ParameterGroupName` in `parameter_store` and `parent_id` in `source_config` |
| `--periodId` | — | Period key — must exist in `MSTR_Per` |
| `--jobRunId` | auto UUID | Correlation ID for logs and `COM_CmnRptDtl.CreateUserId` |

### Config tables (read-only)

| Option | Default | Purpose |
|--------|---------|---------|
| `--paramBqProject` | `--project` | GCP project for config tables |
| `--paramBqDataset` | `pipeline_config` | BQ dataset for `parameter_store`, `source_config`, `MSTR_Per`, `report_*` tables |
| `--paramStoreTable` | `parameter_store` | Parameter store table |

### Runtime tables (framework-managed)

| Option | Default | Purpose |
|--------|---------|---------|
| `--checkpointBqProject` | `--project` | GCP project for runtime tables |
| `--checkpointBqDataset` | `pipeline_metadata` | BQ dataset for DaRefer, DaRec, COM_CmnRptDtl |
| `--daReferTable` | `DaRefer` | Run reference table (one row per pipeline run) |
| `--daRecTable` | `DaRec` | Record table (all source rows as JSON blobs) |
| `--cmnRptDtlTable` | `COM_CmnRptDtl` | Report output detail table (one row per output step) |

### DATA_SOURCE_DOWNLOAD flags

| Option | Default | Purpose |
|--------|---------|---------|
| `--datasourceName` | required | Source name — child key in `source_config` |
| `--subprocessName` | `default` | Subprocess variant e.g. EOD, INTRADAY |
| `--overrideDownload` | `false` | Re-download even if DaRefer shows COMPLETED |

### REPORT_PROCESSING flags

| Option | Default | Purpose |
|--------|---------|---------|
| `--reportName` | required | Maps to `ParameterName`; also used as `SrceNm` in DaRefer |
| `--reportSubprocess` | `default` | Maps to `ParameterDataSource` |
| `--periodStart` | — | Substituted into `{periodStart}` query tokens |
| `--periodEnd` | — | Substituted into `{periodEnd}` query tokens |
