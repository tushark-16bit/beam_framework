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
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.dw`;

-- Parameter store: one row per named parameter group.
-- Three-identifier key: parameter_group_name (parent) / parameter_data_source (child) / parameter_name (name).
CREATE TABLE IF NOT EXISTS `my-gcp-project.dw.parameter_store` (
  parameter_name        STRING    NOT NULL,   -- report or parameter name
  parameter_group_name  STRING    NOT NULL,   -- top-level business group (--parentId)
  parameter_data_source STRING    NOT NULL,   -- subprocess / variant (--reportSubprocess)
  schema_of_json        STRING,               -- {"field": {"required": true, "type": "string"}}
  parameters_val_json   STRING,               -- {"field": "value", ...}
  edit_grp_nm           STRING,
  last_updt_ts          TIMESTAMP,
  lst_update_user_id    STRING
);

-- Period master: one row per period. Pre-populated; framework reads only.
-- per_id encoding:
--   YYYYMM       → MONTHLY    (203012 = Dec 2030)
--   YYYYMMDD     → DAILY      (20301112 = 12 Nov 2030)
--   YYYYMMDDQQ   → QUARTERLY  (2030111201 = 12 Nov 2030 Q1)
CREATE TABLE IF NOT EXISTS `my-gcp-project.dw.MSTR_Per` (
  per_id      STRING    NOT NULL,
  per_dt      DATE      NOT NULL,   -- the specific date for this period
  mo_no       INT64     NOT NULL,   -- calendar month (1–12)
  yr_no       STRING    NOT NULL,   -- fiscal year label e.g. "25-26"
  per_typ_cd  STRING    NOT NULL,   -- MONTHLY | DAILY | ANNUALLY | QUARTERLY
  lst_updt_ts TIMESTAMP NOT NULL
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
  da_id              INT64     NOT NULL,   -- surrogate PK: MAX(da_id)+1 per run
  srce_nm            STRING    NOT NULL,   -- data source name or report name
  vsn_no             INT64     NOT NULL,   -- rerun counter per (srce_nm, per_id): 1, 2, 3 …
  per_id             STRING    NOT NULL,   -- period identifier (from MSTR_Per)
  fl_nm              STRING,               -- source location: BQ table, GCS path, or API endpoint
  bal_and_cntl_smry_tx STRING,            -- JSON BnC summary: {status, srcCount, dstCount, …}
  sta_cd             STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED
  created_ts         TIMESTAMP NOT NULL,
  lst_updt_ts        TIMESTAMP NOT NULL
);

-- DaRec: all data rows from every DATA_SOURCE_DOWNLOAD run, stored as JSON blobs.
-- Filter by da_id (FK → DaRefer) to retrieve all rows for one run.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.DaRec` (
  rec_id        STRING    NOT NULL,   -- UUID per row
  da_id         INT64     NOT NULL,   -- FK → DaRefer.da_id
  row_da_json_tx STRING,              -- source row serialised as JSON after transforms
  load_dt       DATE      NOT NULL,   -- partition column; set once per run
  lst_updt_ts   TIMESTAMP NOT NULL
)
PARTITION BY load_dt;

-- COM_CmnRptDtl: one row per output file written by REPORT_PROCESSING.
-- Written for every sink type (GCS, BQ, API) after the output step completes.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.COM_CmnRptDtl` (
  srce_sys_nm      STRING    NOT NULL,  -- report name (matches srce_nm in DaRefer)
  fl_nm            STRING    NOT NULL,  -- output file name, BQ table ref, or API endpoint
  srce_fl_create_ts TIMESTAMP NOT NULL, -- when this output was generated
  fl_da_json_tx    STRING,              -- output data as JSON (populated by future enhancements)
  rec_ct           INT64,               -- row count written to this output
  creat_ts         TIMESTAMP NOT NULL,
  create_user_id   STRING,
  lst_updt_ts      TIMESTAMP NOT NULL,
  lst_updt_user_id STRING
);
```

---

## 2. Seed sample data

```sql
-- Period master rows
INSERT INTO `my-gcp-project.dw.MSTR_Per`
  (per_id, per_dt, mo_no, yr_no, per_typ_cd, lst_updt_ts)
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
INSERT INTO `my-gcp-project.dw.parameter_store`
  (parameter_name, parameter_group_name, parameter_data_source,
   schema_of_json, parameters_val_json, edit_grp_nm, last_updt_ts, lst_update_user_id)
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
-- Key: parameter_group_name=parentId, parameter_data_source=subprocessName, parameter_name=datasourceName.
-- Period-specific filtering is handled by {periodStart}/{periodEnd} tokens inside bq_query.
INSERT INTO `my-gcp-project.dw.parameter_store`
  (parameter_name, parameter_group_name, parameter_data_source,
   schema_of_json, parameters_val_json, edit_grp_nm, last_updt_ts, lst_update_user_id)
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

-- Report config for REPORT_PROCESSING (ReportPipelineFactory) — stored in parameter_store as nested JSON.
-- Key: parameter_group_name=parentId / parameter_data_source=reportSubprocess / parameter_name=reportName.
-- parameters_val_json holds the full report config: datasources, transforms, outputs, and email.
-- periodId is NOT a lookup key — configs are period-agnostic.
INSERT INTO `my-gcp-project.dw.parameter_store`
  (parameter_name, parameter_group_name, parameter_data_source,
   schema_of_json, parameters_val_json, edit_grp_nm, last_updt_ts, lst_update_user_id)
VALUES (
  'daily_trades_summary',   -- ← --reportName
  'TRADING',                -- ← --parentId
  'eod',                    -- ← --reportSubprocess
  JSON '{"override_key": {"required": false, "type": "boolean"}}',
  JSON '{
    "override_key": false,
    "datasources": [
      {
        "datasource_name":       "trades",
        "datasource_subprocess": "eod",
        "transform_alias":       "raw_trades",
        "is_required":           true
      }
    ],
    "preprocessing": [],
    "transforms": [
      {
        "step_order":      1,
        "step_name":       "aggregate_by_currency",
        "input_alias":     "raw_trades",
        "output_alias":    "summary",
        "query_template":  "SELECT JSON_VALUE(row_da_json_tx, ''$.currency'') AS currency, SUM(CAST(JSON_VALUE(row_da_json_tx, ''$.amount'') AS FLOAT64)) AS total_amount, COUNT(*) AS trade_count FROM {raw_trades} GROUP BY currency ORDER BY total_amount DESC",
        "output_bq_table": "my-gcp-project.reports.daily_trades_summary",
        "query_params_json": {}
      }
    ],
    "outputs": [
      {
        "output_order":   1,
        "input_alias":    "summary",
        "sink_type":      "GCS",
        "output_format":  "CSV",
        "gcs_path":       "gs://my-bucket/reports/daily_trades/",
        "file_prefix":    "",
        "file_suffix":    ".csv",
        "include_header": true
      }
    ],
    "email": {
      "to_list": ["analyst@example.com"],
      "cc_list": [],
      "subject_template": "Daily Trades Report {periodId}",
      "body_template":    "Please find the daily trades summary attached for period {periodId}."
    }
  }',
  'TRADING', CURRENT_TIMESTAMP(), 'setup_script'
);
```

---

## 3. Verify setup

```sql
-- Check period master
SELECT per_id, per_dt, mo_no, yr_no, per_typ_cd
FROM `my-gcp-project.dw.MSTR_Per`
WHERE per_id = '202401';

-- Check parameter store row
SELECT parameter_group_name, parameter_data_source, parameter_name,
       JSON_QUERY(schema_of_json, '$')      AS schema,
       JSON_QUERY(parameters_val_json, '$') AS params
FROM `my-gcp-project.dw.parameter_store`
WHERE parameter_group_name  = 'TRADING'
  AND parameter_data_source = 'eod'
  AND parameter_name        = 'daily_trades_summary';

-- Inspect nested JSON sections inside the report's parameter_store row
SELECT
  JSON_QUERY(parameters_val_json, '$.datasources')  AS datasources,
  JSON_QUERY(parameters_val_json, '$.transforms')   AS transforms,
  JSON_QUERY(parameters_val_json, '$.outputs')      AS outputs,
  JSON_QUERY(parameters_val_json, '$.email')        AS email
FROM `my-gcp-project.dw.parameter_store`
WHERE parameter_group_name  = 'TRADING'
  AND parameter_data_source = 'eod'
  AND parameter_name        = 'daily_trades_summary';
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
    --paramBqDataset=dw
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
| 1 | `SELECT parameters_val_json, schema_of_json FROM parameter_store WHERE parameter_group_name='TRADING' AND ...` | BigQuery |
| 2 | Parse `schema_of_json` → required fields; parse `parameters_val_json` → `Map<String,String>` | Driver JVM |
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
  --paramBqDataset=dw \
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
| 2 | Load `ReportConfig` from `parameter_store` (nested JSON in `parameters_val_json`) | — |
| 3 | `createCheckpoint('daily_trades_summary', '202401', ...)` → inserts DaRefer row | → **LOADING** |
| 4 | Check all required datasources have `sta_cd = COMPLETED` in DaRefer for this per_id | — |
| 5 | Build alias registry: datasource alias → `(SELECT row_da_json_tx FROM DaRec WHERE da_id = X)` | — |
| 6 | Run transform chain: BQ SQL → intermediate BQ tables | — |
| 7 | Route each output via `ReportOutputSinkRouter` (GCS / BQ / API) | — |
| 8 | Insert one row into `COM_CmnRptDtl` per output step | — |
| 9 | Send email with GCS outputs as attachments (if email configured) | — |
| 10a | Success → `updateStatus(da_id, COMPLETED, null)` | → **COMPLETED** |
| 10b | Any failure → `updateStatus(da_id, FAILED, null)` | → **FAILED** |

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
SELECT da_id, srce_nm, vsn_no, per_id, fl_nm, sta_cd, bal_and_cntl_smry_tx, lst_updt_ts
FROM `my-gcp-project.pipeline_metadata.DaRefer`
WHERE srce_nm = 'daily_trades_summary'
ORDER BY lst_updt_ts DESC
LIMIT 5;

-- DaRec: count rows written by a specific run (replace 42 with actual da_id)
SELECT COUNT(*) AS row_count,
       MIN(load_dt) AS load_dt
FROM `my-gcp-project.pipeline_metadata.DaRec`
WHERE da_id = 42;

-- DaRec: sample a few rows for a run
SELECT rec_id, JSON_VALUE(row_da_json_tx, '$.currency') AS currency,
       CAST(JSON_VALUE(row_da_json_tx, '$.amount') AS FLOAT64) AS amount,
       load_dt
FROM `my-gcp-project.pipeline_metadata.DaRec`
WHERE da_id = 42
LIMIT 10;

-- COM_CmnRptDtl: outputs written by this report run
SELECT srce_sys_nm, fl_nm, srce_fl_create_ts, rec_ct, create_user_id
FROM `my-gcp-project.pipeline_metadata.COM_CmnRptDtl`
WHERE srce_sys_nm = 'daily_trades_summary'
ORDER BY srce_fl_create_ts DESC
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
  --paramBqDataset=dw \
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
| 2 | Fetch `parameter_store` row for (TRADING, trades, eod) | — |
| 3 | Validate required parameters present in BQ | — |
| 4 | Check DaRefer — skip if `sta_cd=COMPLETED` already exists (unless `--overrideDownload`) | — |
| 5 | `createCheckpoint('trades', '202401', '<bq-table-ref>')` → DaRefer row | → **LOADING** |
| 6 | Dataflow: read source → apply transforms → write rows to DaRec as `row_da_json_tx` JSON | — |
| 7 | `waitUntilFinish()` | — |
| 8 | `COUNT(*) FROM DaRec WHERE da_id = X` + BnC SUM checks | — |
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
INSERT INTO `my-gcp-project.dw.parameter_store`
  (parameter_name, parameter_group_name, parameter_data_source,
   schema_of_json, parameters_val_json, edit_grp_nm, last_updt_ts, lst_update_user_id)
VALUES (
  'monthly_pnl_report',
  'TRADING',
  'monthly',
  JSON '{"pnl_source_table": {"required": true, "type": "string"}}',
  JSON '{"pnl_source_table": "my-gcp-project.raw_data.pnl"}',
  'TRADING', CURRENT_TIMESTAMP(), 'setup_script'
);

INSERT INTO `my-gcp-project.dw.MSTR_Per`
  (per_id, per_dt, mo_no, yr_no, per_typ_cd, lst_updt_ts)
VALUES ('202402', DATE '2024-02-29', 2, '23-24', 'MONTHLY', CURRENT_TIMESTAMP());
```

---

## 9. Key option flags reference

### Process control

| Option | Default | Purpose |
|--------|---------|---------|
| `--processType` | required | `DATA_SOURCE_DOWNLOAD` or `REPORT_PROCESSING` |
| `--parentId` | — | Business group. Maps to `parameter_group_name` in `parameter_store` |
| `--periodId` | — | Period key — must exist in `MSTR_Per` |
| `--jobRunId` | auto UUID | Correlation ID for logs and `COM_CmnRptDtl.CreateUserId` |

### Config tables (read-only)

| Option | Default | Purpose |
|--------|---------|---------|
| `--paramBqProject` | `--project` | GCP project for config tables |
| `--paramBqDataset` | `dw` | BQ dataset for `parameter_store`, `MSTR_Per`, `report_*` tables |
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
| `--datasourceName` | required | Source name — `parameter_name` key in `parameter_store` |
| `--subprocessName` | `default` | Subprocess variant e.g. EOD, INTRADAY |
| `--overrideDownload` | `false` | Re-download even if DaRefer shows COMPLETED |

### REPORT_PROCESSING flags

| Option | Default | Purpose |
|--------|---------|---------|
| `--reportName` | required | Maps to `parameter_name`; also used as `srce_nm` in DaRefer |
| `--reportSubprocess` | `default` | Maps to `parameter_data_source` |
| `--periodStart` | — | Substituted into `{periodStart}` query tokens |
| `--periodEnd` | — | Substituted into `{periodEnd}` query tokens |
