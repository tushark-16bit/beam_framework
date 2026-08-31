# End-to-End Example: Parameter Store → RptRefer / RptDaMap / RptStageDa / RptOutput

This walkthrough shows the full lifecycle for both pipeline types.

Two ways to run a report:

| Path | Class | Lifecycle tracking? | Use when |
|------|-------|---------------------|----------|
| **ExampleWorkflow** | `ExampleWorkflow` | No | Smoke-test parameter-store plumbing directly |
| **Full pipeline** | `Main` via `ReportPipelineFactory` | Yes — RptRefer LOADING → COMPLETED | Production run with full checkpoint tracking |

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
  last_updt_ts          DATETIME,
  lst_update_user_id    STRING
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

-- DaRefer: one row per DATA_SOURCE_DOWNLOAD run.
-- Created LOADING before pipeline.run(); updated to COMPLETED / FAILED_BNC / FAILED after.
-- Also READ by ReportPipelineFactory to check datasource availability.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.DaRefer` (
  da_id                INT64     NOT NULL,   -- surrogate PK: MAX(da_id)+1 per run
  srce_nm              STRING    NOT NULL,   -- data source name (--datasourceName)
  vsn_no               INT64     NOT NULL,   -- rerun counter per (srce_nm, per_id): 1, 2, 3 …
  per_id               INT64     NOT NULL,   -- period integer (--periodId): 202401, 20240115
  fl_nm                STRING,               -- source location: BQ table, GCS path, or API endpoint
  bal_and_cntl_smry_tx STRING,              -- JSON BnC summary: {status, srcCount, dstCount, …}
  sta_cd               STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED
  creat_ts             DATETIME  NOT NULL,
  lst_updt_ts          DATETIME  NOT NULL
);

-- DaRec: all source rows from every DATA_SOURCE_DOWNLOAD run, stored as paginated JSON blobs.
-- One DaRec row = one PAGE of up to 250 source rows (row_da_json_tx is a JSON ARRAY, e.g.
-- [{"currency":"USD",...},{"currency":"EUR",...}]), not one row per DaRec row. An 8-row source
-- like the trades example below produces a single page (1 DaRec row, page_no=1).
-- Filter by da_id (FK → DaRefer) and UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx)) to retrieve
-- individual source rows across all pages of a run.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.DaRec` (
  rec_id         STRING    NOT NULL,   -- UUID per page
  da_id          INT64     NOT NULL,   -- FK → DaRefer.da_id
  page_no        INT64     NOT NULL,   -- 1-based page number within this da_id (≤250 rows/page)
  row_da_json_tx STRING,               -- JSON array of source rows in this page, after transforms
  load_dt        DATE      NOT NULL,   -- partition column; set once per run
  lst_updt_ts    DATETIME  NOT NULL
)
PARTITION BY load_dt;

-- RptRefer: one row per REPORT_PROCESSING run.
-- Created LOADING before transforms; updated to COMPLETED / FAILED after.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.RptRefer` (
  rpt_id      INT64     NOT NULL,   -- surrogate PK: MAX(rpt_id)+1 per run
  rpt_nm      STRING    NOT NULL,   -- report name (--reportName)
  per_id      INT64     NOT NULL,   -- period integer (--periodId)
  rpt_ds      STRING,               -- report subprocess (--reportSubprocess)
  sta_cd      STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED
  creat_ts    DATETIME  NOT NULL,
  lst_updt_ts DATETIME  NOT NULL
);

-- RptDaMap: links each REPORT_PROCESSING run to the DaRefer.da_id(s) it consumed.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.RptDaMap` (
  map_id      INT64     NOT NULL,   -- surrogate PK
  rpt_id      INT64     NOT NULL,   -- FK → RptRefer.rpt_id
  da_id       INT64     NOT NULL,   -- FK → DaRefer.da_id
  lst_updt_ts DATETIME  NOT NULL
);

-- RptStageDa: transient staging table — DaRec's pages are copied here verbatim (one RptStageDa
-- row per DaRec page, batched like DaRec itself, not one row per source record), then deleted via
-- clearStagedData() at the end of every successful run. stagedDataSubquery() un-nests pages back
-- into individual source-row JSON objects on read, so this batching is invisible to report SQL.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.RptStageDa` (
  stage_id        INT64     NOT NULL,   -- surrogate PK (base + ROW_NUMBER per bulk INSERT), one per page
  map_id          INT64     NOT NULL,   -- FK → RptDaMap.map_id
  stage_ds_json_tx STRING,              -- one DaRec page's JSON, copied verbatim (≤250 records)
  query_config_tx  STRING,              -- JSON metadata: {da_id, map_id}
  load_dt         DATE      NOT NULL,
  lst_updt_ts     DATETIME  NOT NULL
);

-- RptOutput: one row per output step of a REPORT_PROCESSING run.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.RptOutput` (
  outpt_cd      STRING    NOT NULL,   -- output identifier (file name or destination ref)
  rpt_dt        DATETIME  NOT NULL,   -- when this output was produced
  vsn_no        INT64     NOT NULL,   -- rerun counter per (rpt_id, outpt_cd): 1, 2, 3 …
  output_ds     STRING,               -- destination: GCS URI, BQ table, or API endpoint
  line_refer_cd STRING,
  sched_tx      STRING,
  bal_am        FLOAT64,
  rpt_type_cd   STRING,
  rpt_id        INT64     NOT NULL,   -- FK → RptRefer.rpt_id
  lst_updt_ts   DATETIME  NOT NULL
);
```

---

## 2. Seed sample data

```sql
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
  'TRADING', CURRENT_DATETIME(), 'setup_script'
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
    "bnc_rules_json": "[{\"field\":\"amount\",\"expectedTotal\":1170000,\"tolerancePct\":0.01}]",

    "failure_email_to":      "ops-team@example.com,data-owner@example.com",
    "failure_email_cc":      "manager@example.com",
    "failure_email_subject": "FAILED: {datasourceName} download for period {periodId}",
    "failure_email_body":    "Data source download has failed.\n\nDatasource : {datasourceName}\nPeriod     : {periodId}\nStatus     : {staCd}\n\nError:\n{errorMessage}\n\nBnC Summary:\n{bncSummary}",
    "email_smtp_host":       "smtp.gmail.com",
    "email_smtp_port":       "587",
    "smtp_password_secret_id": "projects/my-gcp-project/secrets/smtp-password/versions/latest",
    "from_address":          "pipeline-alerts@example.com"
  }',
  'TRADING', CURRENT_DATETIME(), 'setup_script'
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
        "query_template":  "SELECT JSON_VALUE(stage_ds_json_tx, ''$.currency'') AS currency, SUM(CAST(JSON_VALUE(stage_ds_json_tx, ''$.amount'') AS FLOAT64)) AS total_amount, COUNT(*) AS trade_count FROM {raw_trades} GROUP BY currency ORDER BY total_amount DESC",
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
  'TRADING', CURRENT_DATETIME(), 'setup_script'
);
```

---

## 3. Verify setup

```sql
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

## 4b. Run with Main (full lifecycle — RptRefer + RptDaMap + RptStageDa + RptOutput)

`Main` drives `ReportPipelineFactory`, which adds the full checkpoint and output tracking lifecycle.

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
  --checkpointBqDataset=pipeline_metadata
```

All four runtime table names default to `DaRefer`, `DaRec`, `RptRefer`, `RptDaMap`, `RptStageDa`,
and `RptOutput`. Override with `--daReferTable`, `--daRecTable`, `--rptReferTable`,
`--rptDaMapTable`, `--rptStageDaTable`, `--rptOutputTable` if your table names differ.

### What ReportPipelineFactory does step-by-step

| Step | Action | RptRefer state |
|------|--------|----------------|
| 1 | Load `ReportConfig` from `parameter_store` (nested JSON in `parameters_val_json`) | — |
| 2 | `createCheckpoint('daily_trades_summary', 202401, ...)` → inserts RptRefer row → returns `rpt_id` | → **LOADING** |
| 3 | Run preprocessing steps in `step_order` (BQ_QUERY or API_ENRICHMENT), if any | — |
| 4 | For each required datasource ref: `isCompleted(srce_nm, 202401)` via DaRefer — fail if not COMPLETED | — |
| 5 | For each datasource ref: `fetchLatestCompletedDaId()` → `addDaMapping(rpt_id, da_id)` → RptDaMap row | — |
| 6 | `stageFromDaRec(map_id, da_id)` — copies every DaRec page for that da_id into RptStageDa verbatim (one RptStageDa row per DaRec page, batched like DaRec itself — not one row per source record) | — |
| 7 | Register alias: `raw_trades` → `stagedDataSubquery(map_id)`, which un-nests RptStageDa's pages back into individual source rows on read (`SELECT row_json AS stage_ds_json_tx FROM RptStageDa CROSS JOIN UNNEST(...) AS row_json WHERE map_id=X`) — page boundaries invisible from here on, same as before batching | — |
| 8 | Run transform chain in `step_order`: resolve alias + period tokens → `runQueryToTable(sql, output_bq_table)` | — |
| 9 | Route each output: BQ export job → GCS CSV or JSON | — |
| 10 | `writeOutput(rpt_id, outpt_cd, output_ds, ...)` — inserts one RptOutput row per output step | — |
| 11 | `clearStagedData(rpt_id)` — DELETE all RptStageDa rows linked to this run's map_id(s) | — |
| 12 | Send email with GCS outputs as attachments (if email configured) | — |
| 13a | Success → `updateStatus(rpt_id, COMPLETED)` | → **COMPLETED** |
| 13b | Any failure → `updateStatus(rpt_id, FAILED)` | → **FAILED** |

### Output routing (step 7 detail)

| `sink_type` | What happens | Email attachment? |
|-------------|-------------|-------------------|
| `GCS` | BQ extract job → `gs://.../{reportName}_{periodId}_{runDate}.csv` | Yes — file attached |
| `BQ` | `SELECT * FROM result_table` → destination `bq_sink_table` (WRITE_TRUNCATE) | No |
| `API` | Query rows via `TO_JSON_STRING`, POST JSON array to `api_endpoint` | No |

---

## 5. Inspect runtime state

```sql
-- DaRefer: check download run lifecycle for the 'trades' source (DATA_SOURCE_DOWNLOAD)
SELECT da_id, srce_nm, vsn_no, per_id, fl_nm, sta_cd, bal_and_cntl_smry_tx, lst_updt_ts
FROM `my-gcp-project.pipeline_metadata.DaRefer`
WHERE srce_nm = 'trades' AND per_id = 202401
ORDER BY lst_updt_ts DESC
LIMIT 5;

-- DaRec: how many pages a download run wrote (replace 42 with actual da_id) — NOT the source
-- row count, since each page can hold up to 250 rows; see the next query for that.
SELECT COUNT(*) AS page_count, MIN(load_dt) AS load_dt
FROM `my-gcp-project.pipeline_metadata.DaRec`
WHERE da_id = 42;

-- DaRec: count and sample individual source rows for a run — un-nest every page first
SELECT COUNT(*) AS row_count
FROM `my-gcp-project.pipeline_metadata.DaRec`
CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx)) AS row_json
WHERE da_id = 42;

SELECT
  JSON_VALUE(row_json, '$.currency')                   AS currency,
  CAST(JSON_VALUE(row_json, '$.amount') AS FLOAT64)    AS amount,
  load_dt
FROM `my-gcp-project.pipeline_metadata.DaRec`
CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx)) AS row_json
WHERE da_id = 42
LIMIT 10;

-- RptRefer: check report run lifecycle
SELECT rpt_id, rpt_nm, per_id, rpt_ds, sta_cd, creat_ts, lst_updt_ts
FROM `my-gcp-project.pipeline_metadata.RptRefer`
WHERE rpt_nm = 'daily_trades_summary' AND per_id = 202401
ORDER BY lst_updt_ts DESC
LIMIT 5;

-- RptDaMap: which datasource runs were consumed by a report run (replace 1 with actual rpt_id)
SELECT m.map_id, m.rpt_id, m.da_id, d.srce_nm, d.vsn_no, d.sta_cd
FROM `my-gcp-project.pipeline_metadata.RptDaMap` m
JOIN `my-gcp-project.pipeline_metadata.DaRefer`  d ON d.da_id = m.da_id
WHERE m.rpt_id = 1;

-- RptOutput: outputs written by a report run
SELECT outpt_cd, rpt_dt, vsn_no, output_ds, rpt_id, lst_updt_ts
FROM `my-gcp-project.pipeline_metadata.RptOutput`
WHERE rpt_id = 1
ORDER BY rpt_dt DESC;
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
| 1 | Fetch source configs from `parameter_store` for (TRADING, trades, eod) | — |
| 2 | Fetch `parameter_store` row for (TRADING, trades, eod) | — |
| 3 | Validate required parameters present in BQ | — |
| 4 | Check DaRefer — skip if `sta_cd=COMPLETED` already exists (unless `--overrideDownload`) | — |
| 5 | `createCheckpoint('trades', '202401', '<bq-table-ref>')` → DaRefer row | → **LOADING** |
| 6 | Dataflow: read source → apply transforms → paginate at ≤250 rows/page → write each page as one DaRec row (`row_da_json_tx` = JSON array) | — |
| 7 | `waitUntilFinish()` | — |
| 8 | Un-nest every page (`CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx))`) then `COUNT(*)` + BnC SUM checks against individual rows | — |
| 9a | All checks pass → `updateStatus(COMPLETED, bncJson)` | → **COMPLETED** |
| 9b | BnC mismatch → `updateStatus(FAILED_BNC, bncJson)` → send failure email if `failure_email_to` configured | → **FAILED_BNC** |
| 9c | Infrastructure error → `updateStatus(FAILED, errorJson)` → send failure email if configured | → **FAILED** |

### Failure email fields in `parameters_val_json` (optional — omit to disable)

| Key | Example value | Notes |
|-----|--------------|-------|
| `failure_email_to` | `"ops@example.com,owner@example.com"` | Comma-separated. Required to enable email. |
| `failure_email_cc` | `"manager@example.com"` | Optional. |
| `failure_email_subject` | `"FAILED: {datasourceName} download for period {periodId}"` | Default used if absent. |
| `failure_email_body` | `"Status: {staCd}\nError: {errorMessage}\n\nBnC:\n{bncSummary}"` | Default used if absent. |
| `email_smtp_host` | `"smtp.gmail.com"` | Default `smtp.gmail.com`. |
| `email_smtp_port` | `"587"` | Default `587`. |
| `smtp_password_secret_id` | `"projects/my-project/secrets/smtp-pw/versions/latest"` | Secret Manager resource name. |
| `from_address` | `"pipeline-alerts@example.com"` | Required to enable email. |

Available body/subject tokens: `{datasourceName}`, `{periodId}`, `{staCd}`, `{errorMessage}`, `{bncSummary}`.

---

## 7. Expected GCS output (daily_trades_summary_202401_2024-01-31.csv)

```
currency,total_amount,trade_count
JPY,500000.0,1
USD,455000.0,3
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
  'TRADING', CURRENT_DATETIME(), 'setup_script'
);
```

---

## 9. Key option flags reference

### Process control

| Option | Default | Purpose |
|--------|---------|---------|
| `--processType` | required | `DATA_SOURCE_DOWNLOAD` or `REPORT_PROCESSING` |
| `--parentId` | — | Business group. Maps to `parameter_group_name` in `parameter_store` |
| `--periodId` | — | Period integer e.g. `202401` (MONTHLY) or `20240115` (DAILY) |
| `--jobRunId` | auto UUID | Correlation ID threaded through logs and checkpoint rows |

### Config tables (read-only)

| Option | Default | Purpose |
|--------|---------|---------|
| `--paramBqProject` | `--project` | GCP project for config tables |
| `--paramBqDataset` | `dw` | BQ dataset for `parameter_store` |
| `--paramStoreTable` | `parameter_store` | Parameter store table |

### Runtime tables (framework-managed)

| Option | Default | Purpose |
|--------|---------|---------|
| `--checkpointBqProject` | `--project` | GCP project for runtime tables |
| `--checkpointBqDataset` | `pipeline_metadata` | BQ dataset containing all runtime tables |
| `--daReferTable` | `DaRefer` | One row per DATA_SOURCE_DOWNLOAD run: LOADING → COMPLETED / FAILED_BNC / FAILED |
| `--daRecTable` | `DaRec` | All source rows as JSON blobs (Beam workers write here) |
| `--rptReferTable` | `RptRefer` | One row per REPORT_PROCESSING run: LOADING → COMPLETED / FAILED |
| `--rptDaMapTable` | `RptDaMap` | Maps each report run to the DaRefer.da_id(s) it consumed |
| `--rptStageDaTable` | `RptStageDa` | Transient staging: DaRec rows copied here before transforms; deleted after |
| `--rptOutputTable` | `RptOutput` | One row per output step produced by the report |

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
| `--customParamsJson` | — | Ad-hoc `{"key":"value"}` custom query tokens, resolved for both process types — see section 10 |

---

## 10. Custom query parameters (`--customParamsJson`)

The transform step's `query_params_json` (section 2) already supports custom `{token}`s stored
*in* the parameter_store row. `--customParamsJson` is the CLI-supplied equivalent, for a value
that should come from the invocation itself instead of being edited into stored config —
e.g. an Airflow DAG conf value, or an ad-hoc filter for a one-off run.

The sample trades data has a `desk` column (`FX` / `RATES`) that the transform step doesn't
currently filter on. To make it filterable per-run without editing `parameter_store`, add a
`{desk}` token to the transform's `query_template` (one-time parameter_store edit):

```sql
UPDATE `my-gcp-project.dw.parameter_store`
SET parameters_val_json = JSON_SET(
  parameters_val_json,
  '$.transforms[0].query_template',
  'SELECT JSON_VALUE(stage_ds_json_tx, \'$.currency\') AS currency, '
  || 'SUM(CAST(JSON_VALUE(stage_ds_json_tx, \'$.amount\') AS FLOAT64)) AS total_amount, '
  || 'COUNT(*) AS trade_count '
  || 'FROM {raw_trades} '
  || 'WHERE JSON_VALUE(stage_ds_json_tx, \'$.desk\') = \'{desk}\' '
  || 'GROUP BY currency ORDER BY total_amount DESC'
)
WHERE parameter_group_name = 'TRADING' AND parameter_data_source = 'eod'
  AND parameter_name = 'daily_trades_summary';
```

Now every run of this report must supply `{desk}` or the query fails at BigQuery execution time
(unresolved `{desk}` left as a literal string) — supply it via `--customParamsJson`:

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
  --customParamsJson='{"desk":"FX"}' \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=dw \
  --paramStoreTable=parameter_store \
  --checkpointBqProject=my-gcp-project \
  --checkpointBqDataset=pipeline_metadata
```

With `--customParamsJson='{"desk":"FX"}'`, only the FX-desk trades (T001, T002, T004, T006,
T007 — see the seed data in section 2) are aggregated:

```
currency,total_amount,trade_count
JPY,500000.0,1
USD,360000.0,2
EUR,80000.0,1
GBP,60000.0,1
```

Re-run with `--customParamsJson='{"desk":"RATES"}'` to get the RATES-desk-only breakdown instead
— no `parameter_store` edit needed between runs, only the CLI flag changes.
