# End-to-End Example: BQ Parameter Store → Checkpoint → GCS Report

This walkthrough shows the full lifecycle: parameter fetch, checkpoint tracking,
BQ transform, and GCS export.

Two ways to run:

| Path | Class | Checkpoints? | Use when |
|------|-------|-------------|----------|
| **ExampleWorkflow** | `ExampleWorkflow` | No | Smoke-test the parameter-store plumbing directly |
| **Full pipeline** | `Main` via `ReportPipelineFactory` | Yes | Production-style run with LOADING → COMPLETED tracking |

---

## 1. BQ table DDL (run once in your GCP project)

### Config tables

```sql
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.pipeline_config`;

-- Parameter store: one row per named parameter group
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_config.parameter_store` (
  ParameterName       STRING    NOT NULL,
  ParameterGroupName  STRING    NOT NULL,
  ParameterDataSource STRING    NOT NULL,
  SchemaOfJson        STRING,               -- {"field": {"required": true/false, "type": "string"}}
  ParametersValJson   STRING,               -- {"field": "value", ...}
  EditGrpNm           STRING,
  LastUpdtTs          TIMESTAMP,
  LstUpdateUserId     STRING
);

-- Source config: one row per (parent_id, datasource_name, subprocess_name, period_id).
-- parent_id  = top-level business group (e.g. TRADING).
-- datasource_name = the data source name (child within the group).
-- subprocess_name = variant within the datasource (e.g. EOD, INTRADAY).
-- period_id  = the period this config applies to (e.g. 2024-01-15).
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_config.source_config` (
  parent_id           STRING    NOT NULL,
  datasource_name     STRING    NOT NULL,
  subprocess_name     STRING    NOT NULL,
  period_id           STRING    NOT NULL,
  source_type         STRING    NOT NULL,   -- BQ | API | FILE
  -- BQ source columns
  bq_project_id       STRING,
  bq_dataset          STRING,
  bq_table            STRING,
  bq_query            STRING,
  query_params_json   STRING,               -- {"key": "value"} token overrides
  -- API source columns
  api_endpoint        STRING,
  api_auth_type       STRING,
  api_auth_secret_id  STRING,
  api_headers_json    STRING,
  api_query_params_json STRING,
  api_pagination_enabled BOOL,
  api_pagination_strategy STRING,
  api_page_size       INT64,
  api_next_page_field STRING,
  api_data_array_field STRING,
  -- FILE source columns
  file_type           STRING,
  file_location       STRING,
  file_prefix         STRING,
  file_suffix         STRING,
  file_delimiter      STRING,
  file_has_header     BOOL,
  file_sheet_index    INT64,
  -- Transform + validation
  source_transforms_json STRING,            -- JSON array of LOOKUP/GROUP_BY/SORT_BY configs
  min_row_count       INT64,
  max_row_count       INT64,
  required_headers_json STRING,
  bnc_rules_json      STRING                -- [{"field":"amount","expectedTotal":5000000}]
);

-- Source data table for the example (raw trades)
CREATE TABLE IF NOT EXISTS `my-gcp-project.raw_data.trades` (
  trade_id    STRING,
  currency    STRING,
  amount      FLOAT64,
  trade_date  DATE,
  desk        STRING
);

-- Output BQ table (framework will WRITE_TRUNCATE via BigQueryJobService)
CREATE TABLE IF NOT EXISTS `my-gcp-project.reports.daily_trades_summary` (
  currency     STRING,
  total_amount FLOAT64,
  trade_count  INT64
);
```

### Runtime tables (checkpoint + record)

```sql
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.pipeline_metadata`;

-- One row per pipeline run (DATA_SOURCE_DOWNLOAD source or REPORT_PROCESSING report).
-- Created in LOADING state before the pipeline starts; updated to COMPLETED/FAILED* after.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.data_source_checkpoints` (
  dataSourceId      INT64     NOT NULL,   -- surrogate PK: MAX(dataSourceId)+1 at run time
  srcName           STRING    NOT NULL,   -- datasource name or report name
  vsnNo             INT64     NOT NULL,   -- rerun counter per (srcName, PerId): 1, 2, 3 ...
  PerId             STRING    NOT NULL,   -- periodId passed on the CLI
  DSNm              STRING,               -- human-readable source location (BQ table / GCS path)
  BalAndCntlSmryTx  STRING,               -- JSON: {srcCount, dstCount, srcAmount_X, dstAmount_X, status}
  StaCd             STRING    NOT NULL,   -- LOADING | COMPLETED | FAILED_BNC | FAILED
  CreatedTs         TIMESTAMP NOT NULL,
  LstUpdtTs         TIMESTAMP NOT NULL
);

-- All data rows from every source and every report run, stored as JSON blobs.
-- Replaces per-source output BQ tables. Filter by dataSourceId to get one run's rows.
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_metadata.data_source_records` (
  RecId         STRING    NOT NULL,   -- UUID per row
  dataSourceId  INT64     NOT NULL,   -- FK → data_source_checkpoints.dataSourceId
  RowDSJsonTx   STRING,               -- full row serialised as JSON
  LoadDt        DATE      NOT NULL,   -- partition column; set once per run (not per element)
  LstUpdtTs     TIMESTAMP NOT NULL
)
PARTITION BY LoadDt;
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

-- Parameter store row: all five fields required for this report
INSERT INTO `my-gcp-project.pipeline_config.parameter_store`
  (ParameterName, ParameterGroupName, ParameterDataSource, SchemaOfJson, ParametersValJson,
   EditGrpNm, LastUpdtTs, LstUpdateUserId)
VALUES (
  'daily_trades_summary',
  'REPORT_PROCESSING',
  'eod',
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
```

---

## 3. Verify the parameter setup

```sql
SELECT
  ParameterName,
  ParameterGroupName,
  ParameterDataSource,
  JSON_QUERY(SchemaOfJson,      '$') AS schema,
  JSON_QUERY(ParametersValJson, '$') AS params
FROM `my-gcp-project.pipeline_config.parameter_store`
WHERE ParameterGroupName  = 'REPORT_PROCESSING'
  AND ParameterDataSource = 'eod'
  AND ParameterName       = 'daily_trades_summary';
```

---

## 4a. Run with ExampleWorkflow (no checkpoints)

ExampleWorkflow bypasses the checkpoint lifecycle entirely — it directly fetches
parameters, runs the BQ transform, and exports to GCS. Useful for smoke-testing
the parameter-store setup without needing the checkpoint tables.

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
    --periodId=2024-01
    --periodStart=2024-01-01
    --periodEnd=2024-01-31
    --processType=REPORT_PROCESSING"
```

### What ExampleWorkflow does step-by-step

| Step | Action | Where |
|------|--------|-------|
| 1 | `SELECT ParametersValJson, SchemaOfJson FROM parameter_store WHERE ...` | BigQuery |
| 2 | Parse `SchemaOfJson` → find required fields; parse `ParametersValJson` → `Map<String,String>` | Driver JVM |
| 3 | Validate all required fields present — throws `IllegalStateException` if any missing | Driver JVM |
| 4 | Token substitution: `{periodStart}` → `2024-01-01`, `{source_bq_table}` → actual table | Driver JVM |
| 5 | BQ query job: aggregation SQL → `reports.daily_trades_summary` (WRITE_TRUNCATE) | BigQuery |
| 6 | BQ extract job: `reports.daily_trades_summary` → `gs://my-bucket/.../daily_trades_summary_2024-01.csv` | BigQuery |

---

## 4b. Run with Main (full lifecycle — checkpoints + records)

`Main` drives `ReportPipelineFactory`, which adds the full checkpoint lifecycle:
LOADING checkpoint → datasource availability check → transform chain → COMPLETED/FAILED.

```bash
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --processType=REPORT_PROCESSING \
  --project=my-gcp-project \
  --parentId=TRADING \
  --reportName=daily_trades_summary \
  --reportSubprocess=eod \
  --periodId=2024-01 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=pipeline_config \
  --paramStoreTable=parameter_store \
  --checkpointBqProject=my-gcp-project \
  --checkpointBqDataset=pipeline_metadata \
  --checkpointBqTable=data_source_checkpoints \
  --recordBqTable=data_source_records
```

### What ReportPipelineFactory does step-by-step

| Step | Action | Checkpoint state |
|------|--------|-----------------|
| 1 | Load `ReportConfig` from BQ | — |
| 2 | `createCheckpoint(reportName, periodId, reportName)` | → **LOADING** |
| 3 | Check all required datasources have `StaCd = COMPLETED` for this period | — |
| 4 | Build alias registry: datasource alias → record-table subquery | — |
| 5 | Run transform chain (BQ jobs, each materialised to a BQ table) | — |
| 6 | Export outputs to GCS; send email if configured | — |
| 7a | Success → `updateStatus(COMPLETED, null)` | → **COMPLETED** |
| 7b | Any failure → `updateStatus(FAILED, null)` | → **FAILED** |

### Inspect the checkpoint after the run

```sql
SELECT dataSourceId, srcName, vsnNo, PerId, StaCd, LstUpdtTs
FROM `my-gcp-project.pipeline_metadata.data_source_checkpoints`
WHERE srcName = 'daily_trades_summary'
ORDER BY LstUpdtTs DESC
LIMIT 5;
```

---

## 5. Expected GCS output (daily_trades_summary_2024-01.csv)

```
currency,total_amount,trade_count
USD,455000.0,3
JPY,500000.0,1
EUR,120000.0,2
GBP,95000.0,2
```

---

## 6. Adding a second report

```sql
INSERT INTO `my-gcp-project.pipeline_config.parameter_store`
  (ParameterName, ParameterGroupName, ParameterDataSource, SchemaOfJson, ParametersValJson,
   EditGrpNm, LastUpdtTs, LstUpdateUserId)
VALUES (
  'monthly_pnl_report',
  'REPORT_PROCESSING',
  'monthly',
  JSON '{"pnl_source_table": {"required": true, "type": "string"}, "output_gcs_path": {"required": true, "type": "string"}}',
  JSON '{"pnl_source_table": "my-gcp-project.raw_data.pnl", "output_gcs_path": "gs://my-bucket/reports/pnl/"}',
  'TRADING', CURRENT_TIMESTAMP(), 'setup_script'
);
```

---

## 7. Key option flags reference

| Option | Default | Purpose |
|--------|---------|---------|
| `--parentId` | — | Top-level business group. Maps to `ParameterGroupName` in `parameter_store` and `parent_id` in `source_config`. Example: `TRADING`, `RISK` |
| `--paramBqProject` | `--project` | GCP project for the `parameter_store` table |
| `--paramBqDataset` | `pipeline_config` | BQ dataset containing `parameter_store` |
| `--paramStoreTable` | `parameter_store` | Table name (keyed by `ParameterName`, `ParameterGroupName`, `ParameterDataSource`) |
| `--paramSourceConfigTable` | `source_config` | Source config table used by `DATA_SOURCE_DOWNLOAD` |
| `--checkpointBqProject` | `--project` | GCP project for the checkpoint and record tables |
| `--checkpointBqDataset` | `pipeline_metadata` | BQ dataset for both `data_source_checkpoints` and `data_source_records` |
| `--checkpointBqTable` | `data_source_checkpoints` | Checkpoint table name |
| `--recordBqTable` | `data_source_records` | Record table name (all source rows stored as JSON blobs) |
| `--reportName` | — | Maps to `ParameterName` column; also used as `srcName` in the checkpoint row |
| `--reportSubprocess` | `default` | Maps to `ParameterDataSource` column |
| `--periodStart` | — | Substituted into `{periodStart}` token in query values |
| `--periodEnd` | — | Substituted into `{periodEnd}` token in query values |
| `--periodId` | — | Substituted into `{periodId}` token; used as `PerId` in the checkpoint row |
| `--overrideDownload` | `false` | Re-process even if a COMPLETED checkpoint exists for this (srcName, PerId) |
