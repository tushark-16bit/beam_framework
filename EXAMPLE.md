# End-to-End Example: BQ Parameter Store → GCS Report

This walkthrough shows how the framework fetches all configuration from BigQuery,
runs a transform query, and exports the result to GCS.

## Architecture overview

```
required_parameters_index (BQ)   ← "which keys does this report need?"
        ↓
parameter_store (BQ)             ← "what are the values for this period?"
        ↓
ExampleWorkflow                  ← resolves tokens, orchestrates execution
        ↓
BigQueryJobService               ← runs transform query → BQ output table
        ↓
BigQueryJobService.exportToCsv   ← BQ extract job → GCS CSV file
```

## 1. BQ table DDL (run once in your GCP project)

```sql
-- Dataset: pipeline_config (create this first)
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.pipeline_config`;

-- Key-value parameter store
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_config.parameter_store` (
  process_name    STRING NOT NULL,
  subprocess_name STRING NOT NULL,
  period_id       STRING NOT NULL,
  param_key       STRING NOT NULL,
  param_value     STRING,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Required-parameters index: declares which keys each process needs
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_config.required_parameters_index` (
  process_name    STRING NOT NULL,
  subprocess_name STRING NOT NULL,
  param_key       STRING NOT NULL,
  is_required     BOOL   NOT NULL DEFAULT TRUE,
  description     STRING
);

-- Source data table for the example (raw trades)
CREATE TABLE IF NOT EXISTS `my-gcp-project.raw_data.trades` (
  trade_id    STRING,
  currency    STRING,
  amount      FLOAT64,
  trade_date  DATE,
  desk        STRING
);

-- Output BQ table (framework will create/truncate via WRITE_TRUNCATE)
CREATE TABLE IF NOT EXISTS `my-gcp-project.reports.daily_trades_summary` (
  currency     STRING,
  total_amount FLOAT64,
  trade_count  INT64
);
```

## 2. Seed sample data

```sql
-- Raw trades (source data)
INSERT INTO `my-gcp-project.raw_data.trades` VALUES
  ('T001', 'USD', 150000.00, DATE '2024-01-05', 'FX'),
  ('T002', 'EUR',  80000.00, DATE '2024-01-07', 'FX'),
  ('T003', 'USD',  95000.00, DATE '2024-01-10', 'RATES'),
  ('T004', 'GBP',  60000.00, DATE '2024-01-12', 'FX'),
  ('T005', 'EUR',  40000.00, DATE '2024-01-15', 'RATES'),
  ('T006', 'USD', 210000.00, DATE '2024-01-20', 'FX'),
  ('T007', 'JPY', 500000.00, DATE '2024-01-22', 'FX'),
  ('T008', 'GBP',  35000.00, DATE '2024-01-28', 'RATES');

-- ── required_parameters_index ──────────────────────────────────────────────
-- Tells the framework which param keys it must read for this report/subprocess
INSERT INTO `my-gcp-project.pipeline_config.required_parameters_index` VALUES
  ('daily_trades_summary', 'eod', 'source_bq_table',      TRUE,  'Fully-qualified BQ table with raw trades'),
  ('daily_trades_summary', 'eod', 'transform_query',      TRUE,  'Aggregation SQL; supports {periodStart}/{periodEnd} tokens'),
  ('daily_trades_summary', 'eod', 'transform_output_table', TRUE, 'BQ table to materialise the transform result into'),
  ('daily_trades_summary', 'eod', 'output_gcs_path',      TRUE,  'GCS directory for the exported CSV'),
  ('daily_trades_summary', 'eod', 'output_file_name',     TRUE,  'CSV file name');

-- ── parameter_store ────────────────────────────────────────────────────────
-- Actual values for the January 2024 run
INSERT INTO `my-gcp-project.pipeline_config.parameter_store` VALUES
  -- Source BQ table where raw trades live
  ('daily_trades_summary', 'eod', '2024-01',
   'source_bq_table',
   'my-gcp-project.raw_data.trades',
   CURRENT_TIMESTAMP()),

  -- Aggregation query; {periodStart} / {periodEnd} are resolved at runtime
  ('daily_trades_summary', 'eod', '2024-01',
   'transform_query',
   'SELECT currency, SUM(amount) AS total_amount, COUNT(*) AS trade_count
    FROM `my-gcp-project.raw_data.trades`
    WHERE trade_date BETWEEN DATE ''{periodStart}'' AND DATE ''{periodEnd}''
    GROUP BY currency
    ORDER BY total_amount DESC',
   CURRENT_TIMESTAMP()),

  -- Where to materialise the transform result in BQ
  ('daily_trades_summary', 'eod', '2024-01',
   'transform_output_table',
   'my-gcp-project.reports.daily_trades_summary',
   CURRENT_TIMESTAMP()),

  -- GCS output directory
  ('daily_trades_summary', 'eod', '2024-01',
   'output_gcs_path',
   'gs://my-bucket/reports/daily_trades/',
   CURRENT_TIMESTAMP()),

  -- File name for the exported CSV
  ('daily_trades_summary', 'eod', '2024-01',
   'output_file_name',
   'daily_trades_summary_2024_01.csv',
   CURRENT_TIMESTAMP());
```

## 3. Verify the parameter setup

```sql
-- Check required keys are registered
SELECT * FROM `my-gcp-project.pipeline_config.required_parameters_index`
WHERE process_name = 'daily_trades_summary' AND subprocess_name = 'eod';

-- Check values are present for the period
SELECT param_key, param_value
FROM `my-gcp-project.pipeline_config.parameter_store`
WHERE process_name = 'daily_trades_summary'
  AND subprocess_name = 'eod'
  AND period_id = '2024-01'
ORDER BY param_key;
```

## 4. Run the example

```bash
mvn -pl beam-runner exec:java \
  -Dexec.mainClass=com.yourco.beam.runner.example.ExampleWorkflow \
  -Dexec.args="
    --project=my-gcp-project
    --paramBqProject=my-gcp-project
    --paramBqDataset=pipeline_config
    --paramStoreTable=parameter_store
    --paramRequiredTable=required_parameters_index
    --reportName=daily_trades_summary
    --reportSubprocess=eod
    --periodId=2024-01
    --periodStart=2024-01-01
    --periodEnd=2024-01-31
    --processType=REPORT_PROCESSING
    --sinkType=GCS"
```

## 5. What happens step-by-step

| Step | What runs | Where |
|------|-----------|-------|
| 1 | `SELECT param_key FROM required_parameters_index WHERE ...` | BigQuery |
| 2 | `SELECT param_key, param_value FROM parameter_store WHERE ... AND param_key IN UNNEST(@keys)` | BigQuery |
| 3 | Token substitution: `{periodStart}` → `2024-01-01` | Driver JVM |
| 4 | BQ query job: aggregation SQL → `reports.daily_trades_summary` (WRITE_TRUNCATE) | BigQuery |
| 5 | BQ extract job: `reports.daily_trades_summary` → `gs://my-bucket/reports/daily_trades/daily_trades_summary_2024_01.csv` | BigQuery |

## 6. Expected GCS output (daily_trades_summary_2024_01.csv)

```
currency,total_amount,trade_count
USD,455000.0,3
JPY,500000.0,1
EUR,120000.0,2
GBP,95000.0,2
```

## 7. Adding a new period

```sql
-- 2024-02 run — only the values change, index stays the same
INSERT INTO `my-gcp-project.pipeline_config.parameter_store` VALUES
  ('daily_trades_summary', 'eod', '2024-02', 'source_bq_table',       'my-gcp-project.raw_data.trades', CURRENT_TIMESTAMP()),
  ('daily_trades_summary', 'eod', '2024-02', 'transform_query',       '<same query>', CURRENT_TIMESTAMP()),
  ('daily_trades_summary', 'eod', '2024-02', 'transform_output_table','my-gcp-project.reports.daily_trades_summary', CURRENT_TIMESTAMP()),
  ('daily_trades_summary', 'eod', '2024-02', 'output_gcs_path',       'gs://my-bucket/reports/daily_trades/', CURRENT_TIMESTAMP()),
  ('daily_trades_summary', 'eod', '2024-02', 'output_file_name',      'daily_trades_summary_2024_02.csv', CURRENT_TIMESTAMP());
```

## 8. Key option flags reference

| Option | Default | Purpose |
|--------|---------|---------|
| `--paramBqProject` | `--project` | GCP project for parameter BQ tables |
| `--paramBqDataset` | `pipeline_config` | BQ dataset containing parameter tables |
| `--paramStoreTable` | `parameter_store` | Generic key-value store table name |
| `--paramRequiredTable` | `required_parameters_index` | Required-params index table name |
| `--paramSourceConfigTable` | `source_config` | Source config table (for alias registry) |
| `--reportName` | — | Report name key in parameter tables |
| `--reportSubprocess` | `default` | Subprocess variant |
| `--periodId` | — | Period identifier (used as BQ lookup key) |
| `--periodStart` | — | Substituted into `{periodStart}` token in queries |
| `--periodEnd` | — | Substituted into `{periodEnd}` token in queries |
