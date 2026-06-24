# End-to-End Example: BQ Parameter Store → GCS Report

This walkthrough shows how the framework fetches all configuration from a single
`parameter_store` BigQuery row, validates required fields via `SchemaOfJson`,
runs a transform query, and exports the result to GCS.

## Architecture overview

```
parameter_store (BQ row)
  ├── SchemaOfJson       → declares which fields are required
  └── ParametersValJson  → holds all param values as a JSON blob
           ↓
ExampleWorkflow          ← resolves tokens, orchestrates execution
           ↓
BigQueryJobService       ← runs transform query → BQ output table
           ↓
BigQueryJobService.exportToCsv  ← BQ extract job → GCS CSV file
```

## 1. BQ table DDL (run once in your GCP project)

```sql
-- Dataset: pipeline_config (create this first)
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.pipeline_config`;

-- Parameter store: one row per named parameter group
CREATE TABLE IF NOT EXISTS `my-gcp-project.pipeline_config.parameter_store` (
  ParameterName       STRING    NOT NULL,   -- grouping name,   e.g. "daily_trades_summary"
  ParameterGroupName  STRING    NOT NULL,   -- parent process,  e.g. "REPORT_PROCESSING"
  ParameterDataSource STRING    NOT NULL,   -- subprocess,      e.g. "eod"
  SchemaOfJson        STRING,               -- JSON object: {"field": {"required": true/false, "type": "string"}}
  ParametersValJson   STRING,               -- JSON object: {"field": "value", ...}
  EditGrpNm           STRING,               -- temp/edit group flag
  LastUpdtTs          TIMESTAMP,
  LstUpdateUserId     STRING
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

-- ── parameter_store ─────────────────────────────────────────────────────────
-- One row for the "daily_trades_summary" report group.
-- SchemaOfJson declares which fields are required.
-- ParametersValJson holds the actual values.
INSERT INTO `my-gcp-project.pipeline_config.parameter_store`
  (ParameterName, ParameterGroupName, ParameterDataSource, SchemaOfJson, ParametersValJson,
   EditGrpNm, LastUpdtTs, LstUpdateUserId)
VALUES (
  'daily_trades_summary',
  'REPORT_PROCESSING',
  'eod',

  -- SchemaOfJson: all five fields are required for this report
  JSON '{
    "source_bq_table":        {"required": true,  "type": "string"},
    "transform_query":        {"required": true,  "type": "string"},
    "transform_output_table": {"required": true,  "type": "string"},
    "output_gcs_path":        {"required": true,  "type": "string"},
    "output_file_name":       {"required": true,  "type": "string"}
  }',

  -- ParametersValJson: actual values; {tokens} are resolved at runtime by ExampleWorkflow
  JSON '{
    "source_bq_table":        "my-gcp-project.raw_data.trades",
    "transform_query":        "SELECT currency, SUM(amount) AS total_amount, COUNT(*) AS trade_count FROM `my-gcp-project.raw_data.trades` WHERE trade_date BETWEEN DATE \"{periodStart}\" AND DATE \"{periodEnd}\" GROUP BY currency ORDER BY total_amount DESC",
    "transform_output_table": "my-gcp-project.reports.daily_trades_summary",
    "output_gcs_path":        "gs://my-bucket/reports/daily_trades/",
    "output_file_name":       "daily_trades_summary_{periodId}.csv"
  }',

  'TRADING',
  CURRENT_TIMESTAMP(),
  'setup_script'
);
```

## 3. Verify the parameter setup

```sql
-- Check the row is present and both JSON columns are populated
SELECT
  ParameterName,
  ParameterGroupName,
  ParameterDataSource,
  JSON_QUERY(SchemaOfJson,      '$')  AS schema,
  JSON_QUERY(ParametersValJson, '$')  AS params
FROM `my-gcp-project.pipeline_config.parameter_store`
WHERE ParameterGroupName  = 'REPORT_PROCESSING'
  AND ParameterDataSource = 'eod'
  AND ParameterName       = 'daily_trades_summary';

-- List required fields declared in SchemaOfJson
SELECT key, JSON_VALUE(value, '$.required') AS is_required
FROM `my-gcp-project.pipeline_config.parameter_store`,
UNNEST(JSON_KEYS(SchemaOfJson)) AS key
WHERE ParameterName = 'daily_trades_summary';
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
    --reportName=daily_trades_summary
    --reportSubprocess=eod
    --periodId=2024-01
    --periodStart=2024-01-01
    --periodEnd=2024-01-31
    --processType=REPORT_PROCESSING"
```

## 5. What happens step-by-step

| Step | What runs | Where |
|------|-----------|-------|
| 1 | `SELECT ParametersValJson, SchemaOfJson FROM parameter_store WHERE ParameterGroupName=... AND ParameterDataSource=... AND ParameterName=...` | BigQuery |
| 2 | Parse `SchemaOfJson` → find fields with `"required": true` | Driver JVM |
| 3 | Parse `ParametersValJson` → `Map<String,String>` | Driver JVM |
| 4 | Validate all required fields present — throws `IllegalStateException` if any missing | Driver JVM |
| 5 | Token substitution: `{periodStart}` → `2024-01-01`, `{periodId}` → `2024-01` | Driver JVM |
| 6 | BQ query job: aggregation SQL → `reports.daily_trades_summary` (WRITE_TRUNCATE) | BigQuery |
| 7 | BQ extract job: `reports.daily_trades_summary` → `gs://my-bucket/reports/daily_trades/daily_trades_summary_2024-01.csv` | BigQuery |

## 6. Expected GCS output (daily_trades_summary_2024-01.csv)

```
currency,total_amount,trade_count
USD,455000.0,3
JPY,500000.0,1
EUR,120000.0,2
GBP,95000.0,2
```

## 7. Adding a new parameter group (e.g. a second report)

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
  'TRADING',
  CURRENT_TIMESTAMP(),
  'setup_script'
);
```

## 8. Key option flags reference

| Option | Default | Purpose |
|--------|---------|---------|
| `--paramBqProject` | `--project` | GCP project for the `parameter_store` table |
| `--paramBqDataset` | `pipeline_config` | BQ dataset containing `parameter_store` |
| `--paramStoreTable` | `parameter_store` | Table name (matches `ParameterName`, `ParameterGroupName`, `ParameterDataSource` columns) |
| `--paramSourceConfigTable` | `source_config` | Source config table used by DATA_SOURCE_DOWNLOAD |
| `--reportName` | — | Maps to `ParameterName` column in `parameter_store` |
| `--reportSubprocess` | `default` | Maps to `ParameterDataSource` column |
| `--periodStart` | — | Substituted into `{periodStart}` token in query values |
| `--periodEnd` | — | Substituted into `{periodEnd}` token in query values |
| `--periodId` | — | Substituted into `{periodId}` token in file names / queries |
