# beam-orchestrator

Standalone orchestration JAR. Triggered by an Airflow DAG, it reads `parameter_store` from
BigQuery to discover which sources and reports are scheduled for a given run, creates task
records in a BigQuery table, and writes a JSON manifest to GCS so the DAG can fan out
individual `beam-runner` invocations.

**No Apache Beam dependency.** No dependency on any sibling `beam-*` module. It is a fully
self-contained JAR that only requires the GCP client libraries and Jackson.

---

## Contents

- [Module structure](#module-structure)
- [Run command](#run-command)
- [parameter_store opt-in fields](#parameter_store-opt-in-fields)
- [orchestrator_tasks DDL](#orchestrator_tasks-ddl)
- [Manifest JSON format](#manifest-json-format)
- [Swapping implementations](#swapping-implementations)

---

## Module structure

```
beam-orchestrator/
└── src/main/java/com/yourco/beam/orchestrator/
    ├── OrchestratorMain.java          Entry point — wires impls and runs
    ├── OrchestratorOptions.java       --key=value CLI parser
    ├── Orchestrator.java              Core logic (interface-only deps)
    ├── model/
    │   ├── ResolvedPeriod.java        Period value object
    │   ├── RunSpec.java               One schedulable item
    │   └── TaskItem.java              Persisted task record
    ├── period/
    │   ├── PeriodResolver.java        Extension point interface
    │   └── StandardPeriodResolver.java DAILY / MONTHLY / WEEKLY
    ├── schedule/
    │   ├── RunScheduleResolver.java   Extension point interface
    │   └── BigQueryRunScheduleResolver.java Queries parameter_store
    ├── task/
    │   ├── TaskRepository.java        Extension point interface
    │   └── BigQueryTaskRepository.java BQ streaming insert
    └── manifest/
        ├── ManifestWriter.java        Extension point interface
        └── GcsManifestWriter.java     Writes JSON to GCS
```

---

## Run command

```bash
java -jar beam-orchestrator/target/beam-orchestrator-bundled.jar \
  --parentId=TRADING \
  --runDate=2024-01-31 \
  --frequency=MONTHLY \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=dw \
  --taskBqDataset=pipeline_orchestration \
  --manifestGcsBucket=my-bucket
```

### All CLI flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--parentId` | Yes | — | Business group; maps to `parameter_group_name` in `parameter_store` |
| `--runDate` | Yes | — | `yyyy-MM-dd`; the date this orchestrator run covers |
| `--frequency` | Yes | — | `DAILY`, `MONTHLY`, or `WEEKLY` |
| `--paramBqProject` | Yes | — | GCP project hosting `parameter_store` |
| `--runId` | No | `{parentId}-{frequency}-{runDate}-{uuid[:8]}` | Unique ID for this orchestrator invocation |
| `--paramBqDataset` | No | `dw` | BQ dataset containing `parameter_store` |
| `--paramStoreTable` | No | `parameter_store` | Parameter store table name |
| `--taskBqProject` | No | `paramBqProject` | GCP project for the task table |
| `--taskBqDataset` | No | `pipeline_orchestration` | BQ dataset for the task table |
| `--taskTable` | No | `orchestrator_tasks` | Task table name |
| `--manifestGcsBucket` | No | `null` (skip GCS) | GCS bucket for manifest (no `gs://` prefix) |
| `--manifestGcsPath` | No | `manifests/{runId}/tasks.json` | GCS object path template |

**Path template tokens**: `{runId}`, `{parentId}`, `{frequency}`, `{runDate}`.

---

## parameter_store opt-in fields

A `parameter_store` row opts in to orchestration by including these keys in
`parameters_val_json`. All other keys in that JSON are forwarded to the task as `extra_params`.

| Key | Required | Values | Default | Description |
|---|---|---|---|---|
| `run_type` | **Yes** | `DATA_SOURCE_DOWNLOAD` or `REPORT_PROCESSING` | — | Opts this row in. Rows without `run_type` are ignored by the orchestrator. |
| `enabled` | No | `true` / `false` | `true` | Set `false` to temporarily disable a source/report without removing its row. |
| `frequency` | No | `DAILY`, `MONTHLY`, `WEEKLY`, `ALL` | `ALL` | Limits scheduling to runs of this frequency. `ALL` (or absent) matches every frequency. |
| `run_order` | No | Integer string, e.g. `"10"` | `0` | Controls task ordering in the manifest. Lower values run first. |

### Example INSERT

```sql
INSERT INTO `my-project.dw.parameter_store`
  (parameter_group_name, parameter_data_source, parameter_name,
   schema_of_json, parameters_val_json)
VALUES
  -- DATA_SOURCE_DOWNLOAD source — opts in at MONTHLY frequency, order 10
  ('TRADING', 'eod', 'trades',
   'source_type,run_type,enabled,frequency,run_order',
   JSON '{"source_type":"BQ","bq_project_id":"my-project","bq_dataset":"raw","bq_table":"trades",
          "run_type":"DATA_SOURCE_DOWNLOAD","enabled":"true","frequency":"MONTHLY","run_order":"10"}'),

  -- REPORT_PROCESSING report — opts in at all frequencies, order 20
  ('TRADING', 'eod', 'daily_trades_report',
   'run_type,enabled,run_order',
   JSON '{"run_type":"REPORT_PROCESSING","enabled":"true","run_order":"20",
          "override_key":false,
          "datasources":[{"datasource_name":"trades","datasource_subprocess":"eod",
                          "transform_alias":"raw_trades","is_required":true}],
          "transforms":[{"step_order":1,"step_name":"aggregate","input_alias":"raw_trades",
                         "output_alias":"summary",
                         "query_template":"SELECT ... FROM {raw_trades}",
                         "output_bq_table":"my-project.dw.trades_summary","query_params_json":{}}],
          "outputs":[{"output_order":1,"input_alias":"summary","sink_type":"GCS",
                      "output_format":"CSV","gcs_path":"gs://my-bucket/reports/",
                      "file_prefix":"","file_suffix":".csv","include_header":true}],
          "email":{"to_list":["analyst@example.com"],"cc_list":[],
                   "subject_template":"Report {periodId}","body_template":"Attached."}}');
```

---

## orchestrator_tasks DDL

BigQuery table where the orchestrator records each task it creates.

```sql
CREATE TABLE IF NOT EXISTS `my-project.pipeline_orchestration.orchestrator_tasks`
(
  task_id          STRING    NOT NULL,   -- UUID; BQ streaming dedup key
  run_id           STRING    NOT NULL,   -- ties all tasks of one orchestrator invocation together
  run_type         STRING    NOT NULL,   -- DATA_SOURCE_DOWNLOAD | REPORT_PROCESSING
  parent_id        STRING    NOT NULL,   -- --parentId flag value
  name             STRING    NOT NULL,   -- datasourceName or reportName
  subprocess       STRING,               -- subprocessName or reportSubprocess
  period_id        INT64     NOT NULL,   -- YYYYMMDD / YYYYMM / YYYYWW
  period_start     DATE      NOT NULL,
  period_end       DATE      NOT NULL,
  run_date         DATE      NOT NULL,
  run_order        INT64     NOT NULL,
  status           STRING    NOT NULL,   -- PENDING | RUNNING | COMPLETED | FAILED | SKIPPED
  extra_params_json STRING,              -- JSON blob of non-orchestration keys from parameter_store
  metadata_json    STRING,               -- reserved for future use
  created_at       DATETIME  NOT NULL
);
```

---

## Manifest JSON format

Written to GCS after tasks are persisted. The Airflow DAG reads this to build its fan-out.

```json
{
  "runId":       "TRADING-MONTHLY-2024-01-31-abc12345",
  "generatedAt": "2024-01-31T06:00:00Z",
  "parentId":    "TRADING",
  "frequency":   "MONTHLY",
  "runDate":     "2024-01-31",
  "period": {
    "periodId":    202401,
    "periodStart": "2024-01-01",
    "periodEnd":   "2024-01-31"
  },
  "tasks": [
    {
      "taskId":      "550e8400-e29b-41d4-a716-446655440000",
      "runType":     "DATA_SOURCE_DOWNLOAD",
      "name":        "trades",
      "subprocess":  "eod",
      "periodId":    202401,
      "periodStart": "2024-01-01",
      "periodEnd":   "2024-01-31",
      "runDate":     "2024-01-31",
      "runOrder":    10,
      "status":      "PENDING",
      "extraParams": {"source_type": "BQ", "bq_project_id": "my-project"}
    }
  ]
}
```

---

## Swapping implementations

All four extension points are interfaces. To replace an implementation, change `OrchestratorMain`
only — `Orchestrator` itself never changes.

| To change | Replace in `OrchestratorMain` |
|---|---|
| Period calculation logic (fiscal year, business days, etc.) | `StandardPeriodResolver` → your `PeriodResolver` impl |
| Schedule source (YAML file, REST API, etc.) | `BigQueryRunScheduleResolver` → your `RunScheduleResolver` impl |
| Task storage (Pub/Sub, Cloud SQL, Firestore, etc.) | `BigQueryTaskRepository` → your `TaskRepository` impl |
| Manifest output (S3, webhook, skip entirely) | `GcsManifestWriter` → your `ManifestWriter` impl, or pass `null` to skip |

`PeriodResolver`, `RunScheduleResolver`, and `ManifestWriter` are all `@FunctionalInterface` —
they can be replaced with a lambda for testing.
