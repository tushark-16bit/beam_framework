package com.yourco.beam.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Root options interface for every pipeline in this framework.
 *
 * <p>All CLI flags for every pipeline are declared here. Beam auto-generates
 * the implementation at runtime — you never instantiate this interface directly.
 *
 * <h2>How options work</h2>
 * Each getter maps to a {@code --flagName=value} CLI argument. For example,
 * {@code getSourceType()} maps to {@code --sourceType=BQ}.
 * Airflow passes these as the {@code options} dict in {@code DataflowStartJobOperator}.
 *
 * <h2>How to add a new option</h2>
 * <ol>
 *   <li>Add a getter + setter pair here with {@code @Description} and optional
 *       {@code @Default.*} or {@code @Validation.Required}.</li>
 *   <li>Read it in your transform via the {@code options} argument passed to
 *       {@link com.yourco.beam.transform.BeamTransform#toComposite}.</li>
 *   <li>Pass it from Airflow: {@code "--myNewFlag": "{{ dag_run.conf['myNewFlag'] }}"}</li>
 * </ol>
 *
 * <h2>Option groups</h2>
 * <ul>
 *   <li><b>Process control</b> — processType, jobRunId</li>
 *   <li><b>Data source selection</b> — datasourceName, periodId, subprocessName, overrideDownload</li>
 *   <li><b>Parameter BQ store</b> — BQ project/dataset/table names for parameter_store</li>
 *   <li><b>Checkpoint</b> — BigQuery project/dataset/table for run state tracking</li>
 *   <li><b>Run date</b> — business date for report pipelines (calendarName is in parameter_store)</li>
 * </ul>
 *
 * <p>Source, transform chain, sink, retry/DLQ, calendar, and email configuration are loaded
 * per-datasource from {@code parameter_store} via {@link com.yourco.beam.model.PipelineRunConfig}.
 */
public interface FrameworkOptions extends DataflowPipelineOptions {

    // =========================================================================
    // PROCESS CONTROL
    // =========================================================================

    @Description("Which pipeline process to run. "
                 + "DATA_SOURCE_DOWNLOAD: fetch raw data from external sources (API, file, BQ) "
                 + "using configuration from the parameter DB. --sourceType is not used. "
                 + "REPORT_PROCESSING: read downloaded data, apply the transform chain, and write reports. "
                 + "--sourceType is required.")
    @Validation.Required
    ProcessType getProcessType();
    void setProcessType(ProcessType value);

    @Description("Unique identifier for this job run. Used for checkpoint correlation and log tracing. "
                 + "Auto-generated UUID if not provided. "
                 + "Example: etl-trades-2024-01-15-run1")
    @Default.String("")
    String getJobRunId();
    void setJobRunId(String value);

    // =========================================================================
    // DATA SOURCE SELECTION (DATA_SOURCE_DOWNLOAD only)
    // =========================================================================

    @Description("Top-level business group identifier. "
                 + "Maps to parameter_group_name in parameter_store for both "
                 + "DATA_SOURCE_DOWNLOAD (source config) and REPORT_PROCESSING (report params). "
                 + "Example: TRADING, RISK, MARKET_DATA")
    String getParentId();
    void setParentId(String value);

    @Description("Name of the data source as registered in parameter_store (parameter_name column). "
                 + "Required for DATA_SOURCE_DOWNLOAD. Used as the lookup key alongside "
                 + "--parentId and --subprocessName to fetch source configuration. "
                 + "Example: trades, market-data, fx-rates, customer-positions")
    String getDatasourceName();
    void setDatasourceName(String value);

    @Description("Period identifier for this run as an integer. Must exist in the MSTR_Per table. "
                 + "Encoding — MONTHLY: YYYYMM (e.g. 202401), "
                 + "DAILY: YYYYMMDD (e.g. 20240115), "
                 + "QUARTERLY: YYYYMMDDQQ (e.g. 2024011501). "
                 + "Required for both DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING. "
                 + "Stored as INT64 in DaRefer.per_id and RptRefer.per_id.")
    @Default.Integer(0)
    int getPeriodId();
    void setPeriodId(int value);

    @Description("Subprocess identifier for data sources that have multiple distinct sub-feeds "
                 + "within the same datasource and period. "
                 + "Example: intraday, eod, positions, reference")
    @Default.String("default")
    String getSubprocessName();
    void setSubprocessName(String value);

    @Description("When true, re-downloads data even if a COMPLETED DaRefer row exists for "
                 + "this (datasourceName, periodId). Use for forced reprocessing. "
                 + "Default is false: sources with sta_cd=COMPLETED for the current period are skipped. "
                 + "Prefer --manualOverrun for explicit operator-initiated re-runs.")
    @Default.Boolean(false)
    boolean getOverrideDownload();
    void setOverrideDownload(boolean value);

    @Description("Explicit operator override key. When true, re-runs a DATA_SOURCE_DOWNLOAD even if "
                 + "DaRefer already has a COMPLETED row for this (datasourceName, parentId, periodId) combo. "
                 + "Guards against accidental re-runs: the default (false) hard-blocks execution when "
                 + "a completed run is found. Must be set deliberately in the Airflow DAG or CLI invocation. "
                 + "Takes effect alongside --overrideDownload (either flag enables the re-run). "
                 + "The superseded run always gets a fresh DaRefer row (never overwritten) — once the new "
                 + "run reaches COMPLETED, only the previous run's DaRec rows are deleted, reclaiming the "
                 + "superseded bulk data while the full DaRefer run history is preserved. "
                 + "Also applies under --processType=PIPELINE: every DATA_SOURCE step in the sequence gets "
                 + "this same bypass-and-supersede treatment (PipelineSequenceFactory passes this same "
                 + "options object straight into DataSourcePipelineFactory, unchanged). The terminal REPORT "
                 + "step needs no equivalent flag — it has no COMPLETED guard of its own and always re-runs.")
    @Default.Boolean(false)
    boolean getManualOverrun();
    void setManualOverrun(boolean value);

    @Description("DaRefer da_id for the current run. "
                 + "Set automatically by DataSourcePipelineFactory for Flex Templates and DirectRunner.")
    org.apache.beam.sdk.options.ValueProvider<Long> getDaId();
    void setDaId(org.apache.beam.sdk.options.ValueProvider<Long> value);

    // =========================================================================
    // REPORT SELECTION (REPORT_PROCESSING — DB-configured reports only)
    // When --reportName is set, ReportPipelineFactory runs instead of the
    // generic PipelineFactory. Leave blank to use the legacy transform-chain mode.
    // =========================================================================

    @Description("Report name as registered in parameter_store (parameter_name column). "
                 + "When set together with --processType=REPORT_PROCESSING, "
                 + "ReportPipelineFactory fetches the full report config from parameter_store. "
                 + "Example: daily_trades_report, monthly_pnl_summary")
    @Default.String("")
    String getReportName();
    void setReportName(String value);

    @Description("Report subprocess name. Allows the same report_name to have "
                 + "multiple variants (e.g. intraday vs eod). Matched against "
                 + "report_config.report_subprocess in the parameter DB.")
    @Default.String("default")
    String getReportSubprocess();
    void setReportSubprocess(String value);

    @Description("Period start date in ISO-8601 format (YYYY-MM-DD). "
                 + "Injected into query templates as the {periodStart} token. "
                 + "Example: 2024-01-01")
    String getPeriodStart();
    void setPeriodStart(String value);

    @Description("Period end date in ISO-8601 format (YYYY-MM-DD). "
                 + "Injected into query templates as the {periodEnd} token. "
                 + "Example: 2024-01-31")
    String getPeriodEnd();
    void setPeriodEnd(String value);

    @Description("JSON object of ad-hoc custom query-template tokens, resolved by "
                 + "QueryParameterResolver for both DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING "
                 + "query templates — the CLI-supplied equivalent of a step's own query_params_json "
                 + "in parameter_store, for a value that should come from the invocation itself "
                 + "(Airflow DAG conf, CLI) rather than be hard-coded into the stored config. "
                 + "On a key collision with a step's own query_params_json, this value wins. "
                 + "Values may reference {periodStart}/{periodEnd}/{periodId}/{runDate} — those "
                 + "are resolved first. "
                 + "Example: {\"exchange\":\"NYSE\",\"threshold\":\"10000\"} makes {exchange} and "
                 + "{threshold} available in any query_template/bq_query this run resolves.")
    @Default.String("")
    String getCustomParamsJson();
    void setCustomParamsJson(String value);

    // =========================================================================
    // PARAMETER BIGQUERY STORE  (config tables — read-only at runtime)
    // All pipeline configuration is stored in BigQuery and fetched at startup.
    //
    // Tables in this dataset:
    //   parameter_store  — ALL pipeline params keyed by (parameter_group_name, parameter_data_source, parameter_name)
    //                      Both DATA_SOURCE_DOWNLOAD (source configs) and REPORT_PROCESSING (report configs)
    //                      store their configurations here as JSON blobs in parameters_val_json.
    //                      Source configs use a flat JSON map; report configs use a nested JSON structure
    //                      with keys: override_key, datasources, preprocessing, transforms, outputs, email.
    //   MSTR_Per         — period master (per_id → per_dt, mo_no, yr_no, per_typ_cd); pre-populated externally
    //
    // Table names are configurable so the same binary works in dev/staging/prod.
    // =========================================================================

    @Description("GCP project that contains the parameter BigQuery dataset. "
                 + "Defaults to the pipeline's --project if not set. "
                 + "Example: my-gcp-project")
    String getParamBqProject();
    void setParamBqProject(String value);

    @Description("BigQuery dataset that contains all parameter and config tables. "
                 + "Example: dw")
    @Default.String("dw")
    String getParamBqDataset();
    void setParamBqDataset(String value);

    @Description("BQ table name for the parameter store. "
                 + "Schema: parameter_name STRING, parameter_group_name STRING, "
                 + "parameter_data_source STRING, schema_of_json STRING, parameters_val_json STRING, "
                 + "edit_grp_nm STRING, last_updt_ts TIMESTAMP, lst_update_user_id STRING. "
                 + "Each row holds all parameters for a (parameter_group_name, parameter_data_source, parameter_name) group "
                 + "as a JSON blob in parameters_val_json. Required fields are declared in schema_of_json.")
    @Default.String("parameter_store")
    String getParamStoreTable();
    void setParamStoreTable(String value);


    // =========================================================================
    // RUNTIME TABLE STORAGE
    // Run lifecycle state is persisted to BigQuery. All DATETIME columns (no timezone).
    //
    // DATA_SOURCE_DOWNLOAD tables:
    //   DaRefer  — one row per source run; LOADING → COMPLETED / FAILED_BNC / FAILED
    //   DaRec    — all source rows as JSON blobs (Beam workers write here)
    //
    // REPORT_PROCESSING tables:
    //   RptRefer   — one row per report run; LOADING → COMPLETED / FAILED
    //   RptDaMap   — maps each report run to the DaRefer.da_id(s) it consumed
    //   RptStageDa — transient staging; DaRec rows copied here before transforms, deleted after
    //   RptOutput  — one row per output step produced by the report
    //
    // All tables must be created manually — the framework only reads and writes them.
    // =========================================================================

    @Description("GCP project for the checkpoint BigQuery tables. "
                 + "Defaults to the --project flag if not set.")
    String getCheckpointBqProject();
    void setCheckpointBqProject(String value);

    @Description("BigQuery dataset for all runtime tracking tables.")
    @Default.String("pipeline_metadata")
    String getCheckpointBqDataset();
    void setCheckpointBqDataset(String value);

    @Description("BigQuery table name for the DaRefer reference/checkpoint table. "
                 + "Schema: da_id INT64, srce_nm STRING, vsn_no INT64, per_id INT64, "
                 + "fl_nm STRING, bal_and_cntl_smry_tx STRING, sta_cd STRING, "
                 + "created_ts DATETIME, lst_updt_ts DATETIME. "
                 + "One row per DATA_SOURCE_DOWNLOAD run. sta_cd: LOADING → COMPLETED / FAILED_BNC / FAILED.")
    @Default.String("DaRefer")
    String getDaReferTable();
    void setDaReferTable(String value);

    @Description("BigQuery table name for the DaRec record table. "
                 + "Schema: rec_id STRING, da_id INT64, row_da_json_tx STRING, "
                 + "load_dt DATE, lst_updt_ts DATETIME. Partitioned by load_dt. "
                 + "All source rows stored as JSON blobs, keyed by da_id.")
    @Default.String("DaRec")
    String getDaRecTable();
    void setDaRecTable(String value);

    @Description("BigQuery table name for the RptRefer report checkpoint table. "
                 + "Schema: rpt_id INT64, rpt_nm STRING, per_id INT64, rpt_ds STRING, "
                 + "sta_cd STRING, creat_ts DATETIME, lst_updt_ts DATETIME. "
                 + "One row per REPORT_PROCESSING run. sta_cd: LOADING → COMPLETED / FAILED.")
    @Default.String("RptRefer")
    String getRptReferTable();
    void setRptReferTable(String value);

    @Description("BigQuery table name for the RptDaMap datasource-mapping table. "
                 + "Schema: map_id INT64, rpt_id INT64, da_id INT64, lst_updt_ts DATETIME. "
                 + "Links each REPORT_PROCESSING run to the DaRefer.da_id(s) it consumed.")
    @Default.String("RptDaMap")
    String getRptDaMapTable();
    void setRptDaMapTable(String value);

    @Description("BigQuery table name for the RptStageDa staging table. "
                 + "Schema: stage_id INT64, map_id INT64, stage_ds_json_tx STRING, "
                 + "query_config_tx STRING, load_dt DATE, lst_updt_ts DATETIME. "
                 + "Transient: rows are inserted before the transform chain and deleted after.")
    @Default.String("RptStageDa")
    String getRptStageDaTable();
    void setRptStageDaTable(String value);

    @Description("BigQuery table name for the RptOutput output-tracking table. "
                 + "Schema: outpt_cd STRING, rpt_dt DATETIME, vsn_no INT64, output_ds STRING, "
                 + "line_refer_cd STRING, sched_tx STRING, bal_am FLOAT64, "
                 + "rpt_type_cd STRING, rpt_id INT64, lst_updt_ts DATETIME. "
                 + "One row per output step per REPORT_PROCESSING run.")
    @Default.String("RptOutput")
    String getRptOutputTable();
    void setRptOutputTable(String value);

    // =========================================================================
    // RUN DATE + CALENDAR CONFIGURATION
    // Used by report pipelines to determine which business date to process.
    // Consumed by CalendarUtils and DateUtils in beam-utils.
    // calendarName is fetched from parameter_store via PipelineRunConfig.
    // =========================================================================

    @Description("The business date this pipeline run is processing. ISO-8601 format (YYYY-MM-DD). "
                 + "Defaults to today (UTC) if not set. "
                 + "Set explicitly for reprocessing historical dates: --runDate=2024-01-15. "
                 + "Airflow typically passes this as: \"--runDate\": \"{{ ds }}\"")
    String getRunDate();
    void setRunDate(String value);

    @Description("Number of business days to look back from runDate when computing the "
                 + "reporting window. Example: 1 = yesterday's business day. "
                 + "0 = use runDate itself.")
    @Default.Integer(0)
    int getBusinessDayOffset();
    void setBusinessDayOffset(int value);

    @Description("Business calendar name used by CalendarUtils to resolve business days and "
                 + "apply --businessDayOffset. Supported names are defined per CalendarUtils "
                 + "implementation, e.g. DEFAULT (Mon-Fri, no holidays), NYSE, LSE, UK_BANKING, "
                 + "IN_NSE. See CalendarUtils class Javadoc.")
    @Default.String("DEFAULT")
    String getCalendarName();
    void setCalendarName(String value);
}
