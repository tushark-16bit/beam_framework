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
 *   <li><b>Parameter BQ store</b> — BQ project/dataset/table names for parameter_store, required_parameters_index, source_config</li>
 *   <li><b>Checkpoint</b> — BigQuery project/dataset/table for run state tracking</li>
 *   <li><b>Source</b>   — what to read and from where (REPORT_PROCESSING only)</li>
 *   <li><b>Transform</b> — which transforms to apply and their config</li>
 *   <li><b>Sink</b>     — where to write output</li>
 *   <li><b>Retry/DLQ</b> — failure handling behaviour</li>
 *   <li><b>Run date</b> — business date and calendar config for report pipelines</li>
 *   <li><b>Email</b>    — notification and alerting addresses</li>
 * </ul>
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

    @Description("Period identifier for this run. Must exist in the MSTR_Per table. "
                 + "Encoding — MONTHLY: YYYYMM (e.g. 202401), "
                 + "DAILY: YYYYMMDD (e.g. 20240115), "
                 + "QUARTERLY: YYYYMMDDQQ (e.g. 2024011501). "
                 + "Required for both DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING.")
    String getPeriodId();
    void setPeriodId(String value);

    @Description("Subprocess identifier for data sources that have multiple distinct sub-feeds "
                 + "within the same datasource and period. "
                 + "Example: intraday, eod, positions, reference")
    @Default.String("default")
    String getSubprocessName();
    void setSubprocessName(String value);

    @Description("When true, re-downloads data even if a COMPLETED DaRefer row exists for "
                 + "this (datasourceName, periodId). Use for forced reprocessing. "
                 + "Default is false: sources with StaCd=COMPLETED for the current period are skipped.")
    @Default.Boolean(false)
    boolean getOverrideDownload();
    void setOverrideDownload(boolean value);

    // =========================================================================
    // REPORT SELECTION (REPORT_PROCESSING — DB-configured reports only)
    // When --reportName is set, ReportPipelineFactory runs instead of the
    // generic PipelineFactory. Leave blank to use the legacy transform-chain mode.
    // =========================================================================

    @Description("Report name as registered in the report_config DB table. "
                 + "When set together with --processType=REPORT_PROCESSING, "
                 + "the DB-configured ReportPipelineFactory is used. "
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

    // =========================================================================
    // PARAMETER BIGQUERY STORE  (config tables — read-only at runtime)
    // All pipeline configuration is stored in BigQuery and fetched at startup.
    //
    // Tables in this dataset:
    //   parameter_store  — all pipeline params keyed by (parameter_group_name, parameter_data_source, parameter_name)
    //                      Used by both DATA_SOURCE_DOWNLOAD (source configs in parameters_val_json)
    //                      and REPORT_PROCESSING (report params). schema_of_json declares required fields.
    //   MSTR_Per         — period master (PerId → PerDt, MoNo, YrNo, PerTypeCd); pre-populated externally
    //   report_config + related — six tables that drive ReportPipelineFactory
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
    // RUNTIME TABLE STORAGE (DaRefer, DaRec, COM_CmnRptDtl)
    // Run lifecycle state is persisted to BigQuery by both process types:
    //   DaRefer  — one row per run; LOADING → COMPLETED / FAILED_BNC / FAILED
    //   DaRec    — all source rows as JSON blobs (DATA_SOURCE_DOWNLOAD writes here)
    //   COM_CmnRptDtl — one row per output file written (REPORT_PROCESSING writes here)
    // All three tables must be created manually — the framework only reads and writes them.
    // =========================================================================

    @Description("GCP project for the checkpoint BigQuery table. "
                 + "Defaults to the --project flag if not set.")
    String getCheckpointBqProject();
    void setCheckpointBqProject(String value);

    @Description("BigQuery dataset for the checkpoint table.")
    @Default.String("pipeline_metadata")
    String getCheckpointBqDataset();
    void setCheckpointBqDataset(String value);

    @Description("BigQuery table name for the DaRefer reference/checkpoint table. "
                 + "Schema: DaId INT64, SrceNm STRING, VsnNo INT64, PerId STRING, "
                 + "FlNm STRING, BalAndCntlSmryTx STRING, StaCd STRING, "
                 + "CreatedTs TIMESTAMP, LstUpdtTs TIMESTAMP. "
                 + "One row per pipeline run. StaCd: LOADING → COMPLETED / FAILED_BNC / FAILED.")
    @Default.String("DaRefer")
    String getDaReferTable();
    void setDaReferTable(String value);

    @Description("BigQuery table name for the DaRec record table. "
                 + "Schema: RecId STRING, DaId INT64, RowDaJsonTx STRING, "
                 + "LoadDt DATE, LstUpdtTs TIMESTAMP. Partitioned by LoadDt. "
                 + "All source rows stored as JSON blobs, keyed by DaId.")
    @Default.String("DaRec")
    String getDaRecTable();
    void setDaRecTable(String value);

    @Description("BigQuery table name for the COM_CmnRptDtl common report detail table. "
                 + "Schema: SrceSysNm STRING, FlNm STRING, SrceFlCreateTs TIMESTAMP, "
                 + "FlDaJsonTx STRING, RecCt INT64, CreatTs TIMESTAMP, CreateUserId STRING, "
                 + "LstUpdtTs TIMESTAMP, LstUpdtUserId STRING. "
                 + "REPORT_PROCESSING writes final output rows here after each transform run.")
    @Default.String("COM_CmnRptDtl")
    String getCmnRptDtlTable();
    void setCmnRptDtlTable(String value);

    // =========================================================================
    // SOURCE CONFIGURATION
    // =========================================================================

    @Description("Input source type. One of: GCS | BQ | PUBSUB. "
                 + "Required for REPORT_PROCESSING. Not used for DATA_SOURCE_DOWNLOAD — "
                 + "source types are fetched per-datasource from the parameter DB.")
    SourceType getSourceType();
    void setSourceType(SourceType value);

    @Description("GCS source glob pattern. Required when sourceType=GCS. "
                 + "Example: gs://my-bucket/input/*.json")
    String getGcsSourcePath();
    void setGcsSourcePath(String value);

    @Description("BigQuery source table reference. Required when sourceType=BQ and "
                 + "no bqSourceQuery is set. Format: project:dataset.table")
    String getBqSourceTable();
    void setBqSourceTable(String value);

    @Description("SQL query to read from BigQuery. Optional override for bqSourceTable. "
                 + "Standard SQL only. Example: SELECT * FROM `project.dataset.table`")
    String getBqSourceQuery();
    void setBqSourceQuery(String value);

    @Description("Pub/Sub subscription path. Required when sourceType=PUBSUB. "
                 + "Format: projects/my-project/subscriptions/my-sub")
    String getPubSubSubscription();
    void setPubSubSubscription(String value);

    // =========================================================================
    // TRANSFORM CHAIN CONFIGURATION
    // =========================================================================

    @Description("Ordered comma-separated list of transform names to apply. "
                 + "Names must exactly match BeamTransform.name() for registered transforms. "
                 + "Example: 'filter-nulls,mask-pii' or 'filter-nulls,enrich-customer,mask-pii'. "
                 + "Leave blank to pass data through unchanged.")
    @Default.String("")
    String getTransformChain();
    void setTransformChain(String value);

    @Description("Comma-separated field names to SHA-256 hash in the mask-pii transform. "
                 + "Fields not present in the row schema are silently skipped. "
                 + "Example: 'email,phone,national_id,date_of_birth'")
    @Default.String("email,phone,name,ssn,dob")
    String getPiiFields();
    void setPiiFields(String value);

    // =========================================================================
    // SINK CONFIGURATION
    // =========================================================================

    @Description("Output sink type. One of: GCS | BQ | PUBSUB")
    @Validation.Required
    SinkType getSinkType();
    void setSinkType(SinkType value);

    @Description("BigQuery destination table. Required when sinkType=BQ. "
                 + "Format: project:dataset.table")
    String getBqSinkTable();
    void setBqSinkTable(String value);

    @Description("BigQuery write disposition. TRUNCATE (default, idempotent, safe to re-run) "
                 + "truncates the table before writing. APPEND adds rows — NOT idempotent, "
                 + "re-running will duplicate data.")
    @Default.Enum("TRUNCATE")
    WriteDispositionType getWriteDisposition();
    void setWriteDisposition(WriteDispositionType value);

    @Description("GCS output path prefix. Required when sinkType=GCS. "
                 + "Example: gs://my-bucket/output/")
    String getGcsSinkPath();
    void setGcsSinkPath(String value);

    @Description("Pub/Sub topic to publish to. Required when sinkType=PUBSUB. "
                 + "Format: projects/my-project/topics/my-topic")
    String getPubSubTopic();
    void setPubSubTopic(String value);

    // =========================================================================
    // RETRY + DEAD-LETTER CONFIGURATION
    // =========================================================================

    @Description("Retry strategy for element-level failures. "
                 + "NONE: no retry, fail immediately to DLQ. "
                 + "FIXED: fixed delay between retries (see retryDelayMs). "
                 + "EXPONENTIAL: exponential back-off with jitter (default).")
    @Default.Enum("EXPONENTIAL")
    RetryPolicyType getRetryPolicy();
    void setRetryPolicy(RetryPolicyType value);

    @Description("Maximum number of per-element retry attempts before routing to the DLQ.")
    @Default.Integer(3)
    int getMaxRetries();
    void setMaxRetries(int value);

    @Description("Base delay in milliseconds between retry attempts. "
                 + "Hard-capped at 200ms inside DoFns to avoid stalling Beam workers. "
                 + "For longer back-off, use a retry-topic pattern.")
    @Default.Long(200L)
    long getRetryDelayMs();
    void setRetryDelayMs(long value);

    @Description("GCS path for dead-letter (failed) records. "
                 + "Each failed record is written as a JSON line with payload, error, and timestamp. "
                 + "Required when retryPolicy != NONE. "
                 + "Example: gs://my-bucket/dlq/my-pipeline/")
    String getDeadLetterSink();
    void setDeadLetterSink(String value);

    // =========================================================================
    // RUN DATE + CALENDAR CONFIGURATION
    // Used by report pipelines to determine which business date to process.
    // Consumed by CalendarUtils and DateUtils in beam-utils.
    // =========================================================================

    @Description("The business date this pipeline run is processing. ISO-8601 format (YYYY-MM-DD). "
                 + "Defaults to today (UTC) if not set. "
                 + "Set explicitly for reprocessing historical dates: --runDate=2024-01-15. "
                 + "Airflow typically passes this as: \"--runDate\": \"{{ ds }}\"")
    String getRunDate();
    void setRunDate(String value);

    @Description("The business calendar to use for working-day calculations. "
                 + "Examples: DEFAULT (Mon–Fri, no holidays), NYSE, LSE, UK_BANKING, IN_NSE. "
                 + "Must match a calendar name supported by CalendarUtils. "
                 + "Used to determine if runDate is a valid business day and to compute "
                 + "previous/next business day offsets.")
    @Default.String("DEFAULT")
    String getCalendarName();
    void setCalendarName(String value);

    @Description("Number of business days to look back from runDate when computing the "
                 + "reporting window. Example: 1 = yesterday's business day. "
                 + "0 = use runDate itself.")
    @Default.Integer(0)
    int getBusinessDayOffset();
    void setBusinessDayOffset(int value);

    // =========================================================================
    // EMAIL / NOTIFICATION CONFIGURATION
    // Addresses are fetched from outside the pipeline (Secret Manager or env vars)
    // and passed via these options so no secrets are embedded in code or DAG files.
    // =========================================================================

    @Description("Business/stakeholder email address for report delivery and success notifications. "
                 + "Fetch from GCP Secret Manager and pass via Airflow: "
                 + "\"--businessEmail\": \"{{ var.value.pipeline_business_email }}\". "
                 + "Do NOT hardcode email addresses in DAG files.")
    String getBusinessEmail();
    void setBusinessEmail(String value);

    @Description("Developer/on-call email address for pipeline failure and error alerts. "
                 + "Fetch from GCP Secret Manager and pass via Airflow: "
                 + "\"--devErrorEmail\": \"{{ var.value.pipeline_dev_email }}\". "
                 + "Do NOT hardcode email addresses in DAG files.")
    String getDevErrorEmail();
    void setDevErrorEmail(String value);

    @Description("SMTP host for sending email notifications. "
                 + "Example: smtp.gmail.com or your internal relay. "
                 + "Credentials should be fetched from Secret Manager via SecretManagerUtils, "
                 + "not passed as a pipeline option.")
    @Default.String("smtp.gmail.com")
    String getEmailSmtpHost();
    void setEmailSmtpHost(String value);

    @Description("SMTP port for email notifications.")
    @Default.Integer(587)
    int getEmailSmtpPort();
    void setEmailSmtpPort(int value);

    @Description("GCP Secret Manager secret ID for the SMTP password. "
                 + "The actual password is fetched at runtime by SecretManagerUtils — "
                 + "it is never stored as a pipeline option or in source code. "
                 + "Example: projects/my-project/secrets/smtp-password/versions/latest")
    String getSmtpPasswordSecretId();
    void setSmtpPasswordSecretId(String value);
}
