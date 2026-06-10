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
 *   <li><b>Source</b>   — what to read and from where</li>
 *   <li><b>Transform</b> — which transforms to apply and their config</li>
 *   <li><b>Sink</b>     — where to write output</li>
 *   <li><b>Retry/DLQ</b> — failure handling behaviour</li>
 *   <li><b>Run date</b> — business date and calendar config for report pipelines</li>
 *   <li><b>Email</b>    — notification and alerting addresses</li>
 * </ul>
 */
public interface FrameworkOptions extends DataflowPipelineOptions {

    // =========================================================================
    // SOURCE CONFIGURATION
    // =========================================================================

    @Description("Input source type. One of: GCS | BQ | PUBSUB")
    @Validation.Required
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
