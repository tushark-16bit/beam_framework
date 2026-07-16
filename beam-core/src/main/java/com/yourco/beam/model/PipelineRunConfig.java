package com.yourco.beam.model;

import com.yourco.beam.options.RetryPolicyType;
import com.yourco.beam.options.SinkType;
import com.yourco.beam.options.SourceType;
import com.yourco.beam.options.WriteDispositionType;

import java.util.Collections;
import java.util.Map;

/**
 * Per-datasource runtime configuration loaded from the {@code parameter_store} BigQuery table.
 *
 * <p>Replaces the 21 CLI flags that were previously declared in {@link com.yourco.beam.options.FrameworkOptions}.
 * Values are fetched once at pipeline-assembly time by {@code BigQueryParameterAdapterImpl} and
 * stored in this immutable wrapper.
 *
 * <h2>Extensibility</h2>
 * New parameter_store keys require zero Java code changes. Add the row to BQ and read the value
 * via the generic {@link #get(String)} or {@link #get(String, String)} escape hatch:
 * <pre>{@code
 * String myValue = runConfig.get("my_custom_key", "default");
 * }</pre>
 *
 * <h2>Parameter store key names</h2>
 * Keys match the column names in {@code parameter_store.parameters_val_json} (snake_case).
 */
public final class PipelineRunConfig {

    private final Map<String, String> params;

    public PipelineRunConfig(Map<String, String> params) {
        this.params = params != null ? Map.copyOf(params) : Map.of();
    }

    // ── Source ────────────────────────────────────────────────────────────────

    public SourceType getSourceType() {
        return SourceType.valueOf(require("source_type"));
    }

    public String getGcsSourcePath() {
        return get("gcs_source_path");
    }

    public String getBqSourceTable() {
        return get("bq_source_table");
    }

    public String getBqSourceQuery() {
        return get("bq_source_query");
    }

    public String getPubSubSubscription() {
        return get("pubsub_subscription");
    }

    // ── Transform chain ───────────────────────────────────────────────────────

    public String getTransformChain() {
        return get("transform_chain", "");
    }

    public String getPiiFields() {
        return get("pii_fields", "email,phone,name,ssn,dob");
    }

    // ── Sink ──────────────────────────────────────────────────────────────────

    public SinkType getSinkType() {
        return SinkType.valueOf(require("sink_type"));
    }

    public String getBqSinkTable() {
        return get("bq_sink_table");
    }

    public WriteDispositionType getWriteDisposition() {
        return WriteDispositionType.valueOf(get("write_disposition", "TRUNCATE"));
    }

    public String getGcsSinkPath() {
        return get("gcs_sink_path");
    }

    public String getPubSubTopic() {
        return get("pubsub_topic");
    }

    // ── Retry / DLQ ───────────────────────────────────────────────────────────

    public RetryPolicyType getRetryPolicy() {
        return RetryPolicyType.valueOf(get("retry_policy", "EXPONENTIAL"));
    }

    public int getMaxRetries() {
        return Integer.parseInt(get("max_retries", "3"));
    }

    public long getRetryDelayMs() {
        return Long.parseLong(get("retry_delay_ms", "200"));
    }

    public String getDeadLetterSink() {
        return get("dead_letter_sink");
    }

    // ── Calendar ──────────────────────────────────────────────────────────────

    public String getCalendarName() {
        return get("calendar_name", "DEFAULT");
    }

    // ── Email / SMTP ──────────────────────────────────────────────────────────

    public String getBusinessEmail() {
        return get("business_email");
    }

    public String getDevErrorEmail() {
        return get("dev_error_email");
    }

    public String getEmailSmtpHost() {
        return get("email_smtp_host", "smtp.gmail.com");
    }

    public int getEmailSmtpPort() {
        return Integer.parseInt(get("email_smtp_port", "587"));
    }

    public String getSmtpPasswordSecretId() {
        return get("smtp_password_secret_id");
    }

    // ── Generic extensibility ─────────────────────────────────────────────────

    /** Returns the raw value for any parameter_store key, or {@code null} if absent. */
    public String get(String key) {
        return params.get(key);
    }

    /** Returns the raw value for any key, or {@code defaultValue} if absent or blank. */
    public String get(String key, String defaultValue) {
        String v = params.get(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    /** Returns an unmodifiable view of all loaded parameters. */
    public Map<String, String> all() {
        return Collections.unmodifiableMap(params);
    }

    private String require(String key) {
        String v = params.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(
                "Required parameter '" + key + "' is missing from PipelineRunConfig. "
                + "Add a row with this key to the parameter_store table.");
        }
        return v;
    }
}
