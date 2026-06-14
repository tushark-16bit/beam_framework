package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Email delivery settings for a report.
 *
 * <p>Stored in {@code report_email_config}. After all outputs are exported to GCS,
 * the email is sent with each output file as an attachment.
 *
 * <h2>Template tokens</h2>
 * {@code subjectTemplate} and {@code bodyTemplate} support:
 * <ul>
 *   <li>{@code {reportName}} — the report name</li>
 *   <li>{@code {reportSubprocess}} — the subprocess name</li>
 *   <li>{@code {periodId}} — the period identifier</li>
 *   <li>{@code {periodStart}} — period start date</li>
 *   <li>{@code {periodEnd}} — period end date</li>
 *   <li>{@code {runDate}} — the pipeline run date</li>
 * </ul>
 */
public final class ReportEmailConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final List<String> toList;
    public final List<String> ccList;
    /** Subject line with optional template tokens. */
    public final String subjectTemplate;
    /** Email body with optional template tokens. */
    public final String bodyTemplate;

    public ReportEmailConfig(List<String> toList, List<String> ccList,
                             String subjectTemplate, String bodyTemplate) {
        this.toList          = Collections.unmodifiableList(toList);
        this.ccList          = ccList != null ? Collections.unmodifiableList(ccList)
                                              : Collections.emptyList();
        this.subjectTemplate = subjectTemplate != null ? subjectTemplate : "";
        this.bodyTemplate    = bodyTemplate    != null ? bodyTemplate    : "";
    }
}
