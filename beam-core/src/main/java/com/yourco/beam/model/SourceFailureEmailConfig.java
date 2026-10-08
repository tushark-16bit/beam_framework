package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Failure-notification email config carried on a {@link SourceConfig}.
 *
 * <p>Populated from the following flat keys in {@code parameters_val_json}:
 * <pre>
 *   failure_email_to        — comma-separated recipients (required; absence disables email)
 *   failure_email_cc        — comma-separated CC (optional)
 *   failure_email_subject   — subject template; tokens: {datasourceName} {periodId} {staCd}
 *   failure_email_body      — body template; tokens: above + {errorMessage} {bncSummary}
 *   email_smtp_host         — SMTP relay (e.g. smtp.gmail.com)
 *   email_smtp_port         — SMTP port (default 587)
 *   smtp_password_secret_id — Secret Manager resource name for the SMTP password
 *   from_address            — sender address used for From: and SMTP auth
 * </pre>
 *
 * <p>{@link #isPresent()} returns {@code false} when email is not configured — callers
 * must check before attempting to send.
 */
public final class SourceFailureEmailConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final List<String> to;
    public final List<String> cc;
    public final String subjectTemplate;
    public final String bodyTemplate;
    public final String smtpHost;
    public final int    smtpPort;
    public final String smtpPasswordSecretId;
    public final String fromAddress;

    public SourceFailureEmailConfig(List<String> to, List<String> cc,
                                    String subjectTemplate, String bodyTemplate,
                                    String smtpHost, int smtpPort,
                                    String smtpPasswordSecretId, String fromAddress) {
        this.to                   = to != null ? Collections.unmodifiableList(to) : Collections.emptyList();
        this.cc                   = cc != null ? Collections.unmodifiableList(cc) : Collections.emptyList();
        this.subjectTemplate      = subjectTemplate;
        this.bodyTemplate         = bodyTemplate;
        this.smtpHost             = smtpHost;
        this.smtpPort             = smtpPort;
        this.smtpPasswordSecretId = smtpPasswordSecretId;
        this.fromAddress          = fromAddress;
    }

    /** Returns true only when enough config is present to actually send an email. */
    public boolean isPresent() {
        return !to.isEmpty()
            && smtpHost    != null && !smtpHost.isBlank()
            && fromAddress != null && !fromAddress.isBlank();
    }
}
