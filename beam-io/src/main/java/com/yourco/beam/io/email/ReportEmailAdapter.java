package com.yourco.beam.io.email;

import java.util.List;

/**
 * Port for sending report emails with file attachments.
 *
 * <p>This is a driver-JVM interface — it is called from {@code ReportPipelineFactory}
 * after all outputs have been exported to GCS and downloaded as byte streams.
 * It is NOT used inside Beam workers. For in-pipeline email notifications (no
 * attachments), use {@code SideEffectEmailTransform} instead.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>All {@link EmailAttachment} streams are consumed exactly once and closed
 *       by the implementation after sending.</li>
 *   <li>Failures throw {@link RuntimeException} — the caller (report factory)
 *       decides whether to fail the report or log and continue.</li>
 * </ul>
 *
 * <h2>Implementing a custom adapter</h2>
 * Create a class that implements this interface. The {@code SmtpReportEmailAdapter}
 * in {@code beam-runner} provides an SMTP implementation using Jakarta Mail.
 * For SendGrid, AWS SES, or other providers, implement this interface and inject
 * via {@code ReportPipelineFactory}.
 */
public interface ReportEmailAdapter {

    /**
     * Sends an email with one or more file attachments.
     *
     * @param subject     email subject line (already resolved, no template tokens)
     * @param body        email body text (already resolved, no template tokens)
     * @param to          list of recipient addresses (at least one required)
     * @param cc          CC addresses (may be empty)
     * @param attachments list of file attachments; streams are closed after send
     */
    void send(String subject, String body,
              List<String> to, List<String> cc,
              List<EmailAttachment> attachments);
}
