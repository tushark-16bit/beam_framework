package com.yourco.beam.transforms.side;

import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.SecretManagerUtils;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Side-effect transform that sends an email for each incoming {@link Row}.
 *
 * <h2>What "side effect" means in Beam</h2>
 * This transform produces {@link PDone} — it has no data output. It branches off
 * the main pipeline and runs concurrently with the sink write. Dataflow schedules it
 * on workers just like any other transform. The email is sent by the worker, not by
 * the driver JVM, which means it scales with parallelism.
 *
 * <h2>Input Row schema</h2>
 * Each input Row must contain these STRING fields:
 * <pre>
 *   to       — recipient email address
 *   subject  — email subject line
 *   body     — email body (plain text or HTML)
 *   cc       — (optional) comma-separated CC addresses; null or blank to skip
 * </pre>
 *
 * <h2>Usage in a pipeline factory</h2>
 * <pre>{@code
 * // Send notification when pipeline completes (branch off success path)
 * PCollection<Row> notification = successRows.apply("BuildNotification",
 *     new BuildEmailRowTransform(options));
 * notification.apply("SendEmail", new SideEffectEmailTransform(options));
 * }</pre>
 *
 * <h2>Serialization</h2>
 * SMTP credentials ({@link Session}) are {@code transient} and fetched in {@code @Setup}.
 * SMTP host, port, and Secret Manager path are primitive/String fields — safe to serialize.
 */
public final class SideEffectEmailTransform extends PTransform<PCollection<Row>, PDone> {

    private final String smtpHost;
    private final int smtpPort;
    private final String smtpPasswordSecretId;
    private final String fromAddress;

    public SideEffectEmailTransform(FrameworkOptions options) {
        this.smtpHost             = options.getEmailSmtpHost();
        this.smtpPort             = options.getEmailSmtpPort();
        this.smtpPasswordSecretId = options.getSmtpPasswordSecretId();
        // Use the dev email as the "from" address; falls back to businessEmail
        String from = options.getDevErrorEmail();
        this.fromAddress = (from != null && !from.isBlank()) ? from : options.getBusinessEmail();
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        input.apply("SendEmails", ParDo.of(new SendEmailFn(smtpHost, smtpPort, smtpPasswordSecretId, fromAddress)));
        return PDone.in(input.getPipeline());
    }

    // ── DoFn ────────────────────────────────────────────────────────────────

    private static final class SendEmailFn extends DoFn<Row, Void> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(SendEmailFn.class);

        // Serialized — all primitives or Strings
        private final String smtpHost;
        private final int smtpPort;
        private final String smtpPasswordSecretId;
        private final String fromAddress;

        // Transient — created per worker in @Setup
        private transient Session mailSession;

        SendEmailFn(String smtpHost, int smtpPort, String smtpPasswordSecretId, String fromAddress) {
            this.smtpHost             = smtpHost;
            this.smtpPort             = smtpPort;
            this.smtpPasswordSecretId = smtpPasswordSecretId;
            this.fromAddress          = fromAddress;
        }

        @Setup
        public void setup() {
            String password = (smtpPasswordSecretId != null && !smtpPasswordSecretId.isBlank())
                ? SecretManagerUtils.fetchSecret(smtpPasswordSecretId)
                : "";

            Properties props = new Properties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.host", smtpHost);
            props.put("mail.smtp.port", String.valueOf(smtpPort));

            String finalPassword = password;
            mailSession = Session.getInstance(props, new jakarta.mail.Authenticator() {
                @Override
                protected jakarta.mail.PasswordAuthentication getPasswordAuthentication() {
                    return new jakarta.mail.PasswordAuthentication(fromAddress, finalPassword);
                }
            });
        }

        @ProcessElement
        public void processElement(@Element Row row) {
            String to      = row.getString("to");
            String subject = row.getString("subject");
            String body    = row.getString("body");
            String cc      = row.getSchema().hasField("cc") ? row.getString("cc") : null;

            if (to == null || to.isBlank()) {
                LOG.warn("Skipping email — 'to' field is blank");
                return;
            }

            try {
                MimeMessage message = new MimeMessage(mailSession);
                message.setFrom(new InternetAddress(fromAddress));
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
                if (cc != null && !cc.isBlank()) {
                    for (String ccAddr : cc.split(",")) {
                        message.addRecipient(Message.RecipientType.CC,
                            new InternetAddress(ccAddr.trim()));
                    }
                }
                message.setSubject(subject);
                message.setText(body);
                Transport.send(message);
                LOG.info("Email sent to '{}' with subject '{}'", to, subject);
            } catch (MessagingException e) {
                // Log but don't fail the pipeline — email is best-effort
                LOG.error("Failed to send email to '{}': {}", to, e.getMessage(), e);
            }
        }

        @Teardown
        public void teardown() {
            mailSession = null;
        }
    }
}
