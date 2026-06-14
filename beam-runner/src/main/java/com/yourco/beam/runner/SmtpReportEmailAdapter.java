package com.yourco.beam.runner;

import com.yourco.beam.io.email.EmailAttachment;
import com.yourco.beam.io.email.ReportEmailAdapter;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.SecretManagerUtils;
import jakarta.activation.DataHandler;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.util.ByteArrayDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * SMTP implementation of {@link ReportEmailAdapter} using Jakarta Mail (angus-mail).
 *
 * <p>Credentials are read from Secret Manager at construction time (not per send call).
 * The same SMTP session is reused for all attachments in one report run.
 *
 * <p>This class mirrors the SMTP setup of {@code SideEffectEmailTransform} but runs
 * in the driver JVM (not a Beam worker) and supports multipart attachments.
 */
final class SmtpReportEmailAdapter implements ReportEmailAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(SmtpReportEmailAdapter.class);

    private final Session  session;
    private final String   fromAddress;

    SmtpReportEmailAdapter(FrameworkOptions options) {
        String smtpHost     = options.getEmailSmtpHost();
        int    smtpPort     = options.getEmailSmtpPort();
        String secretId     = options.getSmtpPasswordSecretId();
        String from         = options.getDevErrorEmail();
        this.fromAddress    = (from != null && !from.isBlank()) ? from : options.getBusinessEmail();

        String password = (secretId != null && !secretId.isBlank())
                ? SecretManagerUtils.fetchSecret(secretId)
                : "";

        Properties props = new Properties();
        props.put("mail.smtp.auth",             "true");
        props.put("mail.smtp.starttls.enable",  "true");
        props.put("mail.smtp.host",             smtpHost);
        props.put("mail.smtp.port",             String.valueOf(smtpPort));

        String finalPassword = password;
        this.session = Session.getInstance(props, new jakarta.mail.Authenticator() {
            @Override
            protected jakarta.mail.PasswordAuthentication getPasswordAuthentication() {
                return new jakarta.mail.PasswordAuthentication(fromAddress, finalPassword);
            }
        });
    }

    @Override
    public void send(String subject, String body,
                     List<String> to, List<String> cc,
                     List<EmailAttachment> attachments) {
        if (to == null || to.isEmpty()) {
            LOG.warn("Email skipped — no recipients specified");
            return;
        }
        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(fromAddress));
            for (String addr : to) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(addr.trim()));
            }
            if (cc != null) {
                for (String addr : cc) {
                    if (addr != null && !addr.isBlank()) {
                        message.addRecipient(Message.RecipientType.CC,
                                             new InternetAddress(addr.trim()));
                    }
                }
            }
            message.setSubject(subject);

            MimeMultipart multipart = new MimeMultipart();

            MimeBodyPart bodyPart = new MimeBodyPart();
            bodyPart.setText(body, "utf-8");
            multipart.addBodyPart(bodyPart);

            for (EmailAttachment att : attachments) {
                MimeBodyPart filePart = new MimeBodyPart();
                byte[] bytes = att.content.readAllBytes();
                ByteArrayDataSource ds = new ByteArrayDataSource(bytes, att.contentType);
                filePart.setDataHandler(new DataHandler(ds));
                filePart.setFileName(att.fileName);
                multipart.addBodyPart(filePart);
            }

            message.setContent(multipart);
            Transport.send(message);
            LOG.info("Report email sent to {} recipient(s) with {} attachment(s)",
                     to.size(), attachments.size());

        } catch (MessagingException | IOException e) {
            throw new RuntimeException("Failed to send report email: " + e.getMessage(), e);
        }
    }
}
