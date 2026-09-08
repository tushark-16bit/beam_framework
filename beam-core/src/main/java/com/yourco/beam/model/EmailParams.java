package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Immutable envelope of sender/recipient settings for one email, built by
 * {@code EmailSendUtility.SetEmailParams()} and passed into
 * {@code EmailSendUtility.CreateEmailRequest()}.
 *
 * <p>Deliberately separate from the body/attachments (which vary per call) — this holds only
 * what identifies who the email is from, who it's to, and whether it must go out encrypted.
 */
public final class EmailParams implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String       fromEmailAddress;
    public final String       subject;
    public final List<String> toList;
    public final List<String> ccList;
    public final boolean      encryptedOrNot;

    public EmailParams(String fromEmailAddress, String subject,
                       List<String> toList, List<String> ccList, boolean encryptedOrNot) {
        this.fromEmailAddress = fromEmailAddress;
        this.subject          = subject;
        this.toList           = toList != null ? Collections.unmodifiableList(toList) : Collections.emptyList();
        this.ccList           = ccList != null ? Collections.unmodifiableList(ccList) : Collections.emptyList();
        this.encryptedOrNot   = encryptedOrNot;
    }
}
