package com.yourco.beam.model;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.time.Instant;

/**
 * Envelope for a record that could not be processed after all retries.
 * Written to the dead-letter sink for offline inspection and replay.
 *
 * <p>{@code @DefaultCoder(SerializableCoder.class)} tells Beam which coder to
 * use when this type appears in a {@link org.apache.beam.sdk.values.PCollection}.
 * Without this, Beam falls back to slow reflection-based coder inference that
 * may fail at runtime when the DLQ PCollection is written to a sink.
 *
 * <p>All fields are serializable primitives or {@link String} so this class
 * can be safely held in Beam state and written via standard IOs.
 */
@DefaultCoder(SerializableCoder.class)
public final class FailedRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String payload;        // Row serialized as a human-readable string
    private final String errorMessage;
    private final String errorClass;
    private final int    attemptCount;
    private final String failedAtUtc;    // ISO-8601

    private FailedRecord(String payload, String errorMessage,
                         String errorClass, int attemptCount, String failedAtUtc) {
        this.payload      = payload;
        this.errorMessage = errorMessage;
        this.errorClass   = errorClass;
        this.attemptCount = attemptCount;
        this.failedAtUtc  = failedAtUtc;
    }

    /**
     * Creates a {@link FailedRecord} from a failed {@link Row}.
     *
     * <p>Note: {@code row.toString()} produces a Beam debug string, not JSON.
     * For replay capability, convert the row to JSON before it enters the pipeline
     * (e.g., keep the original raw_json field) and pass that string here instead.
     */
    public static FailedRecord of(Row row, Exception cause, int attemptCount) {
        return new FailedRecord(
                row.toString(),
                cause.getMessage() != null ? cause.getMessage() : "(no message)",
                cause.getClass().getName(),
                attemptCount,
                Instant.now().toString()
        );
    }

    /** Creates a {@link FailedRecord} from a raw string payload (e.g., original JSON line). */
    public static FailedRecord ofRaw(String rawPayload, Exception cause, int attemptCount) {
        return new FailedRecord(
                rawPayload,
                cause.getMessage() != null ? cause.getMessage() : "(no message)",
                cause.getClass().getName(),
                attemptCount,
                Instant.now().toString()
        );
    }

    public String getPayload()      { return payload; }
    public String getErrorMessage() { return errorMessage; }
    public String getErrorClass()   { return errorClass; }
    public int    getAttemptCount() { return attemptCount; }
    public String getFailedAtUtc()  { return failedAtUtc; }

    @Override
    public String toString() {
        return "FailedRecord{errorClass=" + errorClass
               + ", attempts=" + attemptCount
               + ", failedAt=" + failedAtUtc
               + ", message=" + errorMessage + '}';
    }
}
