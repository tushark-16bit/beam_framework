package com.yourco.beam.options;

/** Retry strategies available to all pipelines. Passed via --retryPolicy on the CLI. */
public enum RetryPolicyType {
    /** No retry — failures go straight to the dead-letter sink. */
    NONE,
    /** Fixed delay between retries (--retryDelayMs controls the interval). */
    FIXED,
    /** Exponential back-off with jitter. */
    EXPONENTIAL
}
