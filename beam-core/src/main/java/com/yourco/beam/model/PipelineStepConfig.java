package com.yourco.beam.model;

import java.io.Serializable;

/**
 * One step in a {@link PipelineConfig} sequence. Closed hierarchy — exactly two kinds of step
 * exist ({@link DataSourceStep}, {@link ReportStep}), enforced by the compiler via {@code permits}
 * rather than a string discriminator paired with fields that are only meaningful for one kind.
 *
 * <p>Java 17 does not have finalized pattern-matching {@code switch} over sealed types (that
 * lands in Java 21, and this project does not compile with {@code --enable-preview}) — dispatch
 * is a plain {@code instanceof} chain:
 * <pre>{@code
 * if (step instanceof DataSourceStep ds) {
 *     ...
 * } else if (step instanceof ReportStep rs) {
 *     ...
 * }
 * }</pre>
 * The {@code permits} clause still gives a compiler-enforced closed set — no third step kind can
 * exist without editing this file — even without automatic exhaustiveness checking on the chain.
 */
public sealed interface PipelineStepConfig extends Serializable
        permits DataSourceStep, ReportStep {

    /** Raw {@code type} string from the {@code parameter_store} JSON step object. */
    String type();
}
