package com.yourco.beam.transform;

import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;

/**
 * SPI contract for all pluggable transforms in this framework.
 *
 * <h2>Output contract</h2>
 * Every transform returns a {@link PCollectionTuple} with exactly two tags:
 * <ul>
 *   <li>{@link #SUCCESS_TAG} — rows that were processed successfully</li>
 *   <li>{@link #DEAD_LETTER_TAG} — rows that failed after all retries</li>
 * </ul>
 * {@link com.yourco.beam.runner.PipelineFactory} collects all dead-letter
 * outputs, flattens them, and routes them to the configured DLQ sink.
 *
 * <h2>Serialization rules (MUST follow)</h2>
 * <ul>
 *   <li>Implementing class must be a <strong>named top-level or static nested class</strong>.
 *       Anonymous classes and lambdas are not safely serializable by Beam workers.</li>
 *   <li>Every field must be {@link Serializable}.</li>
 *   <li>Any {@link org.apache.beam.sdk.transforms.DoFn} returned must also be a named static class.</li>
 *   <li>Call {@code .setRowSchema()} on the success {@link PCollection} inside {@code expand()}.</li>
 * </ul>
 *
 * <h2>Registration</h2>
 * Add the fully-qualified class name to:
 * {@code META-INF/services/com.yourco.beam.transform.BeamTransform}
 */
public interface BeamTransform extends Serializable {

    /**
     * Shared output tag for successfully processed rows.
     * Interface constants are implicitly {@code public static final}.
     */
    TupleTag<Row> SUCCESS_TAG = new TupleTag<Row>("success") {};

    /**
     * Shared output tag for rows that could not be processed.
     * Collected by {@link com.yourco.beam.runner.PipelineFactory} and written to the DLQ sink.
     */
    TupleTag<FailedRecord> DEAD_LETTER_TAG = new TupleTag<FailedRecord>("dead-letter") {};

    /**
     * Stable, unique name used in the {@code --transformChain} CLI argument.
     * Example: {@code "filter-nulls"}, {@code "mask-pii"}.
     */
    String name();

    /**
     * Returns a Beam {@link PTransform} that outputs a {@link PCollectionTuple}
     * containing both successful and dead-letter outputs.
     *
     * @param options pipeline options for reading transform-specific config
     */
    PTransform<PCollection<Row>, PCollectionTuple> toComposite(FrameworkOptions options);
}
