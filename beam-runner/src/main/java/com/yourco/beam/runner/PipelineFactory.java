package com.yourco.beam.runner;

import com.yourco.beam.io.sink.DeadLetterSinkTransform;
import com.yourco.beam.io.sink.SinkRouter;
import com.yourco.beam.io.source.SourceRouter;
import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.RetryPolicyType;
import com.yourco.beam.retry.ExponentialRetryPolicy;
import com.yourco.beam.retry.FixedRetryPolicy;
import com.yourco.beam.retry.RetryPolicy;
import com.yourco.beam.transform.BeamTransform;
import com.yourco.beam.transform.TransformRegistry;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles the full Beam pipeline graph from options.
 *
 * <h2>Execution order</h2>
 * <ol>
 *   <li>Read from source (GCS / BQ / Pub/Sub)</li>
 *   <li>Apply each transform in the chain; collect dead-letter side outputs</li>
 *   <li>Write successful rows to the configured sink</li>
 *   <li>Flatten all dead-letter outputs and write to the DLQ sink (C2/C3 fix)</li>
 * </ol>
 *
 * <p>No data moves during assembly — this only builds the Beam graph.
 * Data flows after {@link org.apache.beam.sdk.PipelineResult} is returned
 * from {@code pipeline.run()} in {@link Main}.
 */
public final class PipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineFactory.class);

    public Pipeline assemble(FrameworkOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        // ── Step 1: Source ────────────────────────────────────────────────────
        LOG.info("Configuring source: {}", options.getSourceType());
        PCollection<Row> current = SourceRouter.route(pipeline, options);

        // ── Step 2: Resolve transform chain ───────────────────────────────────
        String chainSpec = options.getTransformChain();
        LOG.info("Resolving transform chain: '{}'", chainSpec);
        TransformRegistry registry = TransformRegistry.load();
        List<BeamTransform> chain  = registry.resolve(chainSpec);

        // Collect dead-letter outputs from every step in the chain
        List<PCollection<FailedRecord>> deadLetterOutputs = new ArrayList<>();

        // ── Step 3: Apply transforms and wire DLQ (C2/C3 fix) ─────────────────
        RetryPolicy retryPolicy = buildRetryPolicy(options);
        LOG.info("Retry policy: {} (maxRetries={}, baseDelayMs={})",
                 options.getRetryPolicy(), options.getMaxRetries(), options.getRetryDelayMs());

        for (BeamTransform transform : chain) {
            LOG.info("  -> applying '{}'", transform.name());

            // Each transform returns PCollectionTuple: success + dead-letter
            PCollectionTuple result = current.apply(
                    transform.name(), transform.toComposite(options));

            current = result.get(BeamTransform.SUCCESS_TAG);
            deadLetterOutputs.add(result.get(BeamTransform.DEAD_LETTER_TAG));
        }

        // ── Step 4: Sink (success path) ───────────────────────────────────────
        LOG.info("Configuring sink: {}", options.getSinkType());
        SinkRouter.route(current, options);

        // ── Step 5: Dead-letter sink ───────────────────────────────────────────
        if (!deadLetterOutputs.isEmpty()) {
            String dlqSink = options.getDeadLetterSink();
            if (dlqSink != null && !dlqSink.isBlank()) {
                PCollection<FailedRecord> allFailures = PCollectionList
                        .of(deadLetterOutputs)
                        .apply("FlattenDeadLetters", Flatten.pCollections());
                allFailures.apply("WriteDLQ", new DeadLetterSinkTransform(options));
                LOG.info("Dead-letter sink configured: {}", dlqSink);
            } else {
                LOG.warn("Dead-letter outputs exist but --deadLetterSink is not set. "
                         + "Failed records will be discarded. Set --deadLetterSink to capture them.");
            }
        }

        return pipeline;
    }

    /**
     * Instantiates the correct {@link RetryPolicy} from options.
     * The policy is passed through options to each transform — transforms
     * that use {@link com.yourco.beam.retry.RetryingDoFn} internally should
     * construct it from options directly.
     */
    private RetryPolicy buildRetryPolicy(FrameworkOptions options) {
        return switch (options.getRetryPolicy()) {
            case NONE        -> new FixedRetryPolicy(0, 0);
            case FIXED       -> new FixedRetryPolicy(options.getMaxRetries(), options.getRetryDelayMs());
            case EXPONENTIAL -> new ExponentialRetryPolicy(options.getMaxRetries(), options.getRetryDelayMs());
        };
    }
}
