package com.yourco.beam.runner;

import com.yourco.beam.io.params.BigQueryParameterAdapterImpl;
import com.yourco.beam.io.sink.DeadLetterSinkTransform;
import com.yourco.beam.io.sink.SinkRouter;
import com.yourco.beam.io.source.SourceRouter;
import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.model.PipelineRunConfig;
import com.yourco.beam.options.FrameworkOptions;
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
 * Assembles the full Beam pipeline graph from per-datasource config loaded from parameter_store.
 *
 * <h2>Execution order</h2>
 * <ol>
 *   <li>Load {@link PipelineRunConfig} from parameter_store via {@link BigQueryParameterAdapterImpl}</li>
 *   <li>Read from source (GCS / BQ / Pub/Sub)</li>
 *   <li>Apply each transform in the chain; collect dead-letter side outputs</li>
 *   <li>Write successful rows to the configured sink</li>
 *   <li>Flatten all dead-letter outputs and write to the DLQ sink</li>
 * </ol>
 *
 * <p>No data moves during assembly — this only builds the Beam graph.
 * Data flows after {@link org.apache.beam.sdk.PipelineResult} is returned
 * from {@code pipeline.run()} in {@link Main}.
 */
public final class PipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineFactory.class);

    private PipelineRunConfig loadedRunConfig;

    public Pipeline assemble(FrameworkOptions options) {
        // ── Step 0: Load per-datasource config from parameter_store ───────────
        BigQueryParameterAdapterImpl paramAdapter = new BigQueryParameterAdapterImpl(options);
        this.loadedRunConfig = new PipelineRunConfig(paramAdapter.fetchParameters(
                options.getParentId(),
                options.getDatasourceName(),
                options.getSubprocessName()));
        LOG.info("Loaded PipelineRunConfig: sourceType={} sinkType={} chain='{}'",
                 loadedRunConfig.getSourceType(), loadedRunConfig.getSinkType(),
                 loadedRunConfig.getTransformChain());

        Pipeline pipeline = Pipeline.create(options);

        // ── Step 1: Source ────────────────────────────────────────────────────
        LOG.info("Configuring source: {}", loadedRunConfig.getSourceType());
        PCollection<Row> current = SourceRouter.route(pipeline, loadedRunConfig);

        // ── Step 2: Resolve transform chain ───────────────────────────────────
        String chainSpec = loadedRunConfig.getTransformChain();
        LOG.info("Resolving transform chain: '{}'", chainSpec);
        TransformRegistry registry = TransformRegistry.load();
        List<BeamTransform> chain  = registry.resolve(chainSpec);

        // Collect dead-letter outputs from every step in the chain
        List<PCollection<FailedRecord>> deadLetterOutputs = new ArrayList<>();

        // ── Step 3: Apply transforms and wire DLQ ─────────────────────────────
        RetryPolicy retryPolicy = buildRetryPolicy(loadedRunConfig);
        LOG.info("Retry policy: {} (maxRetries={}, baseDelayMs={})",
                 loadedRunConfig.getRetryPolicy(), loadedRunConfig.getMaxRetries(),
                 loadedRunConfig.getRetryDelayMs());

        for (BeamTransform transform : chain) {
            LOG.info("  -> applying '{}'", transform.name());
            PCollectionTuple result = current.apply(
                    transform.name(), transform.toComposite(options, loadedRunConfig));
            current = result.get(BeamTransform.SUCCESS_TAG);
            deadLetterOutputs.add(result.get(BeamTransform.DEAD_LETTER_TAG));
        }

        // ── Step 4: Sink (success path) ───────────────────────────────────────
        LOG.info("Configuring sink: {}", loadedRunConfig.getSinkType());
        SinkRouter.route(current, loadedRunConfig);

        // ── Step 5: Dead-letter sink ───────────────────────────────────────────
        if (!deadLetterOutputs.isEmpty()) {
            String dlqSink = loadedRunConfig.getDeadLetterSink();
            if (dlqSink != null && !dlqSink.isBlank()) {
                PCollection<FailedRecord> allFailures = PCollectionList
                        .of(deadLetterOutputs)
                        .apply("FlattenDeadLetters", Flatten.pCollections());
                allFailures.apply("WriteDLQ", new DeadLetterSinkTransform(dlqSink));
                LOG.info("Dead-letter sink configured: {}", dlqSink);
            } else {
                LOG.warn("Dead-letter outputs exist but dead_letter_sink is not set in parameter_store. "
                         + "Failed records will be discarded.");
            }
        }

        return pipeline;
    }

    /** Returns true if the configured source type is streaming (Pub/Sub). */
    public boolean isStreamingSource() {
        if (loadedRunConfig == null) return false;
        return switch (loadedRunConfig.getSourceType()) {
            case PUBSUB -> true;
            default     -> false;
        };
    }

    private RetryPolicy buildRetryPolicy(PipelineRunConfig runConfig) {
        return switch (runConfig.getRetryPolicy()) {
            case NONE        -> new FixedRetryPolicy(0, 0);
            case FIXED       -> new FixedRetryPolicy(runConfig.getMaxRetries(), runConfig.getRetryDelayMs());
            case EXPONENTIAL -> new ExponentialRetryPolicy(runConfig.getMaxRetries(), runConfig.getRetryDelayMs());
        };
    }
}
