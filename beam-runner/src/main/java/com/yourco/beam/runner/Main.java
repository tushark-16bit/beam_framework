package com.yourco.beam.runner;

import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Beam pipeline framework.
 *
 * <h2>Local / DirectRunner</h2>
 * <pre>{@code
 * java -jar beam-runner-bundled.jar \
 *   --runner=DirectRunner \
 *   --sourceType=GCS \
 *   --gcsSourcePath=gs://my-bucket/input/*.json \
 *   --transformChain=filter-nulls,mask-pii \
 *   --sinkType=GCS \
 *   --gcsSinkPath=gs://my-bucket/output/ \
 *   --deadLetterSink=gs://my-bucket/dlq/
 * }</pre>
 *
 * <h2>Dataflow (batch)</h2>
 * <pre>{@code
 * java -jar beam-runner-bundled.jar \
 *   --runner=DataflowRunner \
 *   --project=my-gcp-project \
 *   --region=us-central1 \
 *   --tempLocation=gs://my-bucket/temp \
 *   --sourceType=BQ \
 *   --bqSourceTable=my-project:my-dataset.input \
 *   --transformChain=filter-nulls,mask-pii \
 *   --sinkType=BQ \
 *   --bqSinkTable=my-project:my-dataset.output \
 *   --writeDisposition=TRUNCATE \
 *   --retryPolicy=EXPONENTIAL \
 *   --maxRetries=3 \
 *   --deadLetterSink=gs://my-bucket/dlq/
 * }</pre>
 *
 * <h2>Dataflow (streaming)</h2>
 * Same as batch but use {@code --sourceType=PUBSUB}. The process does NOT
 * block waiting for completion — streaming jobs run indefinitely until cancelled.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOG.info("Starting Beam Pipeline Framework");

        FrameworkOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(FrameworkOptions.class);

        LOG.info("Source:      {}", options.getSourceType());
        LOG.info("Chain:       {}", options.getTransformChain());
        LOG.info("Sink:        {}", options.getSinkType());
        LOG.info("Retry:       {} (max={}, delayMs={})",
                 options.getRetryPolicy(), options.getMaxRetries(), options.getRetryDelayMs());
        LOG.info("DLQ sink:    {}", options.getDeadLetterSink());

        Pipeline pipeline = new PipelineFactory().assemble(options);

        LOG.info("Submitting to runner: {}", options.getRunner().getSimpleName());
        PipelineResult result = pipeline.run();

        // I8 fix: only block for batch sources. Streaming (Pub/Sub) runs indefinitely —
        // blocking here would prevent the process from ever returning, which is fine for
        // a long-running service but wrong for a batch job that Airflow needs to observe.
        if (isBatchSource(options.getSourceType())) {
            result.waitUntilFinish();
            LOG.info("Pipeline finished with state: {}", result.getState());
        } else {
            LOG.info("Streaming pipeline submitted. Job running indefinitely until cancelled.");
        }
    }

    /** Returns {@code true} for sources that produce a bounded {@link PipelineResult}. */
    private static boolean isBatchSource(SourceType sourceType) {
        return switch (sourceType) {
            case GCS, BQ -> true;
            case PUBSUB  -> false;
        };
    }
}
