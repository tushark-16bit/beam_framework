package com.yourco.beam.runner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

/**
 * Result of {@link DataSourcePipelineFactory#assembleForConfigs}: the assembled pipeline plus
 * one {@code PCollection<Long>} per source branch — {@link PostDownloadFinalizeTransform}'s
 * output signal, emitted after that source reaches a terminal checkpoint state (success or
 * failure).
 *
 * <p>{@link PipelineSequenceFactory} passes {@link #finalizeSignals} into {@code Wait.on()} to
 * gate a report step wired onto this same pipeline, so the report only runs once every batched
 * datasource has actually finished — a direct Beam data-dependency barrier, not a poll loop.
 */
final class DataSourceAssembly {

    final Pipeline pipeline;
    final List<PCollection<?>> finalizeSignals;

    DataSourceAssembly(Pipeline pipeline, List<PCollection<?>> finalizeSignals) {
        this.pipeline        = pipeline;
        this.finalizeSignals = finalizeSignals;
    }
}
