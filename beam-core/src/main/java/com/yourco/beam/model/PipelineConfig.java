package com.yourco.beam.model;

import java.io.Serializable;
import java.util.List;

/**
 * Full config for a {@code PIPELINE} run: an ordered sequence of steps, always terminating in a
 * {@link ReportStep}. Fetched from {@code parameter_store} as {@code {"steps": [...]}} — a thin
 * ordered pointer list, not a duplicate of the actual per-datasource / per-report config, which
 * is still fetched by name via the existing {@code BigQuerySourceConfigRepository} /
 * {@code BigQueryReportRepository}.
 */
public record PipelineConfig(List<PipelineStepConfig> steps) implements Serializable {

    public PipelineConfig {
        if (steps == null || steps.isEmpty()) {
            throw new IllegalArgumentException("pipeline steps must not be empty");
        }
        steps = List.copyOf(steps);
        PipelineStepConfig last = steps.get(steps.size() - 1);
        if (!(last instanceof ReportStep)) {
            throw new IllegalArgumentException(
                "last pipeline step must be type=REPORT, got: " + last.type());
        }
    }
}
