package com.yourco.beam.model;

/**
 * The terminal {@code REPORT} step in a {@link PipelineConfig}. {@link PipelineConfig}'s compact
 * constructor enforces that this is always the last step in the sequence.
 */
public record ReportStep(String reportName, String reportSubprocess)
        implements PipelineStepConfig {

    @Override public String type() { return "REPORT"; }
}
