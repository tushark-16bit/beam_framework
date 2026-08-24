package com.yourco.beam.model;

/**
 * A {@code DATA_SOURCE} step in a {@link PipelineConfig}: run this datasource first if it isn't
 * already {@code COMPLETED} for the period — the same skip-logic
 * {@code DataSourcePipelineFactory} already uses standalone.
 *
 * <p>Deliberately carries no "is this required" flag of its own. Whether a failed/incomplete
 * datasource should abort the whole pipeline before the terminal {@link ReportStep} runs is
 * decided by looking up this datasource in the report's own
 * {@code ReportConfig.datasources[].isRequired} — the pre-existing mechanism
 * {@code ReportPipelineFactory.checkDatasourceAvailability()} already enforces for every report
 * run, pipeline or standalone. A second, independently-set flag here would let the two disagree
 * about the same datasource; this step only says "run this datasource," never "is its failure
 * survivable" — that answer has exactly one source of truth.
 */
public record DataSourceStep(String datasourceName, String subprocessName)
        implements PipelineStepConfig {

    @Override public String type() { return "DATA_SOURCE"; }
}
