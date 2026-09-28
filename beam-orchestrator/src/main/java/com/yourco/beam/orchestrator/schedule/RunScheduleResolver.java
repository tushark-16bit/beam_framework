package com.yourco.beam.orchestrator.schedule;

import com.yourco.beam.orchestrator.model.ResolvedPeriod;
import com.yourco.beam.orchestrator.model.RunSpec;

import java.util.List;

/**
 * Determines which pipeline invocations to create tasks for in a given run.
 *
 * <p>Implement this interface to change the source of schedule data — e.g. read
 * from a YAML file, a REST endpoint, or a different BQ table.
 *
 * <p>The default implementation is {@link BigQueryRunScheduleResolver}, which reads
 * {@code parameter_store} rows that carry {@code "run_type"} in their
 * {@code parameters_val_json}, filtering by {@code parentId} and {@code frequency}.
 *
 * <p>Returned specs should be ordered by their intended execution order (sources
 * before reports, lower {@code run_order} first).
 */
@FunctionalInterface
public interface RunScheduleResolver {

    /**
     * @param parentId  the business group identifier (maps to {@code parameter_group_name})
     * @param frequency the frequency this orchestrator invocation is running for
     * @param period    the resolved period; passed so implementations can filter by date
     *                  or inject period-specific context into extra params
     * @return ordered list of run specs to create tasks for; empty list if nothing to run
     */
    List<RunSpec> resolve(String parentId, String frequency, ResolvedPeriod period);
}
