package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Defines one aggregation step in a GROUP BY transform.
 *
 * <p>Used inside {@link SourceTransformConfig} when {@code transformType = GROUP_BY}.
 * Each instance aggregates one field using the specified function and writes the
 * result to {@link #outputField} in the output Row.
 */
public final class AggregationConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SUM   = "SUM";
    public static final String COUNT = "COUNT";
    public static final String AVG   = "AVG";
    public static final String MIN   = "MIN";
    public static final String MAX   = "MAX";

    /** Field to aggregate. Use {@code "*"} for COUNT(*). */
    public final String field;

    /** Aggregation function: {@code SUM | COUNT | AVG | MIN | MAX}. */
    public final String function;

    /** Name for the resulting field in the output Row. */
    public final String outputField;

    public AggregationConfig(String field, String function, String outputField) {
        this.field       = field;
        this.function    = function;
        this.outputField = outputField;
    }

    @Override
    public String toString() {
        return function + "(" + field + ") AS " + outputField;
    }
}
