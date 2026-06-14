package com.yourco.beam.model;

import java.io.Serializable;

/**
 * A single Balance and Control (BnC) check for a fetched data source.
 *
 * <p>After the pipeline writes data to the output table, the driver JVM queries
 * {@code SUM(field)} from the output and compares it to {@link #expectedTotal}.
 * The check passes if the absolute percentage difference is within {@link #tolerancePct}.
 *
 * <p>Stored as elements inside the {@code bnc_rules_json} column of {@code source_config}.
 *
 * <p>Example JSON element:
 * <pre>{@code {"field": "amount", "expectedTotal": 1000000.0, "tolerancePct": 0.01}}</pre>
 * This means: SUM(amount) must be within 1% of 1,000,000.
 */
public final class BncRule implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Numeric field to sum in the output table. Must be a numeric column. */
    public final String field;

    /** Expected sum value. The BnC check compares the actual sum to this. */
    public final double expectedTotal;

    /**
     * Allowed fractional deviation from the expected total.
     * {@code 0.01} means the actual sum must be within 1% of {@link #expectedTotal}.
     * {@code 0.0} means exact match (rarely practical due to floating point).
     */
    public final double tolerancePct;

    public BncRule(String field, double expectedTotal, double tolerancePct) {
        this.field         = field;
        this.expectedTotal = expectedTotal;
        this.tolerancePct  = tolerancePct;
    }

    /**
     * Returns true if the given actual sum is within tolerance of the expected total.
     */
    public boolean passes(double actualTotal) {
        if (expectedTotal == 0.0) return actualTotal == 0.0;
        double deviation = Math.abs(actualTotal - expectedTotal) / Math.abs(expectedTotal);
        return deviation <= tolerancePct;
    }

    @Override
    public String toString() {
        return "BncRule{SUM(" + field + ") ≈ " + expectedTotal
            + " ±" + (tolerancePct * 100) + "%}";
    }
}
