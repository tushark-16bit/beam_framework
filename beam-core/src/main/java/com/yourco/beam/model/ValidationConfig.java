package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Post-fetch validation rules for one data source.
 *
 * <p>Validation runs in the driver JVM <em>after</em> the Beam pipeline writes the
 * output to BigQuery. Three checks are supported:
 *
 * <ol>
 *   <li><b>Header check</b> — verifies that all columns in {@link #requiredHeaders}
 *       exist in the output BQ table schema. Runs at pipeline-assembly time so the
 *       job fails early if expected fields are missing.</li>
 *   <li><b>Row count check</b> — queries {@code SELECT COUNT(*)} from the output table
 *       and confirms the count is within [{@link #minRowCount}, {@link #maxRowCount}].</li>
 *   <li><b>Balance and Control (BnC)</b> — for each {@link BncRule}, queries
 *       {@code SELECT SUM(field)} from the output table and checks it against the
 *       expected total within the configured tolerance.</li>
 * </ol>
 *
 * <p>Set {@link #minRowCount} to {@code 0} to disable the minimum check.
 * Set {@link #maxRowCount} to {@code -1} to disable the maximum check.
 *
 * <p>Stored in the {@code source_config} table across several columns:
 * {@code min_row_count}, {@code max_row_count}, {@code required_headers_json},
 * {@code bnc_rules_json}.
 */
public final class ValidationConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final long NO_MIN = 0L;
    public static final long NO_MAX = -1L;

    /** Minimum acceptable row count in the output. {@code 0} = no check. */
    public final long minRowCount;

    /** Maximum acceptable row count in the output. {@code -1} = no check. */
    public final long maxRowCount;

    /**
     * Column names that must be present in the data schema.
     * Checked at pipeline-assembly time against the fetched data schema.
     * Empty list = no header check.
     */
    public final List<String> requiredHeaders;

    /**
     * Balance and Control rules. Each rule verifies that the sum of a numeric column
     * matches an expected total within a tolerance percentage.
     * Empty list = no BnC checks.
     */
    public final List<BncRule> bncRules;

    public ValidationConfig(long minRowCount, long maxRowCount,
                            List<String> requiredHeaders, List<BncRule> bncRules) {
        this.minRowCount     = minRowCount;
        this.maxRowCount     = maxRowCount;
        this.requiredHeaders = requiredHeaders != null ? Collections.unmodifiableList(requiredHeaders)
                                                      : Collections.emptyList();
        this.bncRules        = bncRules != null ? Collections.unmodifiableList(bncRules)
                                               : Collections.emptyList();
    }

    public boolean hasMinRowCheck()    { return minRowCount > 0; }
    public boolean hasMaxRowCheck()    { return maxRowCount >= 0; }
    public boolean hasHeaderCheck()    { return !requiredHeaders.isEmpty(); }
    public boolean hasBncCheck()       { return !bncRules.isEmpty(); }
    public boolean hasAnyCheck()       { return hasMinRowCheck() || hasMaxRowCheck()
                                             || hasHeaderCheck() || hasBncCheck(); }

    public static ValidationConfig none() {
        return new ValidationConfig(NO_MIN, NO_MAX, Collections.emptyList(), Collections.emptyList());
    }
}
