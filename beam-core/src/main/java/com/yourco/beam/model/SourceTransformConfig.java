package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for a single step in a per-source transformation chain.
 *
 * <p>Each {@link SourceConfig} carries an ordered list of these configs.
 * {@link com.yourco.beam.runner.SourceTransformChainAssembler} applies them left-to-right:
 * the output of each step is the input to the next.
 *
 * <h2>Supported transform types</h2>
 * <ul>
 *   <li>{@code GROUP_BY} — groups rows by {@link #groupByFields} and applies
 *       {@link #aggregations}. Output has group key fields + aggregated fields.</li>
 *   <li>{@code SORT_BY}  — sorts rows by {@link #sortByFields}. In Beam batch mode
 *       this is per-bundle ordering (not globally sorted). Use a BQ view for
 *       global ordering requirements.</li>
 *   <li>{@code LOOKUP}   — left-joins each row with a lookup table defined in
 *       {@link #lookupConfig}, enriching it with additional fields.</li>
 * </ul>
 *
 * <p>Stored in the {@code source_config} table as {@code source_transforms_json}:
 * a JSON array where each element is one transform step.
 *
 * <p>Example JSON:
 * <pre>{@code
 * [
 *   {"type": "LOOKUP", "lookupSourceType": "BQ", "lookupBqTableRef": "proj:ds.fx",
 *    "lookupKeyField": "ccy_code", "dataKeyField": "currency",
 *    "lookupOutputFields": ["rate", "description"]},
 *   {"type": "GROUP_BY", "groupByFields": ["currency", "trade_date"],
 *    "aggregations": [{"field": "amount", "function": "SUM", "outputField": "total_amount"},
 *                     {"field": "*", "function": "COUNT", "outputField": "trade_count"}]}
 * ]
 * }</pre>
 */
public final class SourceTransformConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String GROUP_BY = "GROUP_BY";
    public static final String SORT_BY  = "SORT_BY";
    public static final String LOOKUP   = "LOOKUP";

    public final String transformType;

    // ── GROUP_BY fields ──────────────────────────────────────────────────────
    /** Fields to group on. Non-null when {@code transformType = GROUP_BY}. */
    public final List<String> groupByFields;
    /** Aggregations to compute per group. Non-null when {@code transformType = GROUP_BY}. */
    public final List<AggregationConfig> aggregations;

    // ── SORT_BY fields ───────────────────────────────────────────────────────
    /** Fields to sort on. Non-null when {@code transformType = SORT_BY}. */
    public final List<String> sortByFields;
    /** Sort direction. Used when {@code transformType = SORT_BY}. */
    public final boolean sortDescending;

    // ── LOOKUP fields ────────────────────────────────────────────────────────
    /** Lookup table config. Non-null when {@code transformType = LOOKUP}. */
    public final LookupConfig lookupConfig;

    private SourceTransformConfig(String transformType,
                                  List<String> groupByFields, List<AggregationConfig> aggregations,
                                  List<String> sortByFields, boolean sortDescending,
                                  LookupConfig lookupConfig) {
        this.transformType  = transformType;
        this.groupByFields  = groupByFields  != null ? Collections.unmodifiableList(groupByFields)  : null;
        this.aggregations   = aggregations   != null ? Collections.unmodifiableList(aggregations)   : null;
        this.sortByFields   = sortByFields   != null ? Collections.unmodifiableList(sortByFields)   : null;
        this.sortDescending = sortDescending;
        this.lookupConfig   = lookupConfig;
    }

    public static SourceTransformConfig groupBy(List<String> groupByFields,
                                                List<AggregationConfig> aggregations) {
        return new SourceTransformConfig(GROUP_BY, groupByFields, aggregations, null, false, null);
    }

    public static SourceTransformConfig sortBy(List<String> sortByFields, boolean descending) {
        return new SourceTransformConfig(SORT_BY, null, null, sortByFields, descending, null);
    }

    public static SourceTransformConfig lookup(LookupConfig lookupConfig) {
        return new SourceTransformConfig(LOOKUP, null, null, null, false, lookupConfig);
    }

    public boolean isGroupBy() { return GROUP_BY.equals(transformType); }
    public boolean isSortBy()  { return SORT_BY.equals(transformType); }
    public boolean isLookup()  { return LOOKUP.equals(transformType); }

    @Override
    public String toString() {
        return "SourceTransformConfig{type=" + transformType + "}";
    }
}
