package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for a lookup table used in a LOOKUP transform.
 *
 * <p>The lookup table is also fetched from the parameter DB (the config says
 * where to find it — not a hardcoded reference). Its data is loaded into
 * a Beam side input ({@code PCollectionView<Map<String, Row>>}) and used to
 * enrich each row in the main data source.
 *
 * <h2>Join semantics</h2>
 * <ul>
 *   <li>Join type: left join — main rows without a matching key pass through unchanged.</li>
 *   <li>Key extraction: the value of {@link #dataKeyField} in each main Row is looked up
 *       against the {@link #lookupKeyField} in the lookup table.</li>
 *   <li>The fields listed in {@link #outputFields} are merged into the main Row.
 *       If {@link #outputFields} is empty, ALL lookup fields are merged.</li>
 * </ul>
 *
 * <p>The lookup data source is defined by {@link #sourceType}: {@code BQ} reads
 * from a BigQuery table; {@code JDBC} runs a SQL query against the parameter DB.
 */
public final class LookupConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SOURCE_BQ   = "BQ";
    public static final String SOURCE_JDBC = "JDBC";

    /** Source type for lookup data: {@code BQ} or {@code JDBC}. */
    public final String sourceType;

    /** BigQuery table reference {@code project:dataset.table}. Used when {@code sourceType=BQ}. */
    public final String bqTableRef;

    /** SQL query to run against the parameter DB. Used when {@code sourceType=JDBC}. */
    public final String jdbcQuery;

    /** Field name in the lookup table to match on. */
    public final String lookupKeyField;

    /** Field name in the main data table to use as the join key. */
    public final String dataKeyField;

    /**
     * Which fields from the lookup table to merge into each main row.
     * Empty list means merge all fields from the lookup row.
     */
    public final List<String> outputFields;

    public LookupConfig(String sourceType, String bqTableRef, String jdbcQuery,
                        String lookupKeyField, String dataKeyField, List<String> outputFields) {
        this.sourceType     = sourceType;
        this.bqTableRef     = bqTableRef;
        this.jdbcQuery      = jdbcQuery;
        this.lookupKeyField = lookupKeyField;
        this.dataKeyField   = dataKeyField;
        this.outputFields   = (outputFields != null) ? Collections.unmodifiableList(outputFields)
                                                     : Collections.emptyList();
    }

    public boolean isBqSource()   { return SOURCE_BQ.equalsIgnoreCase(sourceType); }
    public boolean isJdbcSource() { return SOURCE_JDBC.equalsIgnoreCase(sourceType); }
    public boolean mergeAllFields() { return outputFields.isEmpty(); }

    @Override
    public String toString() {
        return "LookupConfig{source=" + sourceType
            + ", joinOn=" + dataKeyField + "->" + lookupKeyField + "}";
    }
}
