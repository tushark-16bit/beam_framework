package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for a LOOKUP transform — enriches each main row by joining
 * against a BigQuery reference table.
 *
 * <h2>Join semantics</h2>
 * <ul>
 *   <li>Left join — main rows without a matching key pass through unchanged.</li>
 *   <li>Key: value of {@link #dataKeyField} in the main row matched against
 *       {@link #lookupKeyField} in the BQ lookup table.</li>
 *   <li>Fields listed in {@link #outputFields} are merged into the main row.
 *       Empty list means merge all fields from the lookup row.</li>
 * </ul>
 */
public final class LookupConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** BigQuery table reference {@code project.dataset.table} for the lookup data. */
    public final String bqTableRef;

    /** Field name in the lookup table to match on. */
    public final String lookupKeyField;

    /** Field name in the main data row to use as the join key. */
    public final String dataKeyField;

    /**
     * Which fields from the lookup table to merge into each main row.
     * Empty list means merge all fields.
     */
    public final List<String> outputFields;

    public LookupConfig(String bqTableRef, String lookupKeyField,
                        String dataKeyField, List<String> outputFields) {
        this.bqTableRef     = bqTableRef;
        this.lookupKeyField = lookupKeyField;
        this.dataKeyField   = dataKeyField;
        this.outputFields   = (outputFields != null)
                              ? Collections.unmodifiableList(outputFields)
                              : Collections.emptyList();
    }

    public boolean mergeAllFields() { return outputFields.isEmpty(); }

    @Override
    public String toString() {
        return "LookupConfig{bqTableRef=" + bqTableRef
            + ", joinOn=" + dataKeyField + "->" + lookupKeyField + "}";
    }
}
