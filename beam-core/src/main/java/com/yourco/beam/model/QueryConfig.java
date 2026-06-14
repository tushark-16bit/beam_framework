package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Configurable query template with injectable parameters for a data source fetch.
 *
 * <p>The {@link #queryTemplate} may contain placeholder tokens that are resolved at
 * runtime before the query is executed:
 *
 * <ul>
 *   <li>{@code {periodStart}} — from {@code --periodStart} CLI option</li>
 *   <li>{@code {periodEnd}}   — from {@code --periodEnd} CLI option</li>
 *   <li>{@code {periodId}}    — from {@code --periodId} CLI option</li>
 *   <li>{@code {runDate}}     — from {@code --runDate} (or today if not set)</li>
 *   <li>{@code {key}}         — from any entry in {@link #paramMappings}</li>
 * </ul>
 *
 * <p>Param mappings themselves may also reference standard tokens, e.g.:
 * {@code {"startDate": "{periodStart}", "exchange": "NYSE"}}.
 *
 * <p>Resolution is handled by {@code QueryParameterResolver} in beam-utils.
 *
 * <p>Stored in the {@code source_config} table under {@code bq_query} (template)
 * and {@code query_params_json} (param mapping JSON object).
 */
public final class QueryConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * SQL query template. May contain placeholder tokens.
     * Null means use the table reference directly (no custom SQL).
     */
    public final String queryTemplate;

    /**
     * Additional named parameters to inject into the query template.
     * Values can themselves reference standard tokens like {@code {periodStart}}.
     * Example: {@code {"startDate": "{periodStart}", "exchange": "NYSE"}}.
     */
    public final Map<String, String> paramMappings;

    public QueryConfig(String queryTemplate, Map<String, String> paramMappings) {
        this.queryTemplate = queryTemplate;
        this.paramMappings = (paramMappings != null) ? Collections.unmodifiableMap(paramMappings)
                                                     : Collections.emptyMap();
    }

    public boolean hasTemplate() {
        return queryTemplate != null && !queryTemplate.isBlank();
    }

    public static QueryConfig empty() {
        return new QueryConfig(null, Collections.emptyMap());
    }
}
