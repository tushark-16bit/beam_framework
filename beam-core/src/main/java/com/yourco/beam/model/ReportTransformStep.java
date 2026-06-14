package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * One step in a report's chained transformation sequence.
 *
 * <p>Steps are stored in {@code report_transformation_config} and executed in
 * {@code step_order} order. The result of step N becomes available as
 * {@code output_alias} for step N+1. The initial aliases come from
 * {@link ReportDatasourceRef#transformAlias}.
 *
 * <h2>Query template token resolution (three layers, in order)</h2>
 * <ol>
 *   <li><b>Alias tokens</b> — {@code {alias}} replaced with the backtick-quoted BQ
 *       table ref for that alias (datasource output or prior step result).</li>
 *   <li><b>Standard tokens</b> — {@code {periodStart}}, {@code {periodEnd}},
 *       {@code {periodId}}, {@code {runDate}} — from pipeline options.</li>
 *   <li><b>Custom tokens</b> — any key in {@code queryParams} (stored in
 *       {@code query_params_json} column). Values may themselves reference standard
 *       tokens. Any number of custom tokens are supported.
 *       Example: {@code {"exchange": "NYSE", "threshold": "10000"}}
 *       resolves {@code {exchange}} → {@code NYSE} and {@code {threshold}} → {@code 10000}.</li>
 * </ol>
 *
 * <h2>Example template</h2>
 * <pre>{@code
 * SELECT t.trade_id, t.amount * f.rate AS amount_usd
 * FROM {trades} t
 * JOIN {fx_rates} f ON t.currency = f.currency_code
 * WHERE t.trade_date BETWEEN '{periodStart}' AND '{periodEnd}'
 *   AND t.exchange = '{exchange}'
 *   AND t.amount > {threshold}
 * }</pre>
 *
 * <h2>Materialization</h2>
 * The resolved query is run as a BQ job. The result is written (WRITE_TRUNCATE) to
 * {@code outputBqTable}. That table ref is then registered under {@code outputAlias}
 * so subsequent steps and output configs can reference it.
 */
public final class ReportTransformStep implements Serializable {

    private static final long serialVersionUID = 1L;

    public final int    stepOrder;
    public final String stepName;
    /** Alias that this step reads as its primary input. */
    public final String inputAlias;
    /** Alias under which this step's result table is registered after execution. */
    public final String outputAlias;
    /** BQ Standard SQL with alias, standard, and custom tokens. */
    public final String queryTemplate;
    /** Destination table for the materialised result ({@code project.dataset.table}). */
    public final String outputBqTable;
    /**
     * Custom token → value mappings for this step's query template.
     * Stored in {@code report_transformation_config.query_params_json}.
     * Values may reference standard tokens (e.g. {@code "{periodStart}"}).
     * Any number of entries are supported.
     */
    public final Map<String, String> queryParams;

    public ReportTransformStep(int stepOrder, String stepName,
                               String inputAlias, String outputAlias,
                               String queryTemplate, String outputBqTable,
                               Map<String, String> queryParams) {
        this.stepOrder     = stepOrder;
        this.stepName      = stepName;
        this.inputAlias    = inputAlias;
        this.outputAlias   = outputAlias;
        this.queryTemplate = queryTemplate;
        this.outputBqTable = outputBqTable;
        this.queryParams   = queryParams != null ? Collections.unmodifiableMap(queryParams)
                                                 : Collections.emptyMap();
    }
}
