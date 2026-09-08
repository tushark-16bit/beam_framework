package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Complete configuration for one report run, assembled from the six BigQuery
 * config tables by {@code BigQueryReportRepository}.
 *
 * <h2>Execution order inside ReportPipelineFactory</h2>
 * <ol>
 *   <li>Period lookup — resolve {@code PerId} via {@code MSTR_Per}</li>
 *   <li>Create DaRefer row with {@code StaCd=LOADING}</li>
 *   <li>Preprocessing steps ({@link #preprocessingSteps}) — in {@code step_order} order</li>
 *   <li>Datasource availability check ({@link #datasources}) — fail fast if any required
 *       datasource has no {@code StaCd=COMPLETED} DaRefer row for this {@code PerId}</li>
 *   <li>Alias registry built: {@link ReportDatasourceRef#transformAlias} →
 *       {@code SELECT RowDaJsonTx FROM DaRec WHERE DaId = X} subquery</li>
 *   <li>Transformation chain ({@link #transformSteps}) — BQ jobs in {@code step_order} order;
 *       each result materialised to a BQ table and registered under its {@code outputAlias}</li>
 *   <li>Write final result to per-report BQ table ({@link #outputBqTable}) if configured</li>
 *   <li>Output routing ({@link #outputConfigs}) — each config dispatches to GCS, BQ, or API sink;
 *       one RptOutput row written per output step</li>
 *   <li>Email ({@link #emailConfig}) — sent only if configured; GCS outputs attached, others noted in body</li>
 *   <li>DaRefer updated to {@code StaCd=COMPLETED} (or {@code FAILED} on error)</li>
 * </ol>
 *
 * <h2>Source</h2>
 * Assembled from a single {@code parameter_store} row whose {@code parameters_val_json}
 * contains a nested JSON blob with keys: {@code override_key}, {@code datasources},
 * {@code preprocessing}, {@code transforms}, {@code outputs}, {@code email},
 * {@code output_bq_table}, {@code output_bq_input_alias}.
 * Lookup key: (parameter_group_name=parentId, parameter_data_source=reportSubprocess,
 * parameter_name=reportName).
 */
public final class ReportConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String reportName;
    public final String reportSubprocess;
    public final int    periodId;
    /**
     * When true, run even if a {@code COMPLETED} status row already exists for this
     * report + period. Acts like {@code --overrideDownload} but for reports.
     */
    public final boolean overrideKey;

    public final List<ReportDatasourceRef>    datasources;
    public final List<ReportPreprocessingStep> preprocessingSteps;
    public final List<ReportTransformStep>     transformSteps;
    public final List<ReportOutputConfig>      outputConfigs;
    /** Null if no email is configured for this report. */
    public final ReportEmailConfig emailConfig;

    /**
     * Dedicated BQ output table for this report's final result.
     * Replaces the generic {@code COM_CmnRptDtl} common table: each report
     * writes to its own table named here (e.g. {@code project.dataset.daily_trades_report}).
     * Null if no BQ table output is needed.
     */
    public final String outputBqTable;

    /**
     * Alias (from the alias registry after transforms) whose result is written to
     * {@link #outputBqTable}. When null or blank, defaults to the last transform step's
     * {@code outputAlias}, or the first datasource alias if no transforms exist.
     */
    public final String outputBqInputAlias;

    public ReportConfig(String reportName, String reportSubprocess, int periodId,
                        boolean overrideKey,
                        List<ReportDatasourceRef>    datasources,
                        List<ReportPreprocessingStep> preprocessingSteps,
                        List<ReportTransformStep>    transformSteps,
                        List<ReportOutputConfig>     outputConfigs,
                        ReportEmailConfig            emailConfig,
                        String                       outputBqTable,
                        String                       outputBqInputAlias) {
        this.reportName         = reportName;
        this.reportSubprocess   = reportSubprocess;
        this.periodId           = periodId;
        this.overrideKey        = overrideKey;
        this.datasources        = Collections.unmodifiableList(datasources);
        this.preprocessingSteps = Collections.unmodifiableList(preprocessingSteps);
        this.transformSteps     = Collections.unmodifiableList(transformSteps);
        this.outputConfigs      = Collections.unmodifiableList(outputConfigs);
        this.emailConfig        = emailConfig;
        this.outputBqTable      = outputBqTable;
        this.outputBqInputAlias = outputBqInputAlias;
    }

    public boolean hasPreprocessing()   { return !preprocessingSteps.isEmpty(); }
    public boolean hasTransforms()      { return !transformSteps.isEmpty(); }
    public boolean hasEmail()           { return emailConfig != null && !emailConfig.toList.isEmpty(); }
    public boolean hasOutputBqTable()   { return outputBqTable != null && !outputBqTable.isBlank(); }
}
