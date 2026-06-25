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
 *   <li>Output routing ({@link #outputConfigs}) — each config dispatches to GCS, BQ, or API sink</li>
 *   <li>COM_CmnRptDtl write — one row per output step (all sink types)</li>
 *   <li>Email ({@link #emailConfig}) — sent only if configured; GCS outputs attached, others noted in body</li>
 *   <li>DaRefer updated to {@code StaCd=COMPLETED} (or {@code FAILED} on error)</li>
 * </ol>
 *
 * <h2>DB tables that feed this object</h2>
 * {@code report_config}, {@code report_datasource_ref},
 * {@code report_preprocessing_config}, {@code report_transformation_config},
 * {@code report_output_config}, {@code report_email_config}
 */
public final class ReportConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String reportName;
    public final String reportSubprocess;
    public final String periodId;
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

    public ReportConfig(String reportName, String reportSubprocess, String periodId,
                        boolean overrideKey,
                        List<ReportDatasourceRef>    datasources,
                        List<ReportPreprocessingStep> preprocessingSteps,
                        List<ReportTransformStep>    transformSteps,
                        List<ReportOutputConfig>     outputConfigs,
                        ReportEmailConfig            emailConfig) {
        this.reportName        = reportName;
        this.reportSubprocess  = reportSubprocess;
        this.periodId          = periodId;
        this.overrideKey       = overrideKey;
        this.datasources       = Collections.unmodifiableList(datasources);
        this.preprocessingSteps = Collections.unmodifiableList(preprocessingSteps);
        this.transformSteps    = Collections.unmodifiableList(transformSteps);
        this.outputConfigs     = Collections.unmodifiableList(outputConfigs);
        this.emailConfig       = emailConfig;
    }

    public boolean hasPreprocessing()  { return !preprocessingSteps.isEmpty(); }
    public boolean hasTransforms()     { return !transformSteps.isEmpty(); }
    public boolean hasEmail()          { return emailConfig != null && !emailConfig.toList.isEmpty(); }
}
