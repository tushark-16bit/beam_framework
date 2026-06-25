package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Reference to a data source required by a report.
 *
 * <p>Stored in {@code report_datasource_ref} table. The {@code transformAlias}
 * field is the key used in query templates: a template containing {@code {alias}}
 * is resolved to the actual BQ output table ref for this datasource.
 *
 * <p>When {@code required=true} and {@code DaRefer} has no {@code StaCd=COMPLETED} row
 * for {@code (SrceNm=datasourceName, PerId=periodId)}, the report run fails fast
 * rather than producing incomplete output.
 */
public final class ReportDatasourceRef implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String datasourceName;
    public final String datasourceSubprocess;
    /** Alias used in report transformation query templates as {@code {alias}}. */
    public final String transformAlias;
    /** When true, missing or non-COMPLETED status causes report to fail. */
    public final boolean required;

    public ReportDatasourceRef(String datasourceName, String datasourceSubprocess,
                               String transformAlias, boolean required) {
        this.datasourceName       = datasourceName;
        this.datasourceSubprocess = datasourceSubprocess;
        this.transformAlias       = transformAlias;
        this.required             = required;
    }
}
