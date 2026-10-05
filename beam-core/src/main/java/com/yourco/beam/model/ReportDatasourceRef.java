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
    /**
     * Lookback range, in periods of the data source's own frequency, relative to the report's
     * period: {@code 0} = the report's own period, {@code -1} = one period before it. Both 0 when
     * not configured ({@link #hasLookback()} false). Always {@code lookbackFrom >= lookbackTo}
     * (e.g. from 0 to -11 = 12 periods).
     */
    public final int lookbackFrom;
    public final int lookbackTo;
    private final boolean lookbackConfigured;

    public ReportDatasourceRef(String datasourceName, String datasourceSubprocess,
                               String transformAlias, boolean required) {
        this(datasourceName, datasourceSubprocess, transformAlias, required, null, null);
    }

    /**
     * @param lookbackFrom {@code lookback_from} — 0 or negative; null when not configured
     * @param lookbackTo   {@code lookback_to} — 0 or negative and {@code <= lookbackFrom}; null when
     *                     not configured. Both must be set together, or neither.
     * @throws IllegalArgumentException on an invalid combination (fails the config load loudly)
     */
    public ReportDatasourceRef(String datasourceName, String datasourceSubprocess,
                               String transformAlias, boolean required,
                               Integer lookbackFrom, Integer lookbackTo) {
        if ((lookbackFrom == null) != (lookbackTo == null)) {
            throw new IllegalArgumentException("datasource '" + datasourceName
                + "': lookback_from and lookback_to must be configured together");
        }
        if (lookbackFrom != null && (lookbackFrom > 0 || lookbackTo > lookbackFrom)) {
            throw new IllegalArgumentException("datasource '" + datasourceName
                + "': lookback_from (" + lookbackFrom + ") must be 0 or negative and lookback_to ("
                + lookbackTo + ") must be <= lookback_from");
        }
        this.datasourceName       = datasourceName;
        this.datasourceSubprocess = datasourceSubprocess;
        this.transformAlias       = transformAlias;
        this.required             = required;
        this.lookbackConfigured   = lookbackFrom != null;
        this.lookbackFrom         = lookbackFrom != null ? lookbackFrom : 0;
        this.lookbackTo           = lookbackTo != null ? lookbackTo : 0;
    }

    /** True when {@code lookback_from}/{@code lookback_to} are configured. */
    public boolean hasLookback() { return lookbackConfigured; }
}
