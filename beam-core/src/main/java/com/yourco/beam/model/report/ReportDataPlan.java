package com.yourco.beam.model.report;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Which data source periods a report run must find {@code COMPLETED} and which of them it reads,
 * per data source — worked out in the driver JVM (it needs each data source's schedule and
 * calendar) and carried to wherever the report executes, including a Beam worker, as plain
 * serializable data.
 *
 * <p>For a data source with no entry the answer is the report's own period — the long-standing
 * behaviour when the data source and the report have the same frequency. An entry exists when:
 * <ul>
 *   <li>the data source is <b>finer</b> than the report (owner decision D7, e.g. daily data
 *       sources for a monthly report): both lists are every data source period inside the report
 *       period — all must be {@code COMPLETED}, and the rows of all of them are staged; or</li>
 *   <li>the data source has a lookback ({@code lookback_from}/{@code lookback_to}): the
 *       <i>validate</i> list is the whole lookback range, the <i>stage</i> list stays the report's
 *       own period.</li>
 * </ul>
 */
public final class ReportDataPlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private final HashMap<String, List<Integer>> validate = new HashMap<>();
    private final HashMap<String, List<Integer>> stage    = new HashMap<>();

    public static ReportDataPlan empty() {
        return new ReportDataPlan();
    }

    /** Records the periods for one data source (by datasource name). */
    public ReportDataPlan put(String datasourceName, List<Integer> validatePeriods, List<Integer> stagePeriods) {
        validate.put(datasourceName, Collections.unmodifiableList(new ArrayList<>(validatePeriods)));
        stage.put(datasourceName, Collections.unmodifiableList(new ArrayList<>(stagePeriods)));
        return this;
    }

    /** Periods of {@code datasourceName} that must be COMPLETED; the report's own period if none recorded. */
    public List<Integer> validatePeriods(String datasourceName, int reportPeriodId) {
        List<Integer> periods = validate.get(datasourceName);
        return periods != null ? periods : List.of(reportPeriodId);
    }

    /** Periods of {@code datasourceName} whose rows the report reads; the report's own period if none recorded. */
    public List<Integer> stagePeriods(String datasourceName, int reportPeriodId) {
        List<Integer> periods = stage.get(datasourceName);
        return periods != null ? periods : List.of(reportPeriodId);
    }

    /** True when this data source has its own entry (finer than the report, or a lookback). */
    public boolean hasEntry(String datasourceName) {
        return validate.containsKey(datasourceName);
    }

    public Map<String, List<Integer>> validateByDatasource() {
        return Collections.unmodifiableMap(validate);
    }

    @Override
    public String toString() {
        return "ReportDataPlan{validate=" + validate + ", stage=" + stage + "}";
    }
}
