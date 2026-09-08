package com.yourco.beam.orchestrator.model;

import java.util.Collections;
import java.util.Map;

/**
 * Describes one pipeline invocation to be scheduled.
 *
 * <p>Produced by {@link com.yourco.beam.orchestrator.schedule.RunScheduleResolver}.
 * Each {@link RunSpec} becomes one {@link TaskItem} row in the task table and one
 * entry in the GCS manifest.
 *
 * <h2>runType values</h2>
 * <ul>
 *   <li>{@code DATA_SOURCE_DOWNLOAD} — maps to {@code --processType=DATA_SOURCE_DOWNLOAD}
 *       in the pipeline JAR invocation</li>
 *   <li>{@code REPORT_PROCESSING}    — maps to {@code --processType=REPORT_PROCESSING}</li>
 * </ul>
 *
 * <h2>extraParams</h2>
 * A freeform string→string map persisted to {@code extra_params_json} in the task table
 * and included in the manifest. Use it for any pipeline flag that doesn't have a
 * dedicated field here (e.g. {@code overrideDownload=true}, {@code runner=DataflowRunner}).
 */
public final class RunSpec {

    public static final String TYPE_DATA_SOURCE_DOWNLOAD = "DATA_SOURCE_DOWNLOAD";
    public static final String TYPE_REPORT_PROCESSING    = "REPORT_PROCESSING";

    public final String         runType;
    public final String         parentId;
    public final String         name;        // datasourceName or reportName
    public final String         subprocess;  // subprocessName or reportSubprocess
    public final ResolvedPeriod period;
    public final int            runOrder;    // lower = runs first; default 999
    public final Map<String, String> extraParams;

    public RunSpec(String runType, String parentId, String name, String subprocess,
                   ResolvedPeriod period, int runOrder, Map<String, String> extraParams) {
        this.runType     = runType;
        this.parentId    = parentId;
        this.name        = name;
        this.subprocess  = subprocess;
        this.period      = period;
        this.runOrder    = runOrder;
        this.extraParams = extraParams != null
                           ? Collections.unmodifiableMap(extraParams)
                           : Collections.emptyMap();
    }

    @Override
    public String toString() {
        return "RunSpec{" + runType + " | " + parentId + "/" + name + "/" + subprocess
            + " | period=" + period.periodId + " | order=" + runOrder + "}";
    }
}
