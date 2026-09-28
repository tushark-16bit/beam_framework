package com.yourco.beam.orchestrator;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * CLI options for the orchestrator JAR.
 *
 * <p>Parsed from {@code --key=value} arguments. No Beam / PipelineOptions dependency.
 *
 * <h2>Required flags</h2>
 * <pre>
 *   --parentId         Business group; maps to parameter_group_name in parameter_store
 *   --runDate          yyyy-MM-dd; the date this orchestrator run is for
 *   --frequency        DAILY | MONTHLY | WEEKLY (or custom if PeriodResolver extended)
 *   --paramBqProject   GCP project hosting the parameter_store table
 * </pre>
 *
 * <h2>Optional flags (all have defaults)</h2>
 * <pre>
 *   --runId              Auto-generated as "{parentId}-{frequency}-{runDate}-{uuid[:8]}"
 *   --paramBqDataset     default: dw
 *   --paramStoreTable    default: parameter_store
 *   --taskBqProject      default: paramBqProject
 *   --taskBqDataset      default: pipeline_orchestration
 *   --taskTable          default: orchestrator_tasks
 *   --manifestGcsBucket  GCS bucket name without gs:// prefix; omit to skip GCS write
 *   --manifestGcsPath    GCS object path template; default: manifests/{runId}/tasks.json
 * </pre>
 */
public final class OrchestratorOptions {

    // Required
    private String    parentId;
    private LocalDate runDate;
    private String    frequency;
    private String    paramBqProject;

    // Optional with defaults
    private String runId;
    private String paramBqDataset  = "dw";
    private String paramStoreTable = "parameter_store";
    private String taskBqProject;      // defaults to paramBqProject
    private String taskBqDataset   = "pipeline_orchestration";
    private String taskTable       = "orchestrator_tasks";
    private String manifestGcsBucket;  // null → skip GCS manifest write
    private String manifestGcsPath;    // null → use GcsManifestWriter.DEFAULT_PATH_TEMPLATE

    private OrchestratorOptions() {}

    public static OrchestratorOptions parse(String[] args) {
        Map<String, String> raw = parseArgs(args);
        OrchestratorOptions opts = new OrchestratorOptions();

        opts.parentId       = require(raw, "parentId");
        opts.frequency      = require(raw, "frequency").toUpperCase();
        opts.paramBqProject = require(raw, "paramBqProject");
        opts.runDate        = parseDate(require(raw, "runDate"));

        opts.runId           = raw.getOrDefault("runId",
            opts.parentId + "-" + opts.frequency + "-" + opts.runDate
            + "-" + java.util.UUID.randomUUID().toString().substring(0, 8));

        if (raw.containsKey("paramBqDataset"))  opts.paramBqDataset  = raw.get("paramBqDataset");
        if (raw.containsKey("paramStoreTable")) opts.paramStoreTable = raw.get("paramStoreTable");
        if (raw.containsKey("taskBqDataset"))   opts.taskBqDataset   = raw.get("taskBqDataset");
        if (raw.containsKey("taskTable"))        opts.taskTable       = raw.get("taskTable");
        opts.taskBqProject      = raw.getOrDefault("taskBqProject", opts.paramBqProject);
        opts.manifestGcsBucket  = raw.get("manifestGcsBucket");
        opts.manifestGcsPath    = raw.get("manifestGcsPath");

        return opts;
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public String    getParentId()          { return parentId; }
    public LocalDate getRunDate()           { return runDate; }
    public String    getFrequency()         { return frequency; }
    public String    getRunId()             { return runId; }
    public String    getParamBqProject()    { return paramBqProject; }
    public String    getParamBqDataset()    { return paramBqDataset; }
    public String    getParamStoreTable()   { return paramStoreTable; }
    public String    getTaskBqProject()     { return taskBqProject; }
    public String    getTaskBqDataset()     { return taskBqDataset; }
    public String    getTaskTable()         { return taskTable; }
    public String    getManifestGcsBucket() { return manifestGcsBucket; }
    public String    getManifestGcsPath()   { return manifestGcsPath; }

    public boolean isManifestEnabled() {
        return manifestGcsBucket != null && !manifestGcsBucket.isBlank();
    }

    /** Fully-qualified BQ reference for the parameter store table. */
    public String paramStoreFqn() {
        return "`" + paramBqProject + "." + paramBqDataset + "." + paramStoreTable + "`";
    }

    // ── Parsing helpers ───────────────────────────────────────────────────────

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) continue;
            String stripped = arg.substring(2);
            int eq = stripped.indexOf('=');
            if (eq < 0) {
                map.put(stripped, "true");
            } else {
                map.put(stripped.substring(0, eq), stripped.substring(eq + 1));
            }
        }
        return map;
    }

    private static String require(Map<String, String> raw, String key) {
        String value = raw.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                "--" + key + " is required. Run with --help for usage.");
        }
        return value;
    }

    private static LocalDate parseDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                "--runDate must be in yyyy-MM-dd format. Got: '" + value + "'");
        }
    }
}
