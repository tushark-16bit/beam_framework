package com.yourco.beam.orchestrator;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.yourco.beam.orchestrator.manifest.GcsManifestWriter;
import com.yourco.beam.orchestrator.manifest.ManifestWriter;
import com.yourco.beam.orchestrator.period.PeriodResolver;
import com.yourco.beam.orchestrator.period.StandardPeriodResolver;
import com.yourco.beam.orchestrator.schedule.BigQueryRunScheduleResolver;
import com.yourco.beam.orchestrator.schedule.RunScheduleResolver;
import com.yourco.beam.orchestrator.task.BigQueryTaskRepository;
import com.yourco.beam.orchestrator.task.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the orchestrator JAR.
 *
 * <h2>What this class does</h2>
 * Wires the concrete implementations to the {@link Orchestrator} and runs it.
 * All extension points are instantiated here — swap an implementation by changing
 * this class only; the {@link Orchestrator} itself never changes.
 *
 * <h2>Run command</h2>
 * <pre>{@code
 * java -jar beam-orchestrator-bundled.jar \
 *   --parentId=TRADING \
 *   --runDate=2024-01-31 \
 *   --frequency=MONTHLY \
 *   --paramBqProject=my-gcp-project \
 *   --paramBqDataset=dw \
 *   --taskBqDataset=pipeline_orchestration \
 *   --manifestGcsBucket=my-bucket
 * }</pre>
 *
 * <h2>Swapping implementations</h2>
 * <ul>
 *   <li>Different period logic → replace {@link StandardPeriodResolver} with your own
 *       {@link PeriodResolver} impl</li>
 *   <li>Different schedule source → replace {@link BigQueryRunScheduleResolver} with your own
 *       {@link RunScheduleResolver} impl</li>
 *   <li>Different task store → replace {@link BigQueryTaskRepository} with your own
 *       {@link TaskRepository} impl</li>
 *   <li>Different manifest output → replace {@link GcsManifestWriter} with your own
 *       {@link ManifestWriter} impl (or pass {@code null} to skip)</li>
 * </ul>
 */
public final class OrchestratorMain {

    private static final Logger LOG = LoggerFactory.getLogger(OrchestratorMain.class);

    public static void main(String[] args) {
        OrchestratorOptions options = OrchestratorOptions.parse(args);

        LOG.info("Orchestrator | runId={} | parentId={} | runDate={} | frequency={}",
                 options.getRunId(), options.getParentId(),
                 options.getRunDate(), options.getFrequency());

        // ── GCP clients ───────────────────────────────────────────────────────
        BigQuery bigquery = BigQueryOptions.newBuilder()
            .setProjectId(options.getParamBqProject())
            .build()
            .getService();

        // ── Wire implementations ──────────────────────────────────────────────

        PeriodResolver periodResolver = new StandardPeriodResolver();
        // → To use business-day calendars or fiscal periods, replace with your impl:
        // PeriodResolver periodResolver = new FiscalYearPeriodResolver(calendarService);

        RunScheduleResolver scheduleResolver = new BigQueryRunScheduleResolver(
            bigquery, options.paramStoreFqn());
        // → To load schedule from a YAML / REST endpoint, replace here.

        TaskRepository taskRepository = new BigQueryTaskRepository(
            bigquery,
            options.getTaskBqProject(),
            options.getTaskBqDataset(),
            options.getTaskTable());
        // → To write tasks to Pub/Sub, Cloud SQL, etc., replace here.

        ManifestWriter manifestWriter = null;
        if (options.isManifestEnabled()) {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            manifestWriter = options.getManifestGcsPath() != null
                ? new GcsManifestWriter(storage, options.getManifestGcsBucket(),
                                        options.getManifestGcsPath())
                : new GcsManifestWriter(storage, options.getManifestGcsBucket());
        }
        // → To write the manifest to S3 or push to a webhook, implement ManifestWriter
        //   and assign it here. Pass null to disable manifest writing entirely.

        // ── Run ───────────────────────────────────────────────────────────────

        Orchestrator orchestrator = new Orchestrator(
            periodResolver, scheduleResolver, taskRepository, manifestWriter);

        String manifestLocation = orchestrator.execute(
            options.getRunId(),
            options.getParentId(),
            options.getRunDate(),
            options.getFrequency());

        if (manifestLocation != null) {
            LOG.info("Manifest available at: {}", manifestLocation);
        }

        LOG.info("Orchestrator run complete: {}", options.getRunId());
    }
}
