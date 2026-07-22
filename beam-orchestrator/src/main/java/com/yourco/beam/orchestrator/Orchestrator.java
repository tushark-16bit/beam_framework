package com.yourco.beam.orchestrator;

import com.yourco.beam.orchestrator.manifest.ManifestWriter;
import com.yourco.beam.orchestrator.model.ResolvedPeriod;
import com.yourco.beam.orchestrator.model.RunSpec;
import com.yourco.beam.orchestrator.model.TaskItem;
import com.yourco.beam.orchestrator.period.PeriodResolver;
import com.yourco.beam.orchestrator.schedule.RunScheduleResolver;
import com.yourco.beam.orchestrator.task.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Core orchestration logic — depends only on interfaces, never on concrete implementations.
 *
 * <h2>Execution steps</h2>
 * <ol>
 *   <li>Resolve the period from {@code runDate} + {@code frequency} via {@link PeriodResolver}</li>
 *   <li>Identify what to run via {@link RunScheduleResolver} → {@link List}&lt;{@link RunSpec}&gt;</li>
 *   <li>Build {@link TaskItem} objects (all status = PENDING)</li>
 *   <li>Persist tasks via {@link TaskRepository}</li>
 *   <li>Write manifest via {@link ManifestWriter} (optional — null writer skips this step)</li>
 * </ol>
 *
 * <p>This class is stateless — the same instance may be reused across invocations.
 * All state comes in via the {@code execute()} parameters.
 */
public final class Orchestrator {

    private static final Logger LOG = LoggerFactory.getLogger(Orchestrator.class);

    private final PeriodResolver      periodResolver;
    private final RunScheduleResolver scheduleResolver;
    private final TaskRepository      taskRepository;
    private final ManifestWriter      manifestWriter;   // null → skip

    public Orchestrator(PeriodResolver periodResolver,
                        RunScheduleResolver scheduleResolver,
                        TaskRepository taskRepository,
                        ManifestWriter manifestWriter) {
        this.periodResolver   = periodResolver;
        this.scheduleResolver = scheduleResolver;
        this.taskRepository   = taskRepository;
        this.manifestWriter   = manifestWriter;
    }

    /**
     * Runs one orchestration cycle.
     *
     * @param runId     unique identifier for this orchestrator invocation
     * @param parentId  business group to orchestrate
     * @param runDate   the date this run covers
     * @param frequency the schedule frequency (DAILY / MONTHLY / WEEKLY)
     * @return the manifest location, or null if no manifest writer is configured
     */
    public String execute(String runId, String parentId, LocalDate runDate, String frequency) {
        LOG.info("Orchestrator starting: runId={} parentId={} runDate={} frequency={}",
                 runId, parentId, runDate, frequency);

        // Step 1 — resolve period
        ResolvedPeriod period = periodResolver.resolve(runDate, frequency);
        LOG.info("Period resolved: {}", period);

        // Step 2 — resolve schedule
        List<RunSpec> specs = scheduleResolver.resolve(parentId, frequency, period);
        if (specs.isEmpty()) {
            LOG.warn("No runnable specs found for parentId={} frequency={}. "
                     + "Check parameter_store rows have run_type set.", parentId, frequency);
        }

        // Step 3 — build task items
        Instant now = Instant.now();
        List<TaskItem> tasks = new ArrayList<>(specs.size());
        for (RunSpec spec : specs) {
            tasks.add(new TaskItem(
                UUID.randomUUID().toString(),
                runId,
                spec,
                TaskItem.STATUS_PENDING,
                now,
                Collections.emptyMap()
            ));
        }
        LOG.info("Created {} task item(s)", tasks.size());

        // Step 4 — persist
        if (!tasks.isEmpty()) {
            taskRepository.save(tasks);
        }

        // Step 5 — write manifest (optional)
        if (manifestWriter != null && !tasks.isEmpty()) {
            String location = manifestWriter.write(
                runId, parentId, frequency, runDate.toString(), tasks);
            LOG.info("Manifest written to: {}", location);
            return location;
        }

        LOG.info("Orchestrator completed: runId={} tasks={}", runId, tasks.size());
        return null;
    }
}
