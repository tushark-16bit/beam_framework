package com.yourco.beam.orchestrator.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.yourco.beam.orchestrator.model.TaskItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

/**
 * Writes a JSON manifest to GCS so the triggering DAG can discover what tasks
 * were created and fan them out into individual pipeline JAR invocations.
 *
 * <h2>GCS path</h2>
 * Default pattern: {@code gs://{bucket}/manifests/{runId}/tasks.json}
 * Override by supplying a custom {@code pathTemplate} to the constructor.
 * Supported tokens in the template: {@code {runId}}, {@code {parentId}},
 * {@code {frequency}}, {@code {runDate}}.
 *
 * <h2>Manifest JSON structure</h2>
 * <pre>{@code
 * {
 *   "runId":       "TRADING-MONTHLY-2024-01-31-abc12345",
 *   "generatedAt": "2024-01-31T06:00:00Z",
 *   "parentId":    "TRADING",
 *   "frequency":   "MONTHLY",
 *   "runDate":     "2024-01-31",
 *   "period": {
 *     "periodId":    202401,
 *     "periodStart": "2024-01-01",
 *     "periodEnd":   "2024-01-31"
 *   },
 *   "tasks": [
 *     {
 *       "taskId":      "uuid",
 *       "runType":     "DATA_SOURCE_DOWNLOAD",
 *       "name":        "trades",
 *       "subprocess":  "eod",
 *       "periodId":    202401,
 *       "periodStart": "2024-01-01",
 *       "periodEnd":   "2024-01-31",
 *       "runDate":     "2024-01-31",
 *       "runOrder":    10,
 *       "status":      "PENDING",
 *       "extraParams": {}
 *     }
 *   ]
 * }
 * }</pre>
 */
public final class GcsManifestWriter implements ManifestWriter {

    private static final Logger LOG = LoggerFactory.getLogger(GcsManifestWriter.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Default GCS object path template. Tokens: {runId} {parentId} {frequency} {runDate} */
    public static final String DEFAULT_PATH_TEMPLATE = "manifests/{runId}/tasks.json";

    private final Storage storage;
    private final String  bucket;
    private final String  pathTemplate;

    public GcsManifestWriter(Storage storage, String bucket) {
        this(storage, bucket, DEFAULT_PATH_TEMPLATE);
    }

    public GcsManifestWriter(Storage storage, String bucket, String pathTemplate) {
        this.storage      = storage;
        this.bucket       = bucket;
        this.pathTemplate = pathTemplate;
    }

    @Override
    public String write(String runId, String parentId, String frequency,
                        String runDate, List<TaskItem> tasks) {
        String objectPath = pathTemplate
            .replace("{runId}",     runId)
            .replace("{parentId}", parentId)
            .replace("{frequency}", frequency)
            .replace("{runDate}",   runDate);

        byte[] content = buildManifest(runId, parentId, frequency, runDate, tasks);

        BlobId   blobId   = BlobId.of(bucket, objectPath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
            .setContentType("application/json")
            .build();

        storage.create(blobInfo, content);

        String uri = "gs://" + bucket + "/" + objectPath;
        LOG.info("Manifest written to {}", uri);
        return uri;
    }

    private byte[] buildManifest(String runId, String parentId, String frequency,
                                  String runDate, List<TaskItem> tasks) {
        try {
            ObjectNode root = JSON.createObjectNode();
            root.put("runId",       runId);
            root.put("generatedAt", Instant.now().toString());
            root.put("parentId",    parentId);
            root.put("frequency",   frequency);
            root.put("runDate",     runDate);

            if (!tasks.isEmpty()) {
                TaskItem first = tasks.get(0);
                ObjectNode period = root.putObject("period");
                period.put("periodId",    first.spec.period.periodId);
                period.put("periodStart", first.spec.period.periodStart.toString());
                period.put("periodEnd",   first.spec.period.periodEnd.toString());
            }

            ArrayNode taskArray = root.putArray("tasks");
            for (TaskItem task : tasks) {
                ObjectNode t = taskArray.addObject();
                t.put("taskId",      task.taskId);
                t.put("runType",     task.spec.runType);
                t.put("name",        task.spec.name);
                t.put("subprocess",  task.spec.subprocess);
                t.put("periodId",    task.spec.period.periodId);
                t.put("periodStart", task.spec.period.periodStart.toString());
                t.put("periodEnd",   task.spec.period.periodEnd.toString());
                t.put("runDate",     task.spec.period.runDate.toString());
                t.put("runOrder",    task.spec.runOrder);
                t.put("status",      task.status);
                t.set("extraParams", JSON.valueToTree(task.spec.extraParams));
            }

            return JSON.writerWithDefaultPrettyPrinter()
                       .writeValueAsBytes(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialise manifest JSON: " + e.getMessage(), e);
        }
    }
}
