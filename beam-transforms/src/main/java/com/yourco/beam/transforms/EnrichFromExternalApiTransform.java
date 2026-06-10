package com.yourco.beam.transforms;

import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.transform.BeamTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Sample transform that demonstrates the Beam {@code @Setup} / {@code @Teardown}
 * lifecycle pattern for managing non-serializable resources (HTTP clients, DB
 * connections, thread pools, etc.) inside a {@code DoFn}.
 *
 * <p>Token: {@code enrich-from-api}
 *
 * <h2>The core problem this solves</h2>
 * Beam serializes every {@code DoFn} instance and ships it to each worker.
 * Resources like {@link HttpClient}, JDBC connections, or gRPC channels are NOT
 * serializable — they cannot be fields in a {@code DoFn}.
 *
 * The solution is the {@code @Setup} / {@code @Teardown} lifecycle:
 * <ol>
 *   <li>Declare the resource field as {@code transient} — it is excluded from
 *       serialization.</li>
 *   <li>Annotate a method with {@code @Setup} — Beam calls it once per worker
 *       <em>after</em> the DoFn is deserialized and before the first element is
 *       processed. Create the resource here.</li>
 *   <li>Annotate a method with {@code @Teardown} — Beam calls it when the worker
 *       is shutting down. Clean up the resource here.</li>
 * </ol>
 *
 * <h2>What IS serialized (must be Serializable)</h2>
 * <ul>
 *   <li>{@code apiEndpoint} — a plain {@code String}, serializable</li>
 *   <li>{@code lookupFieldName} — a plain {@code String}, serializable</li>
 *   <li>{@code timeoutSeconds} — a primitive {@code int}, serializable</li>
 * </ul>
 *
 * <h2>What is NOT serialized (marked transient)</h2>
 * <ul>
 *   <li>{@code httpClient} — a {@link HttpClient}, NOT serializable.
 *       Created in {@code @Setup}, nulled in {@code @Teardown}.</li>
 * </ul>
 *
 * <h2>To adapt this for a real use case</h2>
 * <ol>
 *   <li>Replace {@link HttpClient} with your actual client (JDBC, gRPC, Redis, etc.).</li>
 *   <li>Replace {@code callApi()} with your actual enrichment logic.</li>
 *   <li>Replace {@code mergeEnrichment()} with your field-mapping logic.</li>
 *   <li>Register in {@code META-INF/services} and add your CLI options.</li>
 * </ol>
 *
 * <h2>Thread safety</h2>
 * Each Beam worker creates one DoFn instance per thread. {@code @Setup} is called
 * per-instance, so each thread gets its own client — no shared state, no locking needed.
 */
public final class EnrichFromExternalApiTransform implements BeamTransform {

    private static final long serialVersionUID = 1L;

    @Override
    public String name() {
        return "enrich-from-api";
    }

    @Override
    public PTransform<PCollection<Row>, PCollectionTuple> toComposite(FrameworkOptions options) {
        // Read config from options — these are serializable Strings/ints,
        // safe to pass to the composite and then to the DoFn.
        //
        // To add your own flags: declare them in FrameworkOptions, then read here.
        // Example: options.as(EnrichApiOptions.class).getApiEndpoint()
        String endpoint      = "https://api.example.com/enrich";  // replace with options flag
        String lookupField   = "customer_id";                      // replace with options flag
        int    timeoutSecs   = 5;                                  // replace with options flag

        return new EnrichComposite(endpoint, lookupField, timeoutSecs);
    }

    // =========================================================================
    // COMPOSITE — groups related steps under one labelled node in Dataflow UI
    // =========================================================================

    public static final class EnrichComposite
            extends PTransform<PCollection<Row>, PCollectionTuple> {

        // These fields ARE serialized — they are plain serializable types
        private final String endpoint;
        private final String lookupFieldName;
        private final int    timeoutSeconds;

        public EnrichComposite(String endpoint, String lookupFieldName, int timeoutSeconds) {
            this.endpoint        = endpoint;
            this.lookupFieldName = lookupFieldName;
            this.timeoutSeconds  = timeoutSeconds;
        }

        @Override
        public PCollectionTuple expand(PCollection<Row> input) {
            // Build the output schema by adding the enrichment fields to the input schema.
            // In practice, derive this from your API response structure.
            Schema outputSchema = buildOutputSchema(input.getSchema());

            PCollectionTuple result = input.apply("CallExternalApi",
                    ParDo.of(new EnrichDoFn(endpoint, lookupFieldName, timeoutSeconds, outputSchema))
                         .withOutputTags(SUCCESS_TAG, TupleTagList.of(DEAD_LETTER_TAG)));

            result.get(SUCCESS_TAG).setRowSchema(outputSchema);
            return result;
        }

        /**
         * Extends the input schema with the fields added by the API response.
         * Replace the hardcoded fields below with your actual API response fields.
         */
        private static Schema buildOutputSchema(Schema inputSchema) {
            Schema.Builder builder = Schema.builder();
            // Copy all existing fields from the input
            inputSchema.getFields().forEach(builder::addField);
            // Add new fields from the enrichment API (replace with actual fields)
            builder.addNullableStringField("customer_tier");      // example enrichment field
            builder.addNullableStringField("customer_region");    // example enrichment field
            builder.addNullableStringField("risk_score");         // example enrichment field
            return builder.build();
        }
    }

    // =========================================================================
    // DoFn — the actual per-element processing with lifecycle management
    // =========================================================================

    public static final class EnrichDoFn extends DoFn<Row, Row> {

        private static final Logger LOG = LoggerFactory.getLogger(EnrichDoFn.class);

        // ── Fields that ARE serialized ────────────────────────────────────────
        // These must be Serializable. They configure the DoFn after deserialization.

        private final String  apiEndpoint;      // URL of the enrichment service
        private final String  lookupFieldName;  // which Row field to use as the lookup key
        private final int     timeoutSeconds;   // HTTP timeout
        private final Schema  outputSchema;     // pre-built output schema

        // ── Fields that are NOT serialized ────────────────────────────────────
        // 'transient' tells Java (and Beam) to skip these during serialization.
        // They will be null after deserialization — @Setup must recreate them.

        private transient HttpClient httpClient;     // NOT serializable — recreated in @Setup

        public EnrichDoFn(String apiEndpoint, String lookupFieldName,
                          int timeoutSeconds, Schema outputSchema) {
            this.apiEndpoint     = apiEndpoint;
            this.lookupFieldName = lookupFieldName;
            this.timeoutSeconds  = timeoutSeconds;
            this.outputSchema    = outputSchema;
        }

        // ── Lifecycle: @Setup ─────────────────────────────────────────────────
        /**
         * Called ONCE per worker thread after deserialization, before the first element.
         * Create expensive resources here: HTTP clients, DB connections, gRPC channels.
         *
         * <p>Beam guarantees this runs before any {@code @ProcessElement} call on this instance.
         */
        @Setup
        public void setup() {
            LOG.info("Setting up HTTP client for API enrichment: {}", apiEndpoint);
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                    .build();
            // For DB connections: this.connection = DriverManager.getConnection(jdbcUrl);
            // For gRPC:           this.stub       = MyServiceGrpc.newBlockingStub(channel);
            // For Redis:          this.jedis      = new Jedis(redisHost, redisPort);
        }

        // ── Element processing ────────────────────────────────────────────────
        /**
         * Called for every element. {@code httpClient} is guaranteed non-null here
         * because {@code @Setup} has already run.
         */
        @ProcessElement
        public void processElement(@Element Row row, MultiOutputReceiver out) {
            Object lookupKey = row.getValue(lookupFieldName);
            if (lookupKey == null) {
                // Can't enrich without a key — route to DLQ
                out.get(DEAD_LETTER_TAG).output(FailedRecord.of(row,
                        new IllegalArgumentException(
                            "Lookup field '" + lookupFieldName + "' is null — cannot enrich"),
                        0));
                return;
            }

            try {
                // Call the external API with the lookup key
                String enrichmentJson = callApi(lookupKey.toString());

                // Merge the API response fields into a new Row with the output schema
                Row enriched = mergeEnrichment(row, enrichmentJson, outputSchema);
                out.get(SUCCESS_TAG).output(enriched);

            } catch (Exception e) {
                // API call failed — route to DLQ for inspection and possible replay
                LOG.warn("API enrichment failed for key '{}': {}", lookupKey, e.getMessage());
                out.get(DEAD_LETTER_TAG).output(FailedRecord.of(row, e, 1));
            }
        }

        // ── Lifecycle: @Teardown ──────────────────────────────────────────────
        /**
         * Called ONCE per worker thread when the worker is shutting down.
         * Release resources here: close connections, flush buffers, shut down thread pools.
         *
         * <p>Not guaranteed to run if the worker crashes — design for that possibility.
         */
        @Teardown
        public void teardown() {
            LOG.info("Tearing down HTTP client for API enrichment");
            httpClient = null;
            // For DB:    if (connection != null) connection.close();
            // For gRPC:  if (channel != null) channel.shutdown();
            // For Redis: if (jedis != null) jedis.close();
        }

        // ── Private helpers ───────────────────────────────────────────────────

        /**
         * Calls the enrichment API with the given lookup key.
         * Replace this with your actual API call logic.
         *
         * @param lookupKey the value to look up (e.g., customer_id)
         * @return raw JSON response string from the API
         * @throws IOException          if the HTTP request fails
         * @throws InterruptedException if the thread is interrupted while waiting
         */
        private String callApi(String lookupKey) throws IOException, InterruptedException {
            // Example: GET https://api.example.com/enrich?id=12345
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiEndpoint + "?id=" + lookupKey))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IOException(
                    "API returned HTTP " + response.statusCode()
                    + " for key=" + lookupKey);
            }
            return response.body();
        }

        /**
         * Merges the original Row's fields with fields parsed from the API response
         * into a new Row conforming to {@code outputSchema}.
         *
         * <p>This implementation uses a placeholder — replace with real JSON parsing
         * (e.g., Jackson's {@code ObjectMapper}) once your API contract is known.
         *
         * @param original       the original input row
         * @param enrichmentJson raw JSON string returned by the API
         * @param schema         the target output schema
         * @return a new Row with both original and enriched fields
         */
        private static Row mergeEnrichment(Row original, String enrichmentJson, Schema schema) {
            Row.Builder builder = Row.withSchema(schema);

            // Copy all original fields
            original.getSchema().getFields()
                    .forEach(f -> builder.addValue(original.getValue(f.getName())));

            // TODO: Parse enrichmentJson (e.g., with Jackson ObjectMapper) and
            //       add the API response fields. Currently hardcoded as placeholders.
            //
            // Example with Jackson:
            //   JsonNode node = objectMapper.readTree(enrichmentJson);
            //   builder.addValue(node.path("tier").asText(null));
            //   builder.addValue(node.path("region").asText(null));
            //   builder.addValue(node.path("riskScore").asText(null));

            builder.addValue((String) null);   // customer_tier   — replace with parsed value
            builder.addValue((String) null);   // customer_region — replace with parsed value
            builder.addValue((String) null);   // risk_score      — replace with parsed value

            return builder.build();
        }
    }
}
