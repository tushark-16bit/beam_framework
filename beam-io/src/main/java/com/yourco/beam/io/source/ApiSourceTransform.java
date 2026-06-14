package com.yourco.beam.io.source;

import com.yourco.beam.model.ApiSourceConfig;
import com.yourco.beam.model.Schemas;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.utils.SecretManagerUtils;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;

/**
 * Beam source transform that fetches data from a REST API via {@link ApiSourceAdapter}.
 *
 * <h2>Design: thin Beam wrapper around a testable adapter</h2>
 * All HTTP logic lives in {@link ApiSourceAdapter}. This class only handles:
 * <ul>
 *   <li>The Beam lifecycle — {@code @Setup} / {@code @Teardown} for the {@link HttpClient}</li>
 *   <li>Translating each JSON string record into a Beam {@link Row}</li>
 * </ul>
 * This means {@link ApiSourceAdapter} can be unit-tested without a Beam pipeline.
 *
 * <h2>Execution flow</h2>
 * <pre>
 *   Create.of(sourceConfig)          → single-element trigger PCollection
 *   → ApiDoFn.@ProcessElement        → fetches all pages, emits one Row per record
 *   → .setRowSchema(Schemas.RAW_JSON) → downstream transforms see "raw_json STRING" schema
 * </pre>
 *
 * <h2>Why Create.of as a trigger?</h2>
 * Beam sources must start from a {@code PBegin}. There is no built-in "HTTP source"
 * connector, so we manufacture a single-element collection as the trigger and do all
 * the real work in the DoFn. This is the standard Beam pattern for custom batch sources.
 *
 * <h2>Serialization</h2>
 * {@link SourceConfig} and {@link ApiSourceConfig} are serializable and carried as
 * DoFn constructor fields. The {@link HttpClient} and auth token are {@code transient}
 * and recreated per worker in {@code @Setup}.
 */
public final class ApiSourceTransform extends PTransform<PBegin, PCollection<Row>> {

    private static final long serialVersionUID = 1L;

    private final SourceConfig sourceConfig;

    public ApiSourceTransform(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return input
            .apply("CreateApiTrigger-" + sourceConfig.datasourceName,
                   Create.of(sourceConfig.datasourceName))
            .apply("FetchApiPages-" + sourceConfig.datasourceName,
                   ParDo.of(new ApiDoFn(sourceConfig.apiConfig)))
            .setRowSchema(Schemas.RAW_JSON);
    }

    // ── DoFn — thin Beam wrapper around ApiSourceAdapter ────────────────────

    private static final class ApiDoFn extends DoFn<String, Row> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(ApiDoFn.class);

        // Serialized with the DoFn and shipped to workers — must be Serializable
        private final ApiSourceConfig apiConfig;

        // Created in @Setup, closed in @Teardown — must be transient
        private transient HttpClient httpClient;
        private transient String authToken;
        private transient ApiSourceAdapter adapter;

        ApiDoFn(ApiSourceConfig apiConfig) {
            this.apiConfig = apiConfig;
        }

        @Setup
        public void setup() {
            httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
            // Fetch auth credentials from Secret Manager once per worker
            authToken = (apiConfig.authSecretId != null && !apiConfig.authSecretId.isBlank())
                ? SecretManagerUtils.fetchSecret(apiConfig.authSecretId)
                : "";
            adapter = new ApiSourceAdapter(httpClient);
            LOG.info("ApiSourceAdapter initialised for endpoint: {}", apiConfig.endpoint);
        }

        @ProcessElement
        public void processElement(@Element String datasourceName, OutputReceiver<Row> out) {
            LOG.info("Fetching API data for datasource: {}", datasourceName);
            List<String> records = adapter.fetchAll(apiConfig, authToken);
            LOG.info("Fetched {} records from API for datasource: {}", records.size(), datasourceName);

            for (String jsonRecord : records) {
                out.output(Row.withSchema(Schemas.RAW_JSON).addValue(jsonRecord).build());
            }
        }

        @Teardown
        public void teardown() {
            httpClient = null;
            authToken  = null;
            adapter    = null;
        }
    }
}
