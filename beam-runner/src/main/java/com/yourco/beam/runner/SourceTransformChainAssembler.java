package com.yourco.beam.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.yourco.beam.model.LookupConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.model.SourceTransformConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.transforms.source.GroupByTransform;
import com.yourco.beam.transforms.source.LookupEnrichTransform;
import com.yourco.beam.transforms.source.SortByTransform;
import com.yourco.beam.utils.db.DatabaseAdapter;
import com.yourco.beam.utils.db.DatabaseAdapterFactory;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles the per-source transformation chain from {@link SourceTransformConfig} list.
 *
 * <h2>How it works</h2>
 * For each {@link SourceTransformConfig} in {@link SourceConfig#sourceTransforms}, this
 * assembler instantiates the corresponding Beam transform and applies it in order.
 * The output of each step is the input to the next.
 *
 * <h2>Why this is in beam-runner, not beam-transforms</h2>
 * The LOOKUP step requires loading lookup data from BigQuery (via {@code BigQueryIO.read()})
 * or from the parameter JDBC database. Both of these are available only in {@code beam-runner}
 * (BigQueryIO via beam-io, JDBC via beam-utils). Keeping this class in beam-runner avoids
 * introducing those dependencies into beam-transforms.
 *
 * <h2>Lookup data loading strategy</h2>
 * <ul>
 *   <li>{@code JDBC} lookup — the driver JVM fetches all lookup rows from the parameter DB
 *       before the pipeline runs, wraps them in a {@code PCollectionView<Map<String,String>>}
 *       via {@code Create.of()}, and passes them as a Beam side input.</li>
 *   <li>{@code BQ} lookup — lookup rows are read via {@code BigQueryIO.readTableRows()}
 *       as part of the Beam graph, keyed, and converted to the same
 *       {@code PCollectionView<Map<String,String>>} format.</li>
 * </ul>
 *
 * <p>The map value is always a JSON string of the full lookup row, so the
 * {@link LookupEnrichTransform} doesn't need to know the lookup schema up front.
 *
 * <h2>On-demand parameter access pattern</h2>
 * For JDBC lookup, a fresh {@link DatabaseAdapter} is created, queried, and closed
 * within this method — exactly the "create connection → fetch → close" pattern the
 * framework enforces for all parameter access. The JDBC credentials come from
 * {@code FrameworkOptions} (param DB options).
 */
public final class SourceTransformChainAssembler {

    private static final Logger LOG = LoggerFactory.getLogger(SourceTransformChainAssembler.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private SourceTransformChainAssembler() {}

    /**
     * Applies the full transform chain defined in {@link SourceConfig#sourceTransforms}
     * to {@code data} and returns the final transformed {@code PCollection<Row>}.
     *
     * @param data     the raw source data, freshly fetched from the source
     * @param config   the source configuration carrying the transform chain
     * @param options  pipeline options (used for param DB access for JDBC lookups)
     * @param pipeline the root pipeline (needed to add lookup source branches for BQ lookups)
     * @return the data after all transforms have been applied in order
     */
    public static PCollection<Row> assemble(PCollection<Row> data, SourceConfig config,
                                            FrameworkOptions options, Pipeline pipeline) {
        if (config.sourceTransforms.isEmpty()) {
            LOG.debug("No transforms configured for source: {}", config.datasourceName);
            return data;
        }

        LOG.info("Applying {} transform(s) for source: {}",
                 config.sourceTransforms.size(), config.datasourceName);

        PCollection<Row> current = data;
        String label = config.datasourceName;

        for (SourceTransformConfig transformConfig : config.sourceTransforms) {
            LOG.info("Applying {} transform to source {}", transformConfig.transformType, label);

            current = switch (transformConfig.transformType.toUpperCase()) {
                case SourceTransformConfig.GROUP_BY -> current.apply(
                    "GroupBy-" + label, new GroupByTransform(transformConfig, label));

                case SourceTransformConfig.SORT_BY -> current.apply(
                    "SortBy-" + label, new SortByTransform(transformConfig, label));

                case SourceTransformConfig.LOOKUP -> applyLookup(
                    current, transformConfig.lookupConfig, label, options, pipeline);

                default -> {
                    LOG.warn("Unknown transform type '{}' for source {} — skipping",
                             transformConfig.transformType, label);
                    yield current;
                }
            };
        }

        return current;
    }

    // ── Lookup loading and application ────────────────────────────────────────

    private static PCollection<Row> applyLookup(PCollection<Row> data, LookupConfig lookupConfig,
                                                 String label, FrameworkOptions options,
                                                 Pipeline pipeline) {
        PCollectionView<Map<String, String>> lookupView;

        if (lookupConfig.isJdbcSource()) {
            lookupView = buildJdbcLookupView(lookupConfig, label, options, pipeline);
        } else {
            lookupView = buildBqLookupView(lookupConfig, label, pipeline);
        }

        return data.apply("LookupEnrich-" + label,
            new LookupEnrichTransform(lookupConfig, lookupView, label));
    }

    /**
     * Loads lookup data from the parameter JDBC DB in the driver JVM (before pipeline.run()).
     *
     * <p>Pattern: create connection → query → close. The data is small enough to fit in
     * memory (lookup tables are reference data, typically < 100k rows).
     */
    private static PCollectionView<Map<String, String>> buildJdbcLookupView(
            LookupConfig lookupConfig, String label,
            FrameworkOptions options, Pipeline pipeline) {

        LOG.info("Loading JDBC lookup table for source '{}' — query: {}", label, lookupConfig.jdbcQuery);

        List<KV<String, String>> kvList = new ArrayList<>();
        try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
            List<Map<String, Object>> rows = db.query(lookupConfig.jdbcQuery);
            LOG.info("Loaded {} lookup rows for source '{}'", rows.size(), label);

            for (Map<String, Object> row : rows) {
                Object keyVal = row.get(lookupConfig.lookupKeyField);
                if (keyVal == null) continue;
                String key = keyVal.toString();
                // Convert all field values to String for serialization as JSON
                Map<String, String> strRow = new HashMap<>();
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    strRow.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
                }
                try {
                    kvList.add(KV.of(key, JSON.writeValueAsString(strRow)));
                } catch (Exception e) {
                    LOG.warn("Could not serialize lookup row for key {}: {}", key, e.getMessage());
                }
            }
        }

        return pipeline
            .apply("LookupData-" + label,
                   Create.of(kvList).withCoder(KvCoder.of(
                       StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("LookupView-" + label, View.asMap());
    }

    /**
     * Reads lookup data from BigQuery as part of the Beam graph.
     *
     * <p>The lookup table is read via {@code BigQueryIO.readTableRows()}, each row
     * is serialized to a JSON string, keyed by the lookup key field value, and
     * converted to a {@code PCollectionView<Map<String, String>>}.
     *
     * <p>This is suitable for BQ-hosted reference tables. The data is loaded during
     * pipeline execution (not in the driver JVM).
     */
    private static PCollectionView<Map<String, String>> buildBqLookupView(
            LookupConfig lookupConfig, String label, Pipeline pipeline) {

        LOG.info("Configuring BQ lookup for source '{}' from table: {}",
                 label, lookupConfig.bqTableRef);

        String lookupKey = lookupConfig.lookupKeyField;

        return pipeline
            .apply("LookupBqRead-" + label,
                   BigQueryIO.readTableRows().from(lookupConfig.bqTableRef.replace(":", ".")))
            .apply("LookupBqKey-" + label,
                   MapElements.into(TypeDescriptors.kvs(
                       TypeDescriptors.strings(), TypeDescriptors.strings()))
                       .<TableRow>via(tableRow -> {
                           Object keyVal = tableRow.get(lookupKey);
                           String key = keyVal != null ? keyVal.toString() : "";
                           try {
                               // Convert TableRow (Map<String,Object>) to JSON string
                               Map<String, String> strRow = new HashMap<>();
                               tableRow.forEach((k, v) ->
                                   strRow.put(k, v != null ? v.toString() : null));
                               return KV.of(key, JSON.writeValueAsString(strRow));
                           } catch (Exception e) {
                               return KV.of(key, "{}");
                           }
                       }))
            .apply("LookupBqView-" + label, View.asMap());
    }
}
