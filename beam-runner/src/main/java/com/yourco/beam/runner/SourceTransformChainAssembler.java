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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
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

import java.util.HashMap;
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
 * The LOOKUP step requires {@code BigQueryIO.readTableRows()}, which is only available
 * in {@code beam-runner} (via beam-io). Keeping this class in beam-runner avoids
 * pulling that dependency into beam-transforms.
 *
 * <h2>Lookup data loading strategy</h2>
 * Lookup rows are read via {@code BigQueryIO.readTableRows()} as part of the Beam graph,
 * keyed by {@link LookupConfig#lookupKeyField}, and converted to a
 * {@code PCollectionView<Map<String,String>>} side input.
 * The map value is a JSON string of the full lookup row so
 * {@link LookupEnrichTransform} doesn't need to know the lookup schema up front.
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
     * @param options  pipeline options (passed through for transforms that need them)
     * @param pipeline the root pipeline (needed to add the BQ lookup read branch)
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
        PCollectionView<Map<String, String>> lookupView = buildBqLookupView(lookupConfig, label, pipeline);
        return data.apply("LookupEnrich-" + label,
            new LookupEnrichTransform(lookupConfig, lookupView, label));
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
