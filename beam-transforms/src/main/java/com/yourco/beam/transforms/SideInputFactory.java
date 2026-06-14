package com.yourco.beam.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory for creating Beam {@link PCollectionView} instances from driver-JVM data.
 *
 * <h2>What is a side input?</h2>
 * A side input is a small, read-only dataset that a {@link org.apache.beam.sdk.transforms.DoFn}
 * can access alongside each element it processes. Unlike pipeline data (which flows through
 * the DAG), a side input is materialised once and shared with all worker instances.
 *
 * <p>Common use cases in this framework:
 * <ul>
 *   <li>Passing the list of {@link com.yourco.beam.model.SourceConfig}s so a transform
 *       can look up metadata for each Row it receives.</li>
 *   <li>Providing a reference lookup table (e.g., instrument→currency mappings) fetched
 *       from BigQuery at pipeline-assembly time.</li>
 *   <li>Passing per-run configuration (e.g., report parameters) to every element.</li>
 * </ul>
 *
 * <h2>Usage pattern in a DoFn</h2>
 * <pre>{@code
 * // In PipelineFactory — create the view from a List fetched in driver JVM
 * PCollectionView<List<SourceConfig>> configView =
 *     SideInputFactory.asList(pipeline, "SourceConfigs", configs,
 *                             SerializableCoder.of(SourceConfig.class));
 *
 * // Wire the view into the transform that needs it
 * PCollection<Row> enriched = rows.apply("Enrich",
 *     ParDo.of(new MyEnrichFn(configView))
 *          .withSideInputs(configView));
 *
 * // In the DoFn — access the view in @ProcessElement
 * public void processElement(@Element Row row, ProcessContext c) {
 *     List<SourceConfig> configs = c.sideInput(configView);
 *     // ... use configs ...
 * }
 * }</pre>
 *
 * <h2>Performance note</h2>
 * Side inputs are materialised before the main transform begins. For large datasets
 * (> a few MB), prefer BQ-backed side inputs using {@code BigQueryIO.read()} rather
 * than {@code Create.of()} which bundles the data into the pipeline bundle.
 */
public final class SideInputFactory {

    private SideInputFactory() {}

    /**
     * Creates a {@code PCollectionView<T>} from a single serializable value.
     * All DoFn instances that use this view receive the same object.
     *
     * <pre>{@code
     * PCollectionView<RunConfig> configView =
     *     SideInputFactory.asSingleton(pipeline, "RunConfig", runConfig,
     *                                  SerializableCoder.of(RunConfig.class));
     * }</pre>
     */
    public static <T extends Serializable> PCollectionView<T> asSingleton(
            Pipeline pipeline, String name, T value, Coder<T> coder) {
        return pipeline
            .apply("SideInput-Create-" + name, Create.of(value).withCoder(coder))
            .apply("SideInput-Singleton-" + name, View.asSingleton());
    }

    /**
     * Creates a {@code PCollectionView<List<T>>} from a list of serializable values.
     * All DoFn instances see the complete list.
     *
     * <pre>{@code
     * PCollectionView<List<SourceConfig>> configsView =
     *     SideInputFactory.asList(pipeline, "SourceConfigs", configs,
     *                             SerializableCoder.of(SourceConfig.class));
     * }</pre>
     */
    public static <T> PCollectionView<List<T>> asList(
            Pipeline pipeline, String name, List<T> values, Coder<T> coder) {
        return pipeline
            .apply("SideInput-Create-" + name, Create.of(values).withCoder(coder))
            .apply("SideInput-List-" + name, View.asList());
    }

    /**
     * Creates a {@code PCollectionView<Map<K,V>>} from a Java {@link Map}.
     * DoFns use this as a lookup table: {@code map.get(someKey)}.
     *
     * <pre>{@code
     * Map<String, String> ccyMap = fetchCurrencyMap(); // from BQ or DB in driver JVM
     * PCollectionView<Map<String, String>> ccyView =
     *     SideInputFactory.asMap(pipeline, "CurrencyMap", ccyMap,
     *                            StringUtf8Coder.of(), StringUtf8Coder.of());
     * }</pre>
     */
    public static <K, V> PCollectionView<Map<K, V>> asMap(
            Pipeline pipeline, String name, Map<K, V> values, Coder<K> keyCoder, Coder<V> valueCoder) {
        List<KV<K, V>> kvList = values.entrySet().stream()
            .map(e -> KV.of(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

        return pipeline
            .apply("SideInput-Create-" + name,
                   Create.of(kvList).withCoder(org.apache.beam.sdk.coders.KvCoder.of(keyCoder, valueCoder)))
            .apply("SideInput-Map-" + name, View.asMap());
    }
}
