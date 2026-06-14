package com.yourco.beam.transforms.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.model.LookupConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Enriches each main data row with fields from a lookup table (left join semantics).
 *
 * <h2>Side input format</h2>
 * The lookup data is passed as a {@code PCollectionView<Map<String, String>>} where:
 * <ul>
 *   <li>Key — the value of {@link LookupConfig#lookupKeyField} for each lookup row</li>
 *   <li>Value — a JSON string representing all fields of that lookup row</li>
 * </ul>
 * Using JSON strings avoids Beam coder complexity and works correctly with
 * {@code View.asMap()} for any lookup table schema.
 *
 * <h2>Responsibility split</h2>
 * This transform is purely a Beam graph node. Loading the lookup data (from BQ or JDBC),
 * serialising it to JSON, and building the {@code PCollectionView} is the responsibility
 * of {@link com.yourco.beam.runner.SourceTransformChainAssembler} in beam-runner.
 *
 * <h2>Join semantics</h2>
 * Left join: rows without a matching key pass through unchanged.
 * Name collisions: lookup fields that collide with existing main-row fields are
 * prefixed with {@code "lookup_"}.
 */
public final class LookupEnrichTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

    private final LookupConfig config;
    /** Map of lookupKeyField value → JSON string of that lookup row. */
    private final PCollectionView<Map<String, String>> lookupView;
    private final String label;

    /**
     * @param config     join key fields and which lookup fields to merge into the main row
     * @param lookupView side input built by the assembler: lookupKey → JSON(lookup row)
     * @param sourceLabel label for naming the Beam step in the Dataflow UI
     */
    public LookupEnrichTransform(LookupConfig config,
                                  PCollectionView<Map<String, String>> lookupView,
                                  String sourceLabel) {
        this.config     = config;
        this.lookupView = lookupView;
        this.label      = sourceLabel;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply("LookupEnrich-" + label,
            ParDo.of(new EnrichDoFn(config, lookupView))
                 .withSideInputs(lookupView));
    }

    // ── Enrich DoFn ───────────────────────────────────────────────────────────

    public static final class EnrichDoFn extends DoFn<Row, Row> {

        private static final Logger LOG = LoggerFactory.getLogger(EnrichDoFn.class);
        private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

        private final LookupConfig config;
        private final PCollectionView<Map<String, String>> lookupView;
        private transient ObjectMapper objectMapper;

        public EnrichDoFn(LookupConfig config, PCollectionView<Map<String, String>> lookupView) {
            this.config     = config;
            this.lookupView = lookupView;
        }

        @Setup
        public void setup() {
            objectMapper = new ObjectMapper();
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            Row mainRow = ctx.element();
            Map<String, String> lookupMap = ctx.sideInput(lookupView);

            Object keyVal = mainRow.getValue(config.dataKeyField);
            String lookupKey = keyVal != null ? keyVal.toString() : null;

            String lookupJson = (lookupKey != null) ? lookupMap.get(lookupKey) : null;

            if (lookupJson == null) {
                // No match — pass main row through unchanged (left join)
                ctx.output(mainRow);
                return;
            }

            Map<String, Object> lookupRow;
            try {
                lookupRow = objectMapper.readValue(lookupJson, MAP_TYPE);
            } catch (Exception e) {
                LOG.warn("Could not parse lookup row JSON for key {}: {}", lookupKey, e.getMessage());
                ctx.output(mainRow);
                return;
            }

            // Determine which fields to merge
            List<String> fieldsToAdd = config.mergeAllFields()
                ? new ArrayList<>(lookupRow.keySet())
                : config.outputFields;

            // Build merged schema and values
            Schema mainSchema = mainRow.getSchema();
            List<Schema.Field> mergedFields = new ArrayList<>(mainSchema.getFields());
            List<Object> mergedValues = new ArrayList<>();

            for (Schema.Field f : mainSchema.getFields()) {
                mergedValues.add(mainRow.getValue(f.getName()));
            }

            for (String lookupField : fieldsToAdd) {
                if (!lookupRow.containsKey(lookupField)) continue;
                String outputName = mainSchema.hasField(lookupField)
                    ? "lookup_" + lookupField
                    : lookupField;
                mergedFields.add(Schema.Field.nullable(outputName, Schema.FieldType.STRING));
                Object val = lookupRow.get(lookupField);
                mergedValues.add(val != null ? val.toString() : null);
            }

            Schema mergedSchema = Schema.builder().addFields(mergedFields).build();
            ctx.output(Row.withSchema(mergedSchema).addValues(mergedValues).build());
        }
    }
}
