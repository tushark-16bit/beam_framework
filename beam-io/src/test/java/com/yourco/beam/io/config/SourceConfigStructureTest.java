package com.yourco.beam.io.config;

import com.yourco.beam.model.BncConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The data source config shape: {@code query_details} / {@code file_details} (one populated, by
 * source type), {@code bq_settings} ({@code fetch_options}, {@code storage_options}), {@code bnc} —
 * and the legacy flat keys still being read.
 */
class SourceConfigStructureTest {

    private static final BigQuerySourceConfigRepository REPO = new BigQuerySourceConfigRepository(
        null, PipelineOptionsFactory.create().as(FrameworkOptions.class));

    private static SourceConfig parse(String json) throws Exception {
        return REPO.mapToSourceConfig(BigQuerySourceConfigRepository.flattenParams(json),
                                      "P", "DS", "sub", 202608);
    }

    private static final String STRUCTURED_BQ = "{"
        + "\"source_type\":\"BQ\","
        + "\"query_details\":{\"table\":\"ogl_trial_balance\","
        + "  \"query\":\"SELECT * FROM t WHERE p = '{periodLabel}'\","
        + "  \"query_params\":{\"k\":\"v\"},"
        + "  \"schema\":[{\"name\":\"a\",\"type\":\"STRING\"},{\"name\":\"b\",\"type\":\"INT64\"}]},"
        + "\"file_details\":{},"
        + "\"bq_settings\":{\"fetch_options\":{\"project_id\":\"my-proj\",\"dataset\":\"dw\",\"location\":\"EU\"},"
        + "                 \"storage_options\":{\"page_size\":100}}"
        + "}";

    @Test
    void structuredBqConfigPopulatesQueryDetailsAndSettings() throws Exception {
        SourceConfig c = parse(STRUCTURED_BQ);
        assertEquals("my-proj", c.bqFetchConfig.projectId);
        assertEquals("dw", c.bqFetchConfig.dataset);
        assertEquals("ogl_trial_balance", c.bqFetchConfig.table);
        assertEquals("my-proj:dw.ogl_trial_balance", c.bqFetchConfig.tableRef());
        assertEquals("SELECT * FROM t WHERE p = '{periodLabel}'", c.bqFetchConfig.query);
        assertEquals(Map.of("k", "v"), c.bqFetchConfig.queryParams);
        assertEquals(2, c.bqFetchConfig.schema.size());
        assertEquals("EU", c.bqFetchConfig.location);
        assertEquals(100, c.bqSettings.storage.pageSize);
        assertEquals("EU", c.bqSettings.fetch.location);
        assertNull(c.fileConfig);
        assertEquals(c.bqFetchConfig.query, c.queryConfig.queryTemplate);
    }

    @Test
    void legacyFlatBqKeysStillWork() throws Exception {
        SourceConfig c = parse("{\"source_type\":\"BQ\",\"bq_project_id\":\"p\",\"bq_dataset\":\"d\","
            + "\"bq_table\":\"t\",\"bq_query\":\"SELECT 1\",\"query_params_json\":\"{\\\"a\\\":\\\"b\\\"}\","
            + "\"bq_schema_json\":\"[{\\\"name\\\":\\\"x\\\",\\\"type\\\":\\\"STRING\\\"}]\"}");
        assertEquals("p:d.t", c.bqFetchConfig.tableRef());
        assertEquals("SELECT 1", c.bqFetchConfig.query);
        assertEquals(Map.of("a", "b"), c.bqFetchConfig.queryParams);
        assertEquals(1, c.bqFetchConfig.schema.size());
        assertEquals(250, c.bqSettings.storage.pageSize);
        assertNull(c.bqFetchConfig.location);
    }

    @Test
    void theStructuredValueWinsOverTheLegacyKeyPerField() throws Exception {
        SourceConfig c = parse("{\"source_type\":\"BQ\",\"bq_query\":\"OLD\",\"bq_table\":\"old_t\","
            + "\"query_details\":{\"query\":\"NEW\"},"
            + "\"bq_settings\":{\"fetch_options\":{\"project_id\":\"p\",\"dataset\":\"d\"}}}");
        assertEquals("NEW", c.bqFetchConfig.query);
        assertEquals("old_t", c.bqFetchConfig.table);      // not in query_details → legacy fallback
    }

    @Test
    void aQualifiedTableIsUsedAsGiven() throws Exception {
        SourceConfig c = parse("{\"source_type\":\"BQ\",\"query_details\":{\"table\":\"other.ds2.t2\"},"
            + "\"bq_settings\":{\"fetch_options\":{\"project_id\":\"p\",\"dataset\":\"d\"}}}");
        assertEquals("other:ds2.t2", c.bqFetchConfig.tableRef());
    }

    @Test
    void structuredFileConfigPopulatesFileDetails() throws Exception {
        SourceConfig c = parse("{\"source_type\":\"FILE\",\"query_details\":{},"
            + "\"file_details\":{\"type\":\"excel\",\"location\":\"gs://b/in/\",\"prefix\":\"rev_\","
            + "\"suffix\":\".xlsx\",\"has_header\":true,\"sheet_index\":1,\"first_row\":3,"
            + "\"last_column\":\"t\",\"date_pattern\":\"yyyyMM\"}}");
        assertEquals("EXCEL", c.fileConfig.fileType);
        assertEquals("gs://b/in/", c.fileConfig.location);
        assertEquals("rev_", c.fileConfig.prefix);
        assertTrue(c.fileConfig.hasHeader);
        assertEquals(1, c.fileConfig.sheetIndex);
        assertEquals(3, c.fileConfig.firstRow);
        assertEquals("T", c.fileConfig.lastColumn);
        assertEquals("yyyyMM", c.fileConfig.fileDatePattern);
        assertNull(c.bqFetchConfig);
    }

    @Test
    void legacyFlatFileKeysStillWork() throws Exception {
        SourceConfig c = parse("{\"source_type\":\"FILE\",\"file_type\":\"CSV\",\"file_location\":\"gs://b/\","
            + "\"file_has_header\":\"true\",\"file_delimiter\":\";\"}");
        assertEquals("CSV", c.fileConfig.fileType);
        assertEquals(";", c.fileConfig.delimiter);
        assertTrue(c.fileConfig.hasHeader);
    }

    @Test
    void theOtherSourcesDetailsBlockMustStayEmpty() {
        assertThrows(IllegalStateException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"query_details\":{\"query\":\"q\"},\"file_details\":{\"type\":\"CSV\"}}"));
        assertThrows(IllegalStateException.class, () -> parse(
            "{\"source_type\":\"FILE\",\"file_details\":{\"type\":\"CSV\"},\"query_details\":{\"query\":\"q\"}}"));
        assertThrows(IllegalStateException.class, () -> parse(
            "{\"source_type\":\"API\",\"query_details\":{\"query\":\"q\"}}"));
    }

    @Test
    void aDetailsBlockThatIsNotAnObjectFailsTheLoad() {
        assertThrows(IllegalArgumentException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"query_details\":\"[1,2]\"}"));
    }

    // ── bq_settings ───────────────────────────────────────────────────────────

    @Test
    void aBadPageSizeFailsTheLoad() {
        assertThrows(IllegalArgumentException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"bq_settings\":{\"storage_options\":{\"page_size\":0}}}"));
        assertThrows(IllegalArgumentException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"bq_settings\":{\"storage_options\":{\"page_size\":5000}}}"));
        assertThrows(IllegalArgumentException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"bq_settings\":{\"storage_options\":{\"page_size\":\"lots\"}}}"));
    }

    // ── bnc ───────────────────────────────────────────────────────────────────

    @Test
    void noBncObjectMeansNoCheck() throws Exception {
        SourceConfig c = parse("{\"source_type\":\"BQ\"}");
        assertFalse(c.bncConfig.isActive());
        assertEquals(BncConfig.Type.NONE, c.bncConfig.type);
    }

    @Test
    void aBncObjectIsParsed() throws Exception {
        SourceConfig c = parse("{\"source_type\":\"BQ\",\"bnc\":{\"type\":\"SOURCE_QUERY\","
            + "\"source_query\":\"SELECT 1, 2\",\"dest_column\":\"AMT\",\"variance_allowed\":1.5,"
            + "\"error_on_failure\":false}}");
        BncConfig b = c.bncConfig;
        assertEquals(BncConfig.Type.SOURCE_QUERY, b.type);
        assertEquals("SELECT 1, 2", b.sourceQuery);
        assertEquals("AMT", b.destColumn);
        assertEquals(new BigDecimal("1.5"), b.varianceAllowed);
        assertFalse(b.errorOnFailure);
    }

    @Test
    void bncDefaultsFollowTheLegacyRules() throws Exception {
        BncConfig b = parse("{\"source_type\":\"FILE\",\"bnc\":{\"type\":\"COUNT_ONLY\"}}").bncConfig;
        assertEquals(BigDecimal.ZERO, b.varianceAllowed);
        assertTrue(b.errorOnFailure);                       // blank = true (K1)
    }

    @Test
    void anInvalidBncFailsTheLoad() {
        assertThrows(IllegalArgumentException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"bnc\":{\"type\":\"SOURCE_QUERY\",\"variance_allowed\":\"abc\"}}"));
        assertThrows(IllegalArgumentException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"bnc\":{\"type\":\"MAGIC\"}}"));
        assertThrows(IllegalArgumentException.class, () -> parse(
            "{\"source_type\":\"BQ\",\"bnc\":{\"source_query\":\"x\"}}"));     // no type
    }

    // ── schema_of_json required-field lookup ──────────────────────────────────

    @Test
    void aLegacyRequiredKeyIsFoundWhereTheStructuredConfigKeepsIt() throws Exception {
        Map<String, String> p = BigQuerySourceConfigRepository.flattenParams(STRUCTURED_BQ);
        assertEquals("SELECT * FROM t WHERE p = '{periodLabel}'",
                     BigQuerySourceConfigRepository.lookupParam(p, "bq_query"));
        assertEquals("EU", BigQuerySourceConfigRepository.lookupParam(p, "bq_settings.fetch_options.location"));
        assertEquals("dw", BigQuerySourceConfigRepository.lookupParam(p, "bq_dataset"));
        assertNull(BigQuerySourceConfigRepository.lookupParam(p, "file_type"));
    }
}
