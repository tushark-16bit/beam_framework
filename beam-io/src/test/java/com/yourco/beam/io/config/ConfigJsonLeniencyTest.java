package com.yourco.beam.io.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Config JSON pasted with multi-line SQL (raw line breaks inside a string) must still load. */
class ConfigJsonLeniencyTest {

    private static final String MULTILINE_QUERY =
        "{\"query_template\": \"SELECT a,\n       b\n  FROM t\tWHERE x = '{periodLabel}'\"}";

    @Test
    void strictJsonRejectsARawLineBreakInAString() {
        assertThrows(JsonProcessingException.class, () -> new ObjectMapper().readTree(MULTILINE_QUERY));
    }

    @Test
    void theConfigMapperAcceptsRawLineBreaksAndTabsAndKeepsThem() throws Exception {
        JsonNode node = BigQuerySourceConfigRepository.CONFIG_JSON.readTree(MULTILINE_QUERY);
        assertEquals("SELECT a,\n       b\n  FROM t\tWHERE x = '{periodLabel}'", node.get("query_template").asText());
    }

    @Test
    void dateParamsStoredWithTheJsonStayIntactAlongsideAMultilineQuery() throws Exception {
        JsonNode root = BigQuerySourceConfigRepository.CONFIG_JSON.readTree(
            "{\"q\": \"SELECT 1\n\", \"date_params\": [{\"token\": \"{periodLabel}\", \"source\": \"PERIOD_END\", \"format\": \"MMM-yy\"}]}");
        assertEquals(1, BigQuerySourceConfigRepository.parseDateParams(root.path("date_params")).size());
    }
}
