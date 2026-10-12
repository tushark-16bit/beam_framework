package com.yourco.beam.io.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.model.DateParamConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** date_params parsing, shared by source configs (flat JSON) and report configs (nested JSON). */
class DateParamsParsingTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static List<DateParamConfig> parse(String json) throws Exception {
        return BigQuerySourceConfigRepository.parseDateParams(JSON.readTree(json));
    }

    @Test
    void parsesAnArrayOfObjects() throws Exception {
        List<DateParamConfig> params = parse("[{\"token\":\"{label}\",\"source\":\"PERIOD_END\","
            + "\"format\":\"MMM-yy\",\"apply_to\":[\"step_a\",\"step_b\"]},"
            + "{\"token\":\"%PRD%\",\"source\":\"periodId\"}]");
        assertEquals(2, params.size());
        assertEquals("{label}", params.get(0).token);
        assertEquals(DateParamConfig.Source.PERIOD_END, params.get(0).source);
        assertEquals("MMM-yy", params.get(0).format);
        assertEquals(List.of("step_a", "step_b"), params.get(0).applyTo);
        assertEquals(DateParamConfig.Source.PERIOD_ID, params.get(1).source);
        assertNull(params.get(1).format);
        assertTrue(params.get(1).applyTo.isEmpty());
    }

    @Test
    void aStringContainingTheArrayIsAcceptedToo() throws Exception {
        List<DateParamConfig> params = parse(
            "\"[{\\\"token\\\":\\\"{a}\\\",\\\"source\\\":\\\"RUN_DATE\\\"}]\"");
        assertEquals(1, params.size());
        assertEquals(DateParamConfig.Source.RUN_DATE, params.get(0).source);
    }

    @Test
    void absentOrEmptyMeansNoParameters() throws Exception {
        assertTrue(BigQuerySourceConfigRepository.parseDateParams(null).isEmpty());
        assertTrue(parse("[]").isEmpty());
        assertTrue(parse("\"\"").isEmpty());
    }

    @Test
    void aSingleApplyToStringIsAccepted() throws Exception {
        assertEquals(List.of("bq_query"),
            parse("[{\"token\":\"{a}\",\"source\":\"PERIOD_START\",\"apply_to\":\"bq_query\"}]").get(0).applyTo);
    }

    @Test
    void malformedConfigurationFailsTheLoad() {
        assertThrows(IllegalArgumentException.class, () -> parse("{\"token\":\"{a}\"}"));          // not an array
        assertThrows(IllegalArgumentException.class, () -> parse("[\"x\"]"));                      // entry not an object
        assertThrows(IllegalArgumentException.class, () -> parse("[{\"source\":\"PERIOD_END\"}]"));  // no token
        assertThrows(IllegalArgumentException.class, () -> parse("[{\"token\":\"{a}\",\"source\":\"x\"}]"));
        assertThrows(IllegalArgumentException.class,
            () -> parse("[{\"token\":\"{a}\",\"source\":\"PERIOD_END\",\"format\":\"MMM-yy-bb\"}]"));
    }
}
