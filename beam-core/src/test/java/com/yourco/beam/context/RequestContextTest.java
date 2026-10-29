package com.yourco.beam.context;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestContextTest {

    @Test
    void labelIsLowercasedAndSanitised() {
        assertEquals("etl_trades-2024_run1", RequestContext.toLabel("ETL Trades-2024:run1"));
    }

    @Test
    void labelIsTruncatedTo63() {
        assertEquals(63, RequestContext.toLabel("a".repeat(100)).length());
    }

    @Test
    void uuidIsAValidLabelAsIs() {
        String uuid = "123e4567-e89b-12d3-a456-426614174000";
        assertEquals(uuid, RequestContext.toLabel(uuid));
    }

    @Test
    void attachMakesIdCurrentAndLabelled() {
        RequestContext.attach("abc-123");
        assertEquals("abc-123", RequestContext.current());
        assertEquals("abc-123", RequestContext.labels().get("job_run_id"));
    }
}
