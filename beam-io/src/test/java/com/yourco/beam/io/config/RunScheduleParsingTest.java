package com.yourco.beam.io.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.model.RunScheduleConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * run_details parsing — contract DATE_SCHEDULING_RULES.md Part 1 §3: reports store the run-window
 * start as {@code freqDtl}, data sources as {@code freqRunDay}.
 */
class RunScheduleParsingTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static JsonNode json(String s) throws Exception { return JSON.readTree(s); }

    private static final String FULL = "{\"frequency\":\"MONTHLY\",\"freqRunDay\":\"WD+3\",\"freqDtl\":\"WD+9\","
        + "\"maxFreqRunDay\":\"WD+5\",\"dayLag\":\"WD-1\",\"dateType\":\"lastBusDayMonth\",\"calendarKey\":\"CAL_US\"}";

    @Test
    void aDataSourceReadsFreqRunDayAndIgnoresFreqDtl() throws Exception {
        RunScheduleConfig c = BigQuerySourceConfigRepository.parseRunSchedule(
            json(FULL), BigQuerySourceConfigRepository.SOURCE_RUN_DAY_KEY);
        assertEquals("WD+3", c.freqRunDay);
        assertEquals("MONTHLY", c.frequency);
        assertEquals("WD+5", c.maxFreqRunDay);
        assertEquals("WD-1", c.dayLag);
        assertEquals("lastBusDayMonth", c.dateType);
        assertEquals("CAL_US", c.calendarKey);
    }

    @Test
    void aReportReadsFreqDtlAndIgnoresFreqRunDay() throws Exception {
        RunScheduleConfig c = BigQuerySourceConfigRepository.parseRunSchedule(
            json(FULL), BigQuerySourceConfigRepository.REPORT_RUN_DAY_KEY);
        assertEquals("WD+9", c.freqRunDay);
    }

    @Test
    void aReportWithOnlyFreqRunDayHasNoRunWindowStart() throws Exception {
        RunScheduleConfig c = BigQuerySourceConfigRepository.parseRunSchedule(
            json("{\"frequency\":\"MONTHLY\",\"freqRunDay\":\"WD+3\",\"calendarKey\":\"CAL_US\"}"),
            BigQuerySourceConfigRepository.REPORT_RUN_DAY_KEY);
        assertNull(c.freqRunDay);
    }

    @Test
    void valuesAreKeptAsRawText_aJsonNumberIsNotInterpretedAsAWorkdayOffset() throws Exception {
        RunScheduleConfig c = BigQuerySourceConfigRepository.parseRunSchedule(
            json("{\"frequency\":\"MONTHLY\",\"maxFreqRunDay\":5}"), "freqRunDay");
        assertEquals("5", c.maxFreqRunDay);   // RunDateCalculator rejects this as not evaluable
    }

    @Test
    void blankNullAndMissingValuesBecomeNull() throws Exception {
        RunScheduleConfig c = BigQuerySourceConfigRepository.parseRunSchedule(
            json("{\"frequency\":\"DAILY\",\"dayLag\":\"\",\"dateType\":null,\"calendarKey\":\"  \"}"), "freqRunDay");
        assertNull(c.dayLag);
        assertNull(c.dateType);
        assertNull(c.calendarKey);
        assertNull(c.maxFreqRunDay);
        assertTrue(c.hasSchedule());
    }

    @Test
    void absentOrNonObjectDetailsMeanNoSchedule() throws Exception {
        assertFalse(BigQuerySourceConfigRepository.parseRunSchedule(null, "freqRunDay").hasSchedule());
        assertFalse(BigQuerySourceConfigRepository.parseRunSchedule(json("[1,2]"), "freqRunDay").hasSchedule());
        assertFalse(BigQuerySourceConfigRepository.parseRunSchedule(json("{}"), "freqRunDay").hasSchedule());
    }
}
