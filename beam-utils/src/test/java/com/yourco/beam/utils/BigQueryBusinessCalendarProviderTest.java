package com.yourco.beam.utils;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parsing of the stored calendar JSON, and caching, without touching BigQuery. */
class BigQueryBusinessCalendarProviderTest {

    private static final String SAMPLE =
        "[{\"Calendar\": {\"holiday\":\"20260101,20270901\",\"weekend\":\"saturday,sunday\"}}]";

    private static LocalDate d(int y, int m, int day) { return LocalDate.of(y, m, day); }

    private static BusinessCalendar parse(String json) {
        return BigQueryBusinessCalendarProvider.parse("CAL_US", json);
    }

    @Test
    void theDocumentedShapeBecomesACalendar() {
        BusinessCalendar c = parse(SAMPLE);
        assertFalse(c.isBusinessDay(d(2026, 1, 1)));    // holiday (a Thursday)
        assertFalse(c.isBusinessDay(d(2027, 9, 1)));    // holiday (a Wednesday)
        assertFalse(c.isBusinessDay(d(2026, 9, 5)));    // Saturday
        assertFalse(c.isBusinessDay(d(2026, 9, 6)));    // Sunday
        assertTrue(c.isBusinessDay(d(2026, 9, 7)));     // Monday, not a holiday here
        assertTrue(c.isBusinessDay(d(2026, 1, 2)));     // Friday
    }

    @Test
    void weekendNamesAreCaseInsensitiveTrimmedAndAcceptShortForms() {
        BusinessCalendar c = parse("[{\"Calendar\":{\"holiday\":\"\",\"weekend\":\" Friday , SAT \"}}]");
        assertFalse(c.isBusinessDay(d(2026, 9, 4)));    // Friday
        assertFalse(c.isBusinessDay(d(2026, 9, 5)));    // Saturday
        assertTrue(c.isBusinessDay(d(2026, 9, 6)));     // Sunday is a working day in this calendar
    }

    @Test
    void holidayListToleratesSpacesAndTrailingCommasAndMayBeMissing() {
        BusinessCalendar c = parse("[{\"Calendar\":{\"holiday\":\" 20260101 , 20260102 ,\",\"weekend\":\"saturday,sunday\"}}]");
        assertFalse(c.isBusinessDay(d(2026, 1, 1)));
        assertFalse(c.isBusinessDay(d(2026, 1, 2)));
        assertTrue(parse("[{\"Calendar\":{\"weekend\":\"saturday,sunday\"}}]").isBusinessDay(d(2026, 1, 1)));
    }

    @Test
    void aBlankWeekendIsAllowedButAMissingWeekendIsNot() {
        assertTrue(parse("[{\"Calendar\":{\"holiday\":\"\",\"weekend\":\"\"}}]").isBusinessDay(d(2026, 9, 5)));
        assertThrows(IllegalArgumentException.class,
            () -> parse("[{\"Calendar\":{\"holiday\":\"20260101\"}}]"));
    }

    @Test
    void aBareObjectAndADifferentWrapperCaseAreAccepted() {
        assertFalse(parse("{\"Calendar\":{\"holiday\":\"20260101\",\"weekend\":\"saturday\"}}").isBusinessDay(d(2026, 1, 1)));
        assertFalse(parse("[{\"calendar\":{\"holiday\":\"20260101\",\"weekend\":\"saturday\"}}]").isBusinessDay(d(2026, 1, 1)));
        assertFalse(parse("{\"holiday\":\"20260101\",\"weekend\":\"saturday\"}").isBusinessDay(d(2026, 1, 1)));
    }

    @Test
    void badDataFailsLoudlyInsteadOfDroppingAHoliday() {
        for (String bad : new String[] {
                "[{\"Calendar\":{\"holiday\":\"2026-01-01\",\"weekend\":\"saturday\"}}]",   // wrong date format
                "[{\"Calendar\":{\"holiday\":\"20260101,2026013\",\"weekend\":\"saturday\"}}]",
                "[{\"Calendar\":{\"holiday\":\"20260230\",\"weekend\":\"saturday\"}}]",      // not a real date
                "[{\"Calendar\":{\"holiday\":\"\",\"weekend\":\"saturday,funday\"}}]",       // unknown weekday
                "[{\"Other\":{\"holiday\":\"\",\"weekend\":\"saturday\"}}]",                 // no Calendar object
                "[]", "not json", "", "  "}) {
            assertThrows(IllegalArgumentException.class, () -> parse(bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> parse(null));
    }

    @Test
    void aCalendarIsLoadedOncePerKey() {
        AtomicInteger calls = new AtomicInteger();
        BigQueryBusinessCalendarProvider provider = new BigQueryBusinessCalendarProvider(key -> {
            calls.incrementAndGet();
            return SAMPLE;
        });
        BusinessCalendar first = provider.forKey("CAL_US");
        assertTrue(provider.forKey("CAL_US") == first);
        provider.forKey("CAL_EU");
        assertEquals(2, calls.get());
    }

    @Test
    void aFailedLookupPropagatesAndIsNotCached() {
        AtomicInteger calls = new AtomicInteger();
        BigQueryBusinessCalendarProvider provider = new BigQueryBusinessCalendarProvider(key -> {
            if (calls.incrementAndGet() == 1) throw new IllegalArgumentException("no calendar " + key);
            return SAMPLE;
        });
        assertThrows(IllegalArgumentException.class, () -> provider.forKey("CAL_US"));
        assertTrue(provider.forKey("CAL_US").isBusinessDay(d(2026, 9, 7)));   // second try succeeds
    }

    /** The calendar works end to end through the scheduling rules, not just in isolation. */
    @Test
    void feedsRunDateCalculator() {
        BusinessCalendarProvider provider = new BigQueryBusinessCalendarProvider(key -> SAMPLE);
        var schedule = new com.yourco.beam.model.RunScheduleConfig("MONTHLY", "WD+1", null, null, null, "CAL_US");
        // Jan 1 2026 is a holiday, so WD+1 of January is Fri Jan 2.
        var decision = RunDateCalculator.evaluate(schedule, d(2026, 1, 1),
            RunDateCalculator.ItemType.DATA_SOURCE, provider);
        assertEquals(RunDateCalculator.ScheduleDecision.Status.NOT_YET_ELIGIBLE, decision.status);
        assertEquals(d(2026, 1, 2), decision.freqRunDate);
    }
}
