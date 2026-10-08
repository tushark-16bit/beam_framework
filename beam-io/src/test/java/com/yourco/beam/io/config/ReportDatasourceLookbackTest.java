package com.yourco.beam.io.config;

import com.yourco.beam.model.ReportDatasourceRef;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** lookback_from / lookback_to validation on a report's datasource reference. */
class ReportDatasourceLookbackTest {

    @Test
    void absentLookbackMeansNoExtraCheck() {
        assertFalse(new ReportDatasourceRef("ds", "eod", "a", true).hasLookback());
    }

    @Test
    void zeroToMinus11IsAccepted() {
        ReportDatasourceRef ref = new ReportDatasourceRef("ds", "eod", "a", true, 0, -11);
        assertTrue(ref.hasLookback());
        assertEquals(0, ref.lookbackFrom);
        assertEquals(-11, ref.lookbackTo);
    }

    @Test
    void invalidCombinationsFailTheConfigLoad() {
        assertThrows(IllegalArgumentException.class,
            () -> new ReportDatasourceRef("ds", "eod", "a", true, 0, null));   // only one given
        assertThrows(IllegalArgumentException.class,
            () -> new ReportDatasourceRef("ds", "eod", "a", true, 1, -3));     // positive from
        assertThrows(IllegalArgumentException.class,
            () -> new ReportDatasourceRef("ds", "eod", "a", true, -3, 0));     // to above from
    }
}
