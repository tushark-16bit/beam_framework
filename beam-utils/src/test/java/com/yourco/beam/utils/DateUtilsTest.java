package com.yourco.beam.utils;

import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Business Date resolution: {@code --runDate} wins; otherwise today in {@code --businessTimeZone}. */
class DateUtilsTest {

    private static FrameworkOptions options(String runDate, String zone) {
        FrameworkOptions o = PipelineOptionsFactory.as(FrameworkOptions.class);
        o.setRunDate(runDate);
        if (zone != null) o.setBusinessTimeZone(zone);
        return o;
    }

    @Test
    void anExplicitRunDateWinsOverTheTimeZone() {
        assertEquals(LocalDate.of(2026, 9, 3), DateUtils.resolveRunDate(options("2026-09-03", "Asia/Tokyo")));
    }

    @Test
    void withoutARunDateTodayIsTakenInTheBusinessTimeZone() {
        for (String zone : new String[] {"UTC", "Pacific/Kiritimati", "Pacific/Pago_Pago", "America/New_York"}) {
            ZoneId id = ZoneId.of(zone);
            LocalDate before = LocalDate.now(id);
            LocalDate resolved = DateUtils.resolveRunDate(options(null, zone));
            LocalDate after = LocalDate.now(id);
            assertTrue(!resolved.isBefore(before) && !resolved.isAfter(after), zone + " → " + resolved);
        }
    }

    @Test
    void theDefaultTimeZoneIsUtc() {
        LocalDate before = LocalDate.now(ZoneId.of("UTC"));
        LocalDate resolved = DateUtils.resolveRunDate(options(null, null));
        assertTrue(!resolved.isBefore(before) && !resolved.isAfter(LocalDate.now(ZoneId.of("UTC"))));
    }

    @Test
    void anInvalidTimeZoneFailsWithAClearMessage() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> DateUtils.resolveRunDate(options(null, "Not/AZone")));
        assertTrue(e.getMessage().contains("--businessTimeZone"), e.getMessage());
    }

    @Test
    void anInvalidRunDateFailsLoudly() {
        assertThrows(IllegalArgumentException.class, () -> DateUtils.resolveRunDate(options("03/09/2026", null)));
    }
}
