package com.yourco.beam.utils;

import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.RunScheduleConfig;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunDateCalculatorTest {

    private static final RunDates DATES = new RunDates(
        LocalDate.of(2024, 2, 2), LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 31), 202401);

    @Test
    void noScheduleIsAlwaysInWindow() {
        assertTrue(RunDateCalculator.checkRunWindow(RunScheduleConfig.none(), DATES).shouldRun());
    }

    @Test
    void scheduleWithoutWindowBoundsIsInWindowWithoutCallingStubs() {
        RunScheduleConfig schedule = new RunScheduleConfig(
            RunScheduleConfig.LAST_DAY_OF_MONTH, RunScheduleConfig.MONTHLY, null,
            RunScheduleConfig.NO_MAX, null, null);
        assertTrue(RunDateCalculator.checkRunWindow(schedule, DATES).shouldRun());
    }

    @Test
    void freqRunDayReachesUnimplementedStub() {
        RunScheduleConfig schedule = new RunScheduleConfig(
            RunScheduleConfig.LAST_DAY_OF_MONTH, RunScheduleConfig.MONTHLY, "WD+1",
            RunScheduleConfig.NO_MAX, null, null);
        assertThrows(UnsupportedOperationException.class,
            () -> RunDateCalculator.checkRunWindow(schedule, DATES));
    }

    @Test
    void maxFreqRunDayReachesUnimplementedStub() {
        RunScheduleConfig schedule = new RunScheduleConfig(
            RunScheduleConfig.LAST_DAY_OF_MONTH, RunScheduleConfig.MONTHLY, null, 5, null, null);
        assertThrows(UnsupportedOperationException.class,
            () -> RunDateCalculator.checkRunWindow(schedule, DATES));
    }
}
