package com.yourco.beam.utils;

import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.RunScheduleConfig;
import com.yourco.beam.utils.RunDateCalculator.ItemType;
import com.yourco.beam.utils.RunDateCalculator.ScheduleDecision;
import com.yourco.beam.utils.RunDateCalculator.ScheduleDecision.Status;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Parity tests for the Finance Automation scheduling rules, one per BAU example where the BAU
 * description gives one. Calendar: Sat/Sun weekends, 2026-09-07 (Mon) a holiday.
 *
 * September 2026 business days: Tue 1 (WD+1), Wed 2, Thu 3 (WD+3), Fri 4, [Sat 5, Sun 6,
 * Mon 7 holiday], Tue 8 (WD+5), ... Wed 30 (WD-1).
 */
class RunDateCalculatorTest {

    private static final LocalDate HOLIDAY = LocalDate.of(2026, 9, 7);

    private static final BusinessCalendar CALENDAR = date ->
        date.getDayOfWeek() != DayOfWeek.SATURDAY
            && date.getDayOfWeek() != DayOfWeek.SUNDAY
            && !Set.of(HOLIDAY).contains(date);

    private static final BusinessCalendarProvider PROVIDER = key -> {
        if (!"CAL_US".equals(key)) throw new IllegalArgumentException("unknown calendar " + key);
        return CALENDAR;
    };

    private static RunScheduleConfig schedule(String frequency, String freqRunDay, String maxFreqRunDay,
                                              String dayLag, String dateType) {
        return new RunScheduleConfig(frequency, freqRunDay, maxFreqRunDay, dayLag, dateType, "CAL_US");
    }

    private static ScheduleDecision source(RunScheduleConfig s, LocalDate businessDate) {
        return RunDateCalculator.evaluate(s, businessDate, false, ItemType.DATA_SOURCE, PROVIDER);
    }

    private static LocalDate d(int y, int m, int day) { return LocalDate.of(y, m, day); }

    // ── WHEN: non-DAILY run window (WD+3 .. WD+5) ─────────────────────────────

    private static final RunScheduleConfig MONTHLY_WD3_TO_WD5 =
        schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+5", null, null);

    @Test
    void beforeFreqRunDayIsNotYetEligible() {
        ScheduleDecision d = source(MONTHLY_WD3_TO_WD5, d(2026, 9, 2));
        assertEquals(Status.NOT_YET_ELIGIBLE, d.status);
        assertEquals(d(2026, 9, 3), d.freqRunDate);
        assertNull(d.dates);
    }

    @Test
    void windowIsInclusiveAtBothEnds() {
        assertEquals(Status.ELIGIBLE, source(MONTHLY_WD3_TO_WD5, d(2026, 9, 3)).status);
        ScheduleDecision last = source(MONTHLY_WD3_TO_WD5, d(2026, 9, 8));   // WD+5, after the holiday
        assertEquals(Status.ELIGIBLE, last.status);
        assertEquals(d(2026, 9, 8), last.maxFreqRunDate);
    }

    @Test
    void afterMaxFreqRunDayIsExpired() {
        assertEquals(Status.EXPIRED, source(MONTHLY_WD3_TO_WD5, d(2026, 9, 9)).status);
    }

    @Test
    void manualForceRunBypassesMaxButNotFreqRunDay() {
        ScheduleDecision late = RunDateCalculator.evaluate(MONTHLY_WD3_TO_WD5, d(2026, 9, 9), true,
            ItemType.DATA_SOURCE, PROVIDER);
        assertEquals(Status.ELIGIBLE, late.status);
        ScheduleDecision early = RunDateCalculator.evaluate(MONTHLY_WD3_TO_WD5, d(2026, 9, 2), true,
            ItemType.DATA_SOURCE, PROVIDER);
        assertEquals(Status.NOT_YET_ELIGIBLE, early.status);
    }

    @Test
    void blankMaxFreqRunDayHasNoUpperBound() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD+3", null, null, null);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 9, 29)).status);
    }

    @Test
    void nonDailyRunsOnAWeekendInsideItsWindow() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD+1", "WD+5", null, null);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 9, 5)).status);   // Saturday
    }

    @Test
    void wdMinusOneIsLastBusinessDayAndWdZeroIsLastDayOfPreviousMonth() {
        assertEquals(d(2026, 9, 30), RunDateCalculator.calculateFreqRunDate(
            schedule(RunScheduleConfig.MONTHLY, "WD-1", null, null, null), d(2026, 9, 15), CALENDAR));
        assertEquals(d(2026, 8, 31), RunDateCalculator.calculateFreqRunDate(
            schedule(RunScheduleConfig.MONTHLY, "WD+0", null, null, null), d(2026, 9, 15), CALENDAR));
        // WD+0 → eligible from the first day of the month
        assertEquals(Status.ELIGIBLE,
            source(schedule(RunScheduleConfig.MONTHLY, "WD+0", null, null, null), d(2026, 9, 1)).status);
    }

    // ── WHEN: DAILY business-day gate ─────────────────────────────────────────

    @Test
    void dailySkipsNonBusinessDaysEvenWithForceRunAndIgnoresWindow() {
        RunScheduleConfig s = schedule(RunScheduleConfig.DAILY, "WD+20", "WD+21", null, null);
        assertEquals(Status.NON_BUSINESS_DAY, source(s, HOLIDAY).status);
        assertEquals(Status.NON_BUSINESS_DAY, RunDateCalculator.evaluate(
            s, d(2026, 9, 5), true, ItemType.DATA_SOURCE, PROVIDER).status);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 9, 2)).status);   // window ignored
    }

    // ── WHICH: DAILY lag ──────────────────────────────────────────────────────

    @Test
    void dailyWorkdayLagSkipsHolidayAndWeekend() {
        ScheduleDecision d = source(schedule(RunScheduleConfig.DAILY, null, null, "WD+1", null),
                                    d(2026, 9, 8));
        assertEquals(d(2026, 9, 4), d.dates.periodStart);
        assertEquals(d(2026, 9, 4), d.dates.periodEnd);
        assertEquals(20260904, d.dates.periodId);
        assertEquals(d(2026, 9, 8), d.dates.runDate);   // Business Date stays the run date
    }

    @Test
    void dailyCalendarLagDoesNotAdjust() {
        ScheduleDecision d = source(schedule(RunScheduleConfig.DAILY, null, null, "CAL+1", null),
                                    d(2026, 9, 14));   // Monday
        assertEquals(d(2026, 9, 13), d.dates.periodEnd);   // Sunday
    }

    // ── WHICH: non-DAILY prefix rule ──────────────────────────────────────────

    @Test
    void nonDailyLagUsesOnlyThePrefix() {
        LocalDate bd = d(2026, 9, 3);
        for (String previous : new String[] {null, "WD+1", "WD+5", "CAL+1", "anything"}) {
            RunDates dates = source(schedule(RunScheduleConfig.MONTHLY, null, null, previous, null), bd).dates;
            assertEquals(202608, dates.periodId, "dayLag=" + previous);
        }
        for (String current : new String[] {"WD-1", "WD-5", "CAL-3"}) {
            RunDates dates = source(schedule(RunScheduleConfig.MONTHLY, null, null, current, null), bd).dates;
            assertEquals(202609, dates.periodId, "dayLag=" + current);
        }
    }

    @Test
    void quarterlyAndAnnualPeriods() {
        RunDates q = source(schedule(RunScheduleConfig.QUARTERLY, null, null, null, null), d(2026, 4, 3)).dates;
        assertEquals(d(2026, 1, 1), q.periodStart);
        assertEquals(d(2026, 3, 31), q.periodEnd);
        assertEquals(20261, q.periodId);
        RunDates y = source(schedule(RunScheduleConfig.ANNUALLY, null, null, null, null), d(2026, 1, 5)).dates;
        assertEquals(2025, y.periodId);
        assertEquals(d(2025, 12, 31), y.periodEnd);
    }

    // ── WHAT: data-source dateType ────────────────────────────────────────────

    @Test
    void lastBusDayMonthMovesMonthEndOffTheWeekend() {
        LocalDate bd = d(2026, 2, 3);   // processes January 2026; Jan 31 is a Saturday
        assertEquals(d(2026, 1, 30), source(schedule(RunScheduleConfig.MONTHLY, null, null, null,
            RunScheduleConfig.LAST_BUS_DAY_MONTH), bd).dates.periodEnd);
        assertEquals(d(2026, 1, 31), source(schedule(RunScheduleConfig.MONTHLY, null, null, null,
            RunScheduleConfig.LAST_DAY_MONTH), bd).dates.periodEnd);
        assertEquals(d(2026, 1, 31), source(schedule(RunScheduleConfig.MONTHLY, null, null, null,
            "somethingElse"), bd).dates.periodEnd);
    }

    @Test
    void reportIgnoresDateType() {
        ScheduleDecision d = RunDateCalculator.evaluate(schedule(RunScheduleConfig.MONTHLY, null, null,
            null, RunScheduleConfig.LAST_BUS_DAY_MONTH), d(2026, 2, 3), false, ItemType.REPORT, PROVIDER);
        assertEquals(d(2026, 1, 31), d.dates.periodEnd);
        assertEquals(202601, d.dates.periodId);
    }

    // ── NOT_EVALUABLE ─────────────────────────────────────────────────────────

    @Test
    void configurationProblemsAreNotEvaluable() {
        LocalDate bd = d(2026, 9, 3);
        assertEquals(Status.NOT_EVALUABLE, source(schedule("WEEKLY", null, null, null, null), bd).status);
        assertEquals(Status.NOT_EVALUABLE, source(schedule(null, "WD+1", null, null, null), bd).status);
        assertEquals(Status.NOT_EVALUABLE, source(new RunScheduleConfig(RunScheduleConfig.MONTHLY,
            null, null, null, null, null), bd).status);                               // no calendarKey
        assertEquals(Status.NOT_EVALUABLE, source(new RunScheduleConfig(RunScheduleConfig.MONTHLY,
            null, null, null, null, "NOPE"), bd).status);                             // unknown calendar
        assertEquals(Status.NOT_EVALUABLE,
            source(schedule(RunScheduleConfig.MONTHLY, "WD+1", "5", null, null), bd).status);   // bare int
        assertEquals(Status.NOT_EVALUABLE,
            source(schedule(RunScheduleConfig.DAILY, null, null, "CD+1", null), bd).status);
    }
}
