package com.yourco.beam.utils;

import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.RunScheduleConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.RunDateCalculator.ItemType;
import com.yourco.beam.utils.RunDateCalculator.ScheduleDecision;
import com.yourco.beam.utils.RunDateCalculator.ScheduleDecision.Status;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the Finance Automation scheduling rules. The contract is
 * {@code DATE_SCHEDULING_RULES.md} (repo root) — do not change an expectation here without
 * checking it against that file, and ask the owner if they conflict.
 *
 * <p>Fake calendar (see {@link #calendarSanity}): Saturday/Sunday weekends plus holidays
 * 2025-12-25, 2026-01-01, 2026-04-03 (Good Friday), 2026-06-30 (a fictional month-end holiday),
 * 2026-09-07, 2026-11-26, 2026-12-25.
 *
 * <pre>
 * September 2026:  Tue 1(WD+1) Wed 2 Thu 3(WD+3) Fri 4 | Sat 5 Sun 6 Mon 7=holiday | Tue 8(WD+5) 9 10 11
 *                  ... Mon 28(WD-3) Tue 29(WD-2) Wed 30(WD-1)
 * October 2026:    Thu 1(WD+1) Fri 2 Mon 5(WD+3) Tue 6 Wed 7(WD+5) ... Wed 28(WD-3) Thu 29 Fri 30(WD-1)
 * </pre>
 */
class RunDateCalculatorTest {

    private static final Set<LocalDate> HOLIDAYS = Set.of(
        LocalDate.of(2025, 12, 25), LocalDate.of(2026, 1, 1), LocalDate.of(2026, 4, 3),
        LocalDate.of(2026, 6, 30), LocalDate.of(2026, 9, 7), LocalDate.of(2026, 11, 26),
        LocalDate.of(2026, 12, 25));

    private static final BusinessCalendar CALENDAR = date ->
        date.getDayOfWeek() != DayOfWeek.SATURDAY
            && date.getDayOfWeek() != DayOfWeek.SUNDAY
            && !HOLIDAYS.contains(date);

    private static final BusinessCalendarProvider PROVIDER = key -> {
        if (!"CAL_US".equals(key)) throw new IllegalArgumentException("unknown calendar " + key);
        return CALENDAR;
    };

    // ── helpers ───────────────────────────────────────────────────────────────

    private static RunScheduleConfig schedule(String frequency, String freqRunDay, String maxFreqRunDay,
                                              String dayLag, String dateType) {
        return new RunScheduleConfig(frequency, freqRunDay, maxFreqRunDay, dayLag, dateType, "CAL_US");
    }

    private static ScheduleDecision source(RunScheduleConfig s, LocalDate businessDate) {
        return RunDateCalculator.evaluate(s, businessDate, ItemType.DATA_SOURCE, PROVIDER);
    }

    private static ScheduleDecision report(RunScheduleConfig s, LocalDate businessDate) {
        return RunDateCalculator.evaluate(s, businessDate, ItemType.REPORT, PROVIDER);
    }

    private static LocalDate d(int y, int m, int day) { return LocalDate.of(y, m, day); }

    private static FrameworkOptions options(String runDate, int periodId) {
        FrameworkOptions o = PipelineOptionsFactory.as(FrameworkOptions.class);
        o.setRunDate(runDate);
        o.setPeriodId(periodId);
        return o;
    }

    private static ScheduleDecision source(RunScheduleConfig s, FrameworkOptions o) {
        return RunDateCalculator.evaluate(s, o, ItemType.DATA_SOURCE, PROVIDER);
    }

    /** Asserts period start / end / id of a decision's dates. */
    private static void assertPeriod(ScheduleDecision decision, LocalDate start, LocalDate end, int periodId) {
        assertNotNull(decision.dates, "dates must be calculated: " + decision);
        assertEquals(start, decision.dates.periodStart, "periodStart");
        assertEquals(end, decision.dates.periodEnd, "periodEnd");
        assertEquals(periodId, decision.dates.periodId, "periodId");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // The test calendar itself — if these fail, the expectations below are wrong
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void calendarSanity() {
        assertEquals(DayOfWeek.MONDAY,    d(2026, 9, 7).getDayOfWeek());   // Labor Day
        assertEquals(DayOfWeek.TUESDAY,   d(2026, 9, 1).getDayOfWeek());
        assertEquals(DayOfWeek.THURSDAY,  d(2026, 1, 1).getDayOfWeek());
        assertEquals(DayOfWeek.FRIDAY,    d(2026, 4, 3).getDayOfWeek());   // Good Friday
        assertEquals(DayOfWeek.TUESDAY,   d(2026, 6, 30).getDayOfWeek());
        assertEquals(DayOfWeek.THURSDAY,  d(2026, 11, 26).getDayOfWeek());
        assertEquals(DayOfWeek.FRIDAY,    d(2026, 12, 25).getDayOfWeek());
        assertEquals(DayOfWeek.THURSDAY,  d(2025, 12, 25).getDayOfWeek());
        assertEquals(DayOfWeek.SATURDAY,  d(2026, 1, 31).getDayOfWeek());
        assertEquals(DayOfWeek.SATURDAY,  d(2026, 2, 28).getDayOfWeek());
        assertEquals(DayOfWeek.SUNDAY,    d(2026, 5, 31).getDayOfWeek());
        assertEquals(DayOfWeek.TUESDAY,   d(2028, 2, 29).getDayOfWeek());
        assertFalse(CALENDAR.isBusinessDay(d(2026, 9, 7)));
        assertTrue(CALENDAR.isBusinessDay(d(2026, 9, 8)));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // WHEN — non-DAILY run window (freqRunDay .. maxFreqRunDay, inclusive)
    // ══════════════════════════════════════════════════════════════════════════

    private static final RunScheduleConfig MONTHLY_WD3_TO_WD5 =
        schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+5", null, null);

    @Test
    void beforeFreqRunDayIsNotYetEligible() {
        ScheduleDecision d = source(MONTHLY_WD3_TO_WD5, d(2026, 9, 2));
        assertEquals(Status.NOT_YET_ELIGIBLE, d.status);
        assertEquals(d(2026, 9, 3), d.freqRunDate);
        assertFalse(d.shouldRun());
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

    /**
     * WD+3..WD+5 (Sep 3 .. Sep 8) across every day of September: weekends and the Sep 7 holiday are
     * NON_BUSINESS_DAY (D6); business days are not-yet → eligible → expired.
     */
    @Test
    void everyDayOfSeptemberAgainstAWd3ToWd5Window() {
        for (int day = 1; day <= 30; day++) {
            LocalDate bd = d(2026, 9, day);
            ScheduleDecision dec = source(MONTHLY_WD3_TO_WD5, bd);
            Status expected = !CALENDAR.isBusinessDay(bd) ? Status.NON_BUSINESS_DAY
                : day < 3 ? Status.NOT_YET_ELIGIBLE : day <= 8 ? Status.ELIGIBLE : Status.EXPIRED;
            assertEquals(expected, dec.status, "Sep " + day);
            assertEquals(202608, dec.dates.periodId, "Sep " + day + " always processes August");
        }
    }

    /** CONTRACT D6: every frequency needs the Business Date to be a business day, even inside the window. */
    @Test
    void nonDailyIsSkippedOnAWeekendAndOnAHolidayEvenInsideItsWindow() {
        for (LocalDate nonBusinessDay : new LocalDate[] {d(2026, 9, 5), d(2026, 9, 6), d(2026, 9, 7)}) {
            ScheduleDecision dec = source(MONTHLY_WD3_TO_WD5, nonBusinessDay);   // Sat, Sun, the holiday
            assertEquals(Status.NON_BUSINESS_DAY, dec.status, nonBusinessDay.toString());
            assertFalse(dec.shouldRun());
            // the window and the period are still worked out and carried on the skipped decision
            assertEquals(d(2026, 9, 3), dec.freqRunDate);
            assertEquals(d(2026, 9, 8), dec.maxFreqRunDate);
            assertPeriod(dec, d(2026, 8, 1), d(2026, 8, 31), 202608);
        }
        // the surrounding business days, still inside the window, run
        assertEquals(Status.ELIGIBLE, source(MONTHLY_WD3_TO_WD5, d(2026, 9, 4)).status);   // Friday
        assertEquals(Status.ELIGIBLE, source(MONTHLY_WD3_TO_WD5, d(2026, 9, 8)).status);   // Tuesday
    }

    /** A skipped weekend loses nothing: the next business day in the window picks the item up. */
    @Test
    void anItemSkippedOnTheWeekendRunsOnTheNextBusinessDayInsideItsWindow() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+5", null, null);
        // the Friday run didn't happen (say it failed); Saturday and Sunday skip, Tuesday (WD+5) runs
        assertEquals(Status.NON_BUSINESS_DAY, source(s, d(2026, 9, 5)).status);
        assertEquals(Status.NON_BUSINESS_DAY, source(s, d(2026, 9, 6)).status);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 9, 8)).status);
    }

    @Test
    void aMalformedRunDayIsStillReportedOnAWeekend() {
        ScheduleDecision dec = source(schedule(RunScheduleConfig.MONTHLY, "WD+x", null, null, null), d(2026, 9, 5));
        assertEquals(Status.NOT_EVALUABLE, dec.status);
    }

    /** "Weekends are not business days in WD calculations": WD+n counts neither weekend days nor holidays. */
    @Test
    void wdCalculationsNeverCountWeekendsOrHolidays() {
        // September: Sep 5/6 weekend, Sep 7 holiday → WD+5 is Tue Sep 8 (not Sat/Sun/Mon), WD+4 is Fri Sep 4
        assertEquals(d(2026, 9, 4), RunDateCalculator.calculateFreqRunDate(
            schedule(RunScheduleConfig.MONTHLY, "WD+4", null, null, null), d(2026, 9, 15), CALENDAR));
        assertEquals(d(2026, 9, 8), RunDateCalculator.calculateFreqRunDate(
            schedule(RunScheduleConfig.MONTHLY, "WD+5", null, null, null), d(2026, 9, 15), CALENDAR));
        // Counting backwards from month end (Wed Sep 30): WD-1..WD-3 = Sep 30, 29, 28
        assertEquals(d(2026, 9, 28), RunDateCalculator.calculateFreqRunDate(
            schedule(RunScheduleConfig.MONTHLY, "WD-3", null, null, null), d(2026, 9, 15), CALENDAR));
        // Month end falling on a weekend: Jan 31 2026 is a Saturday → WD-1 is Fri Jan 30
        assertEquals(d(2026, 1, 30), RunDateCalculator.calculateFreqRunDate(
            schedule(RunScheduleConfig.MONTHLY, "WD-1", null, null, null), d(2026, 1, 15), CALENDAR));
        // DAILY business-day lag skips the weekend too (Monday - 1 business day = Friday)
        assertPeriod(daily("WD-1", d(2026, 9, 14)), d(2026, 9, 11), d(2026, 9, 11), 20260911);
    }

    @Test
    void blankMaxFreqRunDayHasNoUpperBoundUntilThePeriodRollsOver() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD+3", null, null, null);
        for (int day : new int[] {3, 15, 29, 30}) {
            ScheduleDecision dec = source(s, d(2026, 9, day));
            assertEquals(Status.ELIGIBLE, dec.status, "Sep " + day);
            assertEquals(202608, dec.dates.periodId);
            assertNull(dec.maxFreqRunDate);
        }
        // Oct 1: a new cycle — the window for October hasn't opened (WD+3 = Oct 5) and the period moved on.
        ScheduleDecision oct1 = source(s, d(2026, 10, 1));
        assertEquals(Status.NOT_YET_ELIGIBLE, oct1.status);
        assertEquals(202609, oct1.dates.periodId);
    }

    @Test
    void windowAndPeriodRollOverTogetherOnTheFirstOfTheNextMonth() {
        assertEquals(Status.NOT_YET_ELIGIBLE, source(MONTHLY_WD3_TO_WD5, d(2026, 10, 1)).status);
        assertEquals(d(2026, 10, 5), source(MONTHLY_WD3_TO_WD5, d(2026, 10, 1)).freqRunDate);
        ScheduleDecision oct5 = source(MONTHLY_WD3_TO_WD5, d(2026, 10, 5));
        assertEquals(Status.ELIGIBLE, oct5.status);
        assertEquals(202609, oct5.dates.periodId);
        assertEquals(Status.ELIGIBLE, source(MONTHLY_WD3_TO_WD5, d(2026, 10, 7)).status);   // WD+5
        assertEquals(Status.EXPIRED,  source(MONTHLY_WD3_TO_WD5, d(2026, 10, 8)).status);
    }

    /** Month-end processing: window on the last three business days, processing the CURRENT month. */
    @Test
    void monthEndWindowUsingNegativeWorkdays() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD-3", "WD-1", "WD-1", null);
        assertEquals(Status.NOT_YET_ELIGIBLE, source(s, d(2026, 9, 25)).status);          // Friday, before WD-3
        assertEquals(Status.NON_BUSINESS_DAY, source(s, d(2026, 9, 27)).status);          // Sunday
        ScheduleDecision first = source(s, d(2026, 9, 28));                                // WD-3
        assertEquals(Status.ELIGIBLE, first.status);
        assertEquals(d(2026, 9, 28), first.freqRunDate);
        assertEquals(d(2026, 9, 30), first.maxFreqRunDate);
        assertEquals(202609, first.dates.periodId);                                        // current month
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 9, 30)).status);                   // WD-1 inclusive
        ScheduleDecision oct1 = source(s, d(2026, 10, 1));
        assertEquals(Status.NOT_YET_ELIGIBLE, oct1.status);                                // October's window
        assertEquals(d(2026, 10, 28), oct1.freqRunDate);
        assertEquals(202610, oct1.dates.periodId);
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

    /**
     * The 15th of September, WD-1 vs WD+0: in both cases the period is calculated and stored next to
     * the decision — only the status differs. (WD-1 = Sep 30 → not yet eligible on the 15th;
     * WD+0 = Aug 31 → eligible.)
     */
    @Test
    void wdMinusOneVersusWdZeroOnTheFifteenth_periodIsCalculatedEitherWay() {
        ScheduleDecision wdMinus1 = source(schedule(RunScheduleConfig.MONTHLY, "WD-1", null, null, null), d(2026, 9, 15));
        ScheduleDecision wdZero   = source(schedule(RunScheduleConfig.MONTHLY, "WD+0", null, null, null), d(2026, 9, 15));

        assertEquals(Status.NOT_YET_ELIGIBLE, wdMinus1.status);
        assertEquals(d(2026, 9, 30), wdMinus1.freqRunDate);
        assertEquals(Status.ELIGIBLE, wdZero.status);
        assertEquals(d(2026, 8, 31), wdZero.freqRunDate);

        assertPeriod(wdMinus1, d(2026, 8, 1), d(2026, 8, 31), 202608);   // not null although skipped
        assertPeriod(wdZero,   d(2026, 8, 1), d(2026, 8, 31), 202608);
        assertEquals(d(2026, 9, 15), wdMinus1.dates.runDate);
        assertEquals(d(2026, 9, 15), wdZero.dates.runDate);
    }

    @Test
    void wdZeroWindowOpensOnTheFirstAndClosesAfterWd1() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD+0", "WD+1", null, null);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 9, 1)).status);   // max = Sep 1
        assertEquals(Status.EXPIRED,  source(s, d(2026, 9, 2)).status);
    }

    @Test
    void emptyWindowIsNeverEligible() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD+5", "WD+3", null, null);   // misconfigured
        for (int day = 1; day <= 30; day++) {
            ScheduleDecision dec = source(s, d(2026, 9, day));
            assertNotEquals(Status.ELIGIBLE, dec.status, "Sep " + day);
            assertNotNull(dec.dates);
        }
    }

    @Test
    void singleDayWindow() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+3", null, null);
        assertEquals(Status.NOT_YET_ELIGIBLE, source(s, d(2026, 9, 2)).status);
        assertEquals(Status.ELIGIBLE,         source(s, d(2026, 9, 3)).status);
        assertEquals(Status.EXPIRED,          source(s, d(2026, 9, 4)).status);
    }

    @Test
    void runDaysCountOnlyBusinessDays_goodFridayAndNewYear() {
        RunScheduleConfig april = schedule(RunScheduleConfig.MONTHLY, "WD+3", null, null, null);
        assertEquals(Status.NOT_YET_ELIGIBLE, source(april, d(2026, 4, 2)).status);
        ScheduleDecision goodFriday = source(april, d(2026, 4, 3));
        assertEquals(Status.NON_BUSINESS_DAY, goodFriday.status);        // Apr 1, Apr 2, [Apr 3 holiday], Apr 6
        assertEquals(d(2026, 4, 6), goodFriday.freqRunDate);
        assertEquals(Status.ELIGIBLE, source(april, d(2026, 4, 6)).status);

        RunScheduleConfig january = schedule(RunScheduleConfig.MONTHLY, "WD+1", null, null, null);
        ScheduleDecision newYear = source(january, d(2026, 1, 1));
        assertEquals(Status.NON_BUSINESS_DAY, newYear.status);           // Jan 1 is a holiday → WD+1 = Jan 2
        assertEquals(d(2026, 1, 2), newYear.freqRunDate);
        assertEquals(Status.ELIGIBLE, source(january, d(2026, 1, 2)).status);
        assertPeriod(source(january, d(2026, 1, 2)), d(2025, 12, 1), d(2025, 12, 31), 202512);
    }

    // ── quarterly / annual windows (assumption: window month = first month of the quarter / January) ──

    @Test
    void quarterlyWindowInTheFirstMonthOfTheQuarter() {
        RunScheduleConfig s = schedule(RunScheduleConfig.QUARTERLY, "WD+3", "WD+5", null, null);
        assertEquals(Status.NOT_YET_ELIGIBLE, source(s, d(2026, 4, 2)).status);
        ScheduleDecision goodFriday = source(s, d(2026, 4, 3));
        assertEquals(Status.NON_BUSINESS_DAY, goodFriday.status);
        assertPeriod(goodFriday, d(2026, 1, 1), d(2026, 3, 31), 2026010101);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 4, 6)).status);                  // WD+3
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 4, 8)).status);                  // WD+5
        assertEquals(Status.EXPIRED,  source(s, d(2026, 4, 9)).status);
        assertEquals(Status.EXPIRED,  source(s, d(2026, 5, 12)).status);                 // window month already past
        assertEquals(Status.NOT_YET_ELIGIBLE, source(s, d(2026, 7, 2)).status);          // new quarter, WD+3 = Jul 3
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 7, 3)).status);
    }

    @Test
    void annualWindowInJanuary() {
        RunScheduleConfig s = schedule(RunScheduleConfig.ANNUALLY, "WD+3", "WD+5", null, null);
        ScheduleDecision jan5 = source(s, d(2026, 1, 5));                                // WD+2
        assertEquals(Status.NOT_YET_ELIGIBLE, jan5.status);
        assertPeriod(jan5, d(2025, 1, 1), d(2025, 12, 31), 2025);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 1, 6)).status);                  // WD+3
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 1, 8)).status);                  // WD+5
        assertEquals(Status.EXPIRED,  source(s, d(2026, 1, 9)).status);
        assertEquals(Status.EXPIRED,  source(s, d(2026, 6, 1)).status);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // WHEN — DAILY business-day gate
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void dailyIgnoresTheWindowFieldsAndSkipsNonBusinessDays() {
        RunScheduleConfig s = schedule(RunScheduleConfig.DAILY, "WD+20", "WD+21", null, null);
        assertEquals(Status.NON_BUSINESS_DAY, source(s, d(2026, 9, 7)).status);          // holiday
        assertEquals(Status.NON_BUSINESS_DAY, source(s, d(2026, 9, 5)).status);          // Saturday
        ScheduleDecision tuesday = source(s, d(2026, 9, 2));
        assertEquals(Status.ELIGIBLE, tuesday.status);                                   // window ignored
        assertNull(tuesday.freqRunDate);
        assertNull(tuesday.maxFreqRunDate);
    }

    @Test
    void dailyMalformedWindowFieldsAreIgnored() {
        RunScheduleConfig s = schedule(RunScheduleConfig.DAILY, "garbage", "also garbage", null, null);
        assertEquals(Status.ELIGIBLE, source(s, d(2026, 9, 2)).status);
    }

    @Test
    void dailyEligibleExactlyOnBusinessDays_andPeriodIsTheBusinessDate() {
        RunScheduleConfig s = schedule(RunScheduleConfig.DAILY, null, null, null, null);
        for (int day = 1; day <= 30; day++) {
            LocalDate bd = d(2026, 9, day);
            ScheduleDecision dec = source(s, bd);
            assertEquals(CALENDAR.isBusinessDay(bd) ? Status.ELIGIBLE : Status.NON_BUSINESS_DAY, dec.status, "Sep " + day);
            assertPeriod(dec, bd, bd, 20260900 + day);
        }
    }

    @Test
    void dailySkippedDayStillCarriesThePeriodItWouldHaveProcessed() {
        ScheduleDecision saturday = source(schedule(RunScheduleConfig.DAILY, null, null, "WD-1", null), d(2026, 9, 5));
        assertEquals(Status.NON_BUSINESS_DAY, saturday.status);
        assertPeriod(saturday, d(2026, 9, 4), d(2026, 9, 4), 20260904);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // WHICH — DAILY lag
    // ══════════════════════════════════════════════════════════════════════════

    private static ScheduleDecision daily(String dayLag, LocalDate businessDate) {
        return source(schedule(RunScheduleConfig.DAILY, null, null, dayLag, null), businessDate);
    }

    @Test
    void dailyWorkdayLagSkipsHolidayAndWeekend() {
        ScheduleDecision d = daily("WD-1", d(2026, 9, 8));            // the documented BAU example
        assertPeriod(d, d(2026, 9, 4), d(2026, 9, 4), 20260904);
        assertEquals(d(2026, 9, 8), d.dates.runDate);                  // Business Date stays the run date
    }

    @Test
    void dailyWorkdayLagFromMondaySkipsTheWeekend() {
        assertPeriod(daily("WD-1", d(2026, 9, 14)), d(2026, 9, 11), d(2026, 9, 11), 20260911);
    }

    @Test
    void dailyWorkdayLagTwoAcrossAHoliday() {
        assertPeriod(daily("WD-2", d(2026, 9, 8)), d(2026, 9, 3), d(2026, 9, 3), 20260903);
    }

    @Test
    void dailyZeroLagIsTheBusinessDate() {
        assertPeriod(daily("WD-0", d(2026, 9, 9)), d(2026, 9, 9), d(2026, 9, 9), 20260909);
        assertPeriod(daily("CAL-0", d(2026, 9, 9)), d(2026, 9, 9), d(2026, 9, 9), 20260909);
    }

    @Test
    void dailyCalendarLagDoesNotAdjustForWeekends() {
        ScheduleDecision sunday = daily("CAL-1", d(2026, 9, 14));     // Monday → Sunday
        assertPeriod(sunday, d(2026, 9, 13), d(2026, 9, 13), 20260913);
        assertPeriod(daily("CAL-3", d(2026, 9, 14)), d(2026, 9, 11), d(2026, 9, 11), 20260911);
    }

    @Test
    void dailyLagAcrossYearEnd() {
        assertPeriod(daily("WD-1", d(2026, 1, 2)), d(2025, 12, 31), d(2025, 12, 31), 20251231);   // Jan 1 holiday
        assertPeriod(daily("WD-1", d(2026, 1, 5)), d(2026, 1, 2), d(2026, 1, 2), 20260102);
        assertPeriod(daily("WD-2", d(2026, 1, 5)), d(2025, 12, 31), d(2025, 12, 31), 20251231);
    }

    @Test
    void dailyLagAcrossMonthBoundariesAndLeapDay() {
        assertPeriod(daily("CAL-1", d(2026, 10, 1)), d(2026, 9, 30), d(2026, 9, 30), 20260930);
        assertPeriod(daily("CAL-5", d(2026, 3, 3)), d(2026, 2, 26), d(2026, 2, 26), 20260226);
        assertPeriod(daily("CAL-1", d(2028, 3, 1)), d(2028, 2, 29), d(2028, 2, 29), 20280229);
    }

    @Test
    void dailyWorkdayLagAroundThanksgivingAndGoodFriday() {
        assertPeriod(daily("WD-1", d(2026, 11, 27)), d(2026, 11, 25), d(2026, 11, 25), 20261125);
        assertPeriod(daily("WD-1", d(2026, 4, 6)), d(2026, 4, 2), d(2026, 4, 2), 20260402);
    }

    /** CONTRACT D4: a DAILY lag is WD-n / CAL-n; a positive lag is an error (filtered upstream, checked here). */
    @Test
    void dailyPositiveLagIsAnError_andNegativeLagIsTheValidForm() {
        ScheduleDecision minus = daily("WD-1", d(2026, 9, 9));
        assertEquals(Status.ELIGIBLE, minus.status);
        assertPeriod(minus, d(2026, 9, 8), d(2026, 9, 8), 20260908);

        for (String positive : new String[] {"WD+1", "CAL+1", "WD+0", "CAL+3"}) {
            ScheduleDecision dec = daily(positive, d(2026, 9, 9));
            assertEquals(Status.NOT_EVALUABLE, dec.status, positive);
            assertTrue(dec.detail.contains("positive"), dec.detail);
            assertFalse(dec.shouldRun());
        }
    }

    @Test
    void dailyMalformedLagsAreNotEvaluable() {
        for (String lag : new String[] {"CD-1", "wd-1", "cal-1", " WD-1", "WD-1 ", "WD-", "WD-x", "WD", "1", "WD-1.5"}) {
            assertEquals(Status.NOT_EVALUABLE, daily(lag, d(2026, 9, 9)).status, "dayLag='" + lag + "'");
        }
    }

    /** ASSUMPTION (contract Part 3 #3): blank DAILY dayLag = lag 0. */
    @Test
    void assumption_dailyBlankLagIsTheBusinessDate() {
        assertPeriod(daily(null, d(2026, 9, 9)), d(2026, 9, 9), d(2026, 9, 9), 20260909);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // WHICH — non-DAILY dayLag: only the prefix matters
    // ══════════════════════════════════════════════════════════════════════════

    private static ScheduleDecision monthly(String dayLag, LocalDate businessDate) {
        return source(schedule(RunScheduleConfig.MONTHLY, null, null, dayLag, null), businessDate);
    }

    @Test
    void nonDailyLagUsesOnlyThePrefix() {
        LocalDate bd = d(2026, 9, 3);
        for (String previous : new String[] {null, "WD+1", "WD+5", "CAL+1", "anything"}) {
            assertEquals(202608, monthly(previous, bd).dates.periodId, "dayLag=" + previous);
        }
        for (String current : new String[] {"WD-1", "WD-5", "CAL-3", "WD-0", "CAL-0", "WD-"}) {
            assertEquals(202609, monthly(current, bd).dates.periodId, "dayLag=" + current);
        }
    }

    /** ASSUMPTION (contract Part 3 #5): prefixes are matched exactly, case-sensitively. */
    @Test
    void assumption_nonDailyLagPrefixIsCaseSensitive() {
        assertEquals(202608, monthly("wd-1", d(2026, 9, 3)).dates.periodId);
    }

    @Test
    void monthlyPeriodBoundaries() {
        assertPeriod(monthly(null, d(2026, 9, 3)), d(2026, 8, 1), d(2026, 8, 31), 202608);
        assertPeriod(monthly("WD-1", d(2026, 9, 3)), d(2026, 9, 1), d(2026, 9, 30), 202609);
        assertPeriod(monthly(null, d(2026, 4, 3)), d(2026, 3, 1), d(2026, 3, 31), 202603);   // two-digit month
        assertPeriod(monthly(null, d(2026, 5, 1)), d(2026, 4, 1), d(2026, 4, 30), 202604);   // 30-day month
    }

    @Test
    void monthlyPreviousPeriodAcrossTheYearBoundary() {
        assertPeriod(monthly(null, d(2026, 1, 5)), d(2025, 12, 1), d(2025, 12, 31), 202512);
        assertPeriod(monthly("WD-1", d(2026, 1, 5)), d(2026, 1, 1), d(2026, 1, 31), 202601);
        assertPeriod(monthly("WD-1", d(2026, 12, 31)), d(2026, 12, 1), d(2026, 12, 31), 202612);
    }

    @Test
    void monthlyFebruaryInLeapAndNonLeapYears() {
        assertPeriod(monthly(null, d(2028, 3, 2)), d(2028, 2, 1), d(2028, 2, 29), 202802);
        assertPeriod(monthly(null, d(2026, 3, 3)), d(2026, 2, 1), d(2026, 2, 28), 202602);
    }

    @Test
    void quarterlyPeriodIdIsFirstDateOfQuarterPlusQuarterNumber() {
        assertEquals(2026010101, RunDateCalculator.quarterPeriodId(d(2026, 1, 1), 1));
        assertEquals(2026040102, RunDateCalculator.quarterPeriodId(d(2026, 4, 1), 2));
        assertEquals(2026070103, RunDateCalculator.quarterPeriodId(d(2026, 7, 1), 3));
        assertEquals(2026100104, RunDateCalculator.quarterPeriodId(d(2026, 10, 1), 4));
    }

    private static ScheduleDecision quarterly(String dayLag, LocalDate businessDate) {
        return source(schedule(RunScheduleConfig.QUARTERLY, null, null, dayLag, null), businessDate);
    }

    @Test
    void quarterlyPreviousAndCurrentPeriods() {
        assertPeriod(quarterly(null,   d(2026, 4, 3)),  d(2026, 1, 1),  d(2026, 3, 31), 2026010101);   // Q1
        assertPeriod(quarterly("WD-1", d(2026, 4, 3)),  d(2026, 4, 1),  d(2026, 6, 30), 2026040102);   // Q2
        assertPeriod(quarterly(null,   d(2026, 7, 2)),  d(2026, 4, 1),  d(2026, 6, 30), 2026040102);
        assertPeriod(quarterly(null,   d(2026, 10, 2)), d(2026, 7, 1),  d(2026, 9, 30), 2026070103);   // third quarter
        assertPeriod(quarterly("WD-1", d(2026, 11, 2)), d(2026, 10, 1), d(2026, 12, 31), 2026100104);  // Q4
    }

    @Test
    void quarterlyAcrossTheYearBoundaryAndOnQuarterEnd() {
        assertPeriod(quarterly(null, d(2026, 1, 5)),   d(2025, 10, 1), d(2025, 12, 31), 2025100104);
        assertPeriod(quarterly(null, d(2026, 3, 31)),  d(2025, 10, 1), d(2025, 12, 31), 2025100104);   // still in Q1
        assertPeriod(quarterly("WD-1", d(2026, 3, 31)), d(2026, 1, 1), d(2026, 3, 31), 2026010101);
        assertPeriod(quarterly(null, d(2026, 4, 1)),   d(2026, 1, 1),  d(2026, 3, 31), 2026010101);
    }

    private static ScheduleDecision annually(String dayLag, LocalDate businessDate) {
        return source(schedule(RunScheduleConfig.ANNUALLY, null, null, dayLag, null), businessDate);
    }

    @Test
    void annualPreviousAndCurrentPeriods() {
        assertPeriod(annually(null, d(2026, 1, 5)),    d(2025, 1, 1), d(2025, 12, 31), 2025);
        assertPeriod(annually("CAL-1", d(2026, 1, 5)), d(2026, 1, 1), d(2026, 12, 31), 2026);
        assertPeriod(annually(null, d(2026, 12, 31)),  d(2025, 1, 1), d(2025, 12, 31), 2025);
        assertPeriod(annually("WD-1", d(2026, 12, 31)), d(2026, 1, 1), d(2026, 12, 31), 2026);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // WHAT — data-source dateType (period end), MONTHLY only
    // ══════════════════════════════════════════════════════════════════════════

    private static LocalDate monthEnd(String dateType, LocalDate businessDate) {
        return source(schedule(RunScheduleConfig.MONTHLY, null, null, null, dateType), businessDate).dates.periodEnd;
    }

    @Test
    void lastBusDayMonthMovesMonthEndOffTheWeekend() {
        LocalDate bd = d(2026, 2, 3);   // processes January 2026; Jan 31 is a Saturday
        assertEquals(d(2026, 1, 30), monthEnd(RunScheduleConfig.LAST_BUS_DAY_MONTH, bd));
        assertEquals(d(2026, 1, 31), monthEnd(RunScheduleConfig.LAST_DAY_MONTH, bd));
        assertEquals(d(2026, 1, 31), monthEnd("somethingElse", bd));
        assertEquals(d(2026, 1, 31), monthEnd(null, bd));
    }

    @Test
    void lastBusDayMonthForEveryKindOfMonthEnd() {
        String lbd = RunScheduleConfig.LAST_BUS_DAY_MONTH;
        assertEquals(d(2026, 2, 27), monthEnd(lbd, d(2026, 3, 3)));    // Feb 28 Saturday
        assertEquals(d(2026, 5, 29), monthEnd(lbd, d(2026, 6, 2)));    // May 30 Sat + May 31 Sun
        assertEquals(d(2026, 6, 29), monthEnd(lbd, d(2026, 7, 2)));    // Jun 30 is a holiday
        assertEquals(d(2026, 9, 30), monthEnd(lbd, d(2026, 10, 2)));   // Sep 30 is already a business day
        assertEquals(d(2025, 12, 31), monthEnd(lbd, d(2026, 1, 5)));   // year end, business day
        assertEquals(d(2028, 2, 29), monthEnd(lbd, d(2028, 3, 2)));    // leap day, business day
        assertEquals(d(2026, 6, 30), monthEnd(RunScheduleConfig.LAST_DAY_MONTH, d(2026, 7, 2)));
    }

    @Test
    void lastBusDayMonthAppliesToTheCurrentPeriodToo() {
        ScheduleDecision may = source(schedule(RunScheduleConfig.MONTHLY, null, null, "WD-1",
            RunScheduleConfig.LAST_BUS_DAY_MONTH), d(2026, 5, 15));
        assertPeriod(may, d(2026, 5, 1), d(2026, 5, 29), 202605);
    }

    @Test
    void dateTypeIsIgnoredForDailyQuarterlyAndAnnual() {
        String lbd = RunScheduleConfig.LAST_BUS_DAY_MONTH;
        // QUARTERLY: Q2 ends on the holiday Jun 30 — dateType does not move it (MONTHLY June would give Jun 29).
        assertEquals(d(2026, 6, 30), source(schedule(RunScheduleConfig.QUARTERLY, null, null, null, lbd),
            d(2026, 7, 2)).dates.periodEnd);
        assertEquals(d(2025, 12, 31), source(schedule(RunScheduleConfig.ANNUALLY, null, null, null, lbd),
            d(2026, 1, 5)).dates.periodEnd);
        assertEquals(d(2026, 9, 9), source(schedule(RunScheduleConfig.DAILY, null, null, null, lbd),
            d(2026, 9, 9)).dates.periodEnd);
    }

    @Test
    void dateTypeNeverAffectsEligibilityOrPeriodStart() {
        for (int day = 1; day <= 30; day++) {
            LocalDate bd = d(2026, 9, day);
            ScheduleDecision a = source(schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+5", null, null), bd);
            ScheduleDecision b = source(schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+5", null,
                RunScheduleConfig.LAST_BUS_DAY_MONTH), bd);
            assertEquals(a.status, b.status, "Sep " + day);
            assertEquals(a.dates.periodStart, b.dates.periodStart);
            assertEquals(a.dates.periodId, b.dates.periodId);
        }
    }

    @Test
    void reportIgnoresDateType() {
        ScheduleDecision d = report(schedule(RunScheduleConfig.MONTHLY, null, null, null,
            RunScheduleConfig.LAST_BUS_DAY_MONTH), d(2026, 2, 3));
        assertPeriod(d, d(2026, 1, 1), d(2026, 1, 31), 202601);          // not Jan 30
        assertEquals(d(2026, 6, 30), report(schedule(RunScheduleConfig.MONTHLY, null, null, null,
            RunScheduleConfig.LAST_BUS_DAY_MONTH), d(2026, 7, 2)).dates.periodEnd);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Reports and data sources: scheduled independently, agree on the period
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void reportAndDataSourceHaveIndependentWindows() {
        RunScheduleConfig src = schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+5", null, null);
        RunScheduleConfig rpt = schedule(RunScheduleConfig.MONTHLY, "WD+6", "WD+8", null, null);

        // Fri Sep 4: the source is inside its window, the report's (opens Sep 9) is not.
        assertEquals(Status.ELIGIBLE, source(src, d(2026, 9, 4)).status);
        assertEquals(Status.NOT_YET_ELIGIBLE, report(rpt, d(2026, 9, 4)).status);
        // Wed Sep 9: the source window has closed, the report has opened.
        assertEquals(Status.EXPIRED, source(src, d(2026, 9, 9)).status);
        assertEquals(Status.ELIGIBLE, report(rpt, d(2026, 9, 9)).status);
        assertEquals(d(2026, 9, 9), report(rpt, d(2026, 9, 9)).freqRunDate);
    }

    @Test
    void reportAndDataSourceWithTheSameFrequencyAndLagAlwaysAgreeOnThePeriodId() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, null, null, null, null);
        RunScheduleConfig current = schedule(RunScheduleConfig.MONTHLY, null, null, "WD-1", null);
        RunScheduleConfig quarterly = schedule(RunScheduleConfig.QUARTERLY, null, null, null, null);
        for (LocalDate bd = d(2025, 12, 1); !bd.isAfter(d(2026, 12, 31)); bd = bd.plusDays(1)) {
            for (RunScheduleConfig cfg : List.of(s, current, quarterly)) {
                assertEquals(source(cfg, bd).dates.periodId, report(cfg, bd).dates.periodId, bd + " " + cfg);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NOT_EVALUABLE — bad configuration or calendar; never guessed
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void unsupportedOrMissingFrequencyIsNotEvaluable() {
        LocalDate bd = d(2026, 9, 3);
        for (String frequency : new String[] {"WEEKLY", "monthly", "ANNUAL", "YEARLY", "Daily", null}) {
            ScheduleDecision dec = source(schedule(frequency, "WD+1", null, null, null), bd);
            assertEquals(Status.NOT_EVALUABLE, dec.status, "frequency=" + frequency);
            assertNull(dec.dates);
        }
    }

    @Test
    void missingOrUnknownCalendarKeyIsNotEvaluable() {
        LocalDate bd = d(2026, 9, 3);
        assertEquals(Status.NOT_EVALUABLE, source(new RunScheduleConfig(RunScheduleConfig.MONTHLY,
            null, null, null, null, null), bd).status);
        assertEquals(Status.NOT_EVALUABLE, source(new RunScheduleConfig(RunScheduleConfig.DAILY,
            null, null, null, null, null), bd).status);
        assertEquals(Status.NOT_EVALUABLE, source(new RunScheduleConfig(RunScheduleConfig.MONTHLY,
            null, null, null, null, "NOPE"), bd).status);
    }

    @Test
    void malformedRunDaysAreNotEvaluableButTheCalculatedPeriodIsStillShown() {
        for (String bad : new String[] {"WD+x", "CD+3", "3", "wd+1", "WD", "WD+1 ", "+1"}) {
            ScheduleDecision freq = source(schedule(RunScheduleConfig.MONTHLY, bad, null, null, null), d(2026, 9, 3));
            assertEquals(Status.NOT_EVALUABLE, freq.status, "freqRunDay='" + bad + "'");
            assertEquals(202608, freq.dates.periodId);
            ScheduleDecision max = source(schedule(RunScheduleConfig.MONTHLY, null, bad, null, null), d(2026, 9, 3));
            assertEquals(Status.NOT_EVALUABLE, max.status, "maxFreqRunDay='" + bad + "'");
        }
    }

    @Test
    void aBareNumberMaxFreqRunDayIsNotEvaluable() {
        assertEquals(Status.NOT_EVALUABLE,
            source(schedule(RunScheduleConfig.MONTHLY, "WD+1", "5", null, null), d(2026, 9, 3)).status);
    }

    @Test
    void aCalendarThatFailsIsNotEvaluable() {
        BusinessCalendarProvider broken = key -> date -> { throw new IllegalStateException("calendar DB down"); };
        ScheduleDecision dec = RunDateCalculator.evaluate(
            schedule(RunScheduleConfig.DAILY, null, null, null, null), d(2026, 9, 3), ItemType.DATA_SOURCE, broken);
        assertEquals(Status.NOT_EVALUABLE, dec.status);
        assertTrue(dec.detail.contains("calendar DB down"), dec.detail);
    }

    @Test
    void aCalendarWithNoBusinessDaysNeverHangs() {
        BusinessCalendarProvider closed = key -> date -> false;
        // DAILY, no lag: nothing to count, and today is simply not a business day.
        assertEquals(Status.NON_BUSINESS_DAY, RunDateCalculator.evaluate(
            schedule(RunScheduleConfig.DAILY, null, null, null, null), d(2026, 9, 3), ItemType.DATA_SOURCE, closed).status);
        // A WD run day can never be located.
        ScheduleDecision dec = RunDateCalculator.evaluate(
            schedule(RunScheduleConfig.MONTHLY, "WD+1", null, null, null), d(2026, 9, 3), ItemType.DATA_SOURCE, closed);
        assertEquals(Status.NOT_EVALUABLE, dec.status);
        assertTrue(dec.detail.contains("no business days"), dec.detail);
        // ...and neither can a business-day lag, or a last-business-day month end.
        assertEquals(Status.NOT_EVALUABLE, RunDateCalculator.evaluate(
            schedule(RunScheduleConfig.DAILY, null, null, "WD-1", null), d(2026, 9, 3), ItemType.DATA_SOURCE, closed).status);
        assertEquals(Status.NOT_EVALUABLE, RunDateCalculator.evaluate(
            schedule(RunScheduleConfig.MONTHLY, null, null, null, RunScheduleConfig.LAST_BUS_DAY_MONTH),
            d(2026, 9, 3), ItemType.DATA_SOURCE, closed).status);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Options-based entry point: Business Date from --runDate, period id optional
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void scheduledItemNeedsNoPeriodId_itIsCalculatedFromTheRunDate() {
        ScheduleDecision dec = source(MONTHLY_WD3_TO_WD5, options("2026-09-03", 0));
        assertEquals(Status.ELIGIBLE, dec.status);
        assertPeriod(dec, d(2026, 8, 1), d(2026, 8, 31), 202608);
        assertEquals(d(2026, 9, 3), dec.dates.runDate);
    }

    @Test
    void scheduledItemIgnoresAPeriodIdPassedOnTheCommandLine() {
        ScheduleDecision dec = source(MONTHLY_WD3_TO_WD5, options("2026-09-03", 999999));
        assertEquals(202608, dec.dates.periodId);
    }

    @Test
    void theBusinessDateComesFromRunDate() {
        assertEquals(Status.NOT_YET_ELIGIBLE, source(MONTHLY_WD3_TO_WD5, options("2026-09-02", 0)).status);
        assertEquals(Status.ELIGIBLE,         source(MONTHLY_WD3_TO_WD5, options("2026-09-03", 0)).status);
        assertEquals(Status.EXPIRED,          source(MONTHLY_WD3_TO_WD5, options("2026-09-09", 0)).status);
    }

    /** CONTRACT D5: an item with no run schedule is not processed — there is no fallback to CLI dates. */
    @Test
    void anItemWithNoScheduleIsNotProcessed_whateverTheCommandLineSays() {
        FrameworkOptions full = options("2024-01-31", 202401);
        full.setPeriodStart("2024-01-01");
        full.setPeriodEnd("2024-01-31");
        for (FrameworkOptions o : List.of(full, options("2026-09-03", 0))) {
            for (ItemType type : ItemType.values()) {
                ScheduleDecision dec = RunDateCalculator.evaluate(RunScheduleConfig.none(), o, type, PROVIDER);
                assertEquals(Status.NOT_EVALUABLE, dec.status, type + " " + o.getRunDate());
                assertFalse(dec.shouldRun());
                assertNull(dec.dates);
                assertTrue(dec.detail.contains("no run schedule"), dec.detail);
            }
        }
        // ...also through the pure entry point
        assertEquals(Status.NOT_EVALUABLE, source(RunScheduleConfig.none(), d(2026, 9, 3)).status);
        assertEquals(Status.NOT_EVALUABLE, report(RunScheduleConfig.none(), d(2026, 9, 3)).status);
    }

    /** CONTRACT D5: a calendar must exist — a schedule alone is not enough. */
    @Test
    void aScheduleWithoutAnExistingCalendarIsNotProcessed() {
        LocalDate bd = d(2026, 9, 3);
        // only a calendarKey, no frequency
        assertEquals(Status.NOT_EVALUABLE, source(new RunScheduleConfig(null, null, null, null, null, "CAL_US"), bd).status);
        // schedule but no calendarKey / unknown calendarKey (also covered in missingOrUnknownCalendarKeyIsNotEvaluable)
        assertEquals(Status.NOT_EVALUABLE, source(new RunScheduleConfig(
            RunScheduleConfig.MONTHLY, "WD+1", null, null, null, null), bd).status);
        assertEquals(Status.NOT_EVALUABLE, report(new RunScheduleConfig(
            RunScheduleConfig.MONTHLY, "WD+1", null, null, null, "NOPE"), bd).status);
    }

    /** CONTRACT Part 2, D1: --manualOverrun is only about storage — it never changes eligibility or dates. */
    @Test
    void manualOverrunNeverChangesEligibilityOrDates() {
        String[] runDates = {"2026-09-02", "2026-09-03", "2026-09-05", "2026-09-08", "2026-09-09", "2026-09-30"};
        for (String runDate : runDates) {
            FrameworkOptions off = options(runDate, 0);
            FrameworkOptions on  = options(runDate, 0);
            on.setManualOverrun(true);
            ScheduleDecision a = source(MONTHLY_WD3_TO_WD5, off);
            ScheduleDecision b = source(MONTHLY_WD3_TO_WD5, on);
            assertEquals(a.status, b.status, runDate);
            assertEquals(a.dates.periodId, b.dates.periodId, runDate);
            assertEquals(a.freqRunDate, b.freqRunDate, runDate);
            assertEquals(a.maxFreqRunDate, b.maxFreqRunDate, runDate);
        }
        FrameworkOptions late = options("2026-09-09", 0);
        late.setManualOverrun(true);
        assertEquals(Status.EXPIRED, source(MONTHLY_WD3_TO_WD5, late).status);          // not bypassed
        FrameworkOptions early = options("2026-09-02", 0);
        early.setManualOverrun(true);
        assertEquals(Status.NOT_YET_ELIGIBLE, source(MONTHLY_WD3_TO_WD5, early).status);
        FrameworkOptions holiday = options("2026-09-07", 0);
        holiday.setManualOverrun(true);
        assertEquals(Status.NON_BUSINESS_DAY, source(
            schedule(RunScheduleConfig.DAILY, null, null, null, null), holiday).status);
    }

    @Test
    void anInvalidRunDateFailsLoudly() {
        assertThrows(IllegalArgumentException.class, () -> source(MONTHLY_WD3_TO_WD5, options("2026-13-01", 0)));
    }

    @Test
    void whenTheCalendarCannotBeLoadedNothingIsProcessed() {
        // e.g. the calendar row is missing from the parameter table, or BigQuery is unavailable.
        BusinessCalendarProvider unavailable = key -> {
            throw new IllegalArgumentException("no calendar '" + key + "' in the parameter table");
        };
        ScheduleDecision scheduled = RunDateCalculator.evaluate(
            MONTHLY_WD3_TO_WD5, options("2026-09-03", 0), ItemType.DATA_SOURCE, unavailable);
        assertEquals(Status.NOT_EVALUABLE, scheduled.status);
        assertTrue(scheduled.detail.contains("CAL_US"), scheduled.detail);

        ScheduleDecision unscheduled = RunDateCalculator.evaluate(
            RunScheduleConfig.none(), options("2026-09-03", 202609), ItemType.REPORT, unavailable);
        assertEquals(Status.NOT_EVALUABLE, unscheduled.status);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Invariants over a year of days × many schedules, both item types
    // ══════════════════════════════════════════════════════════════════════════

    private static final List<RunScheduleConfig> SWEEP_SCHEDULES = List.of(
        schedule(RunScheduleConfig.DAILY, null, null, null, null),
        schedule(RunScheduleConfig.DAILY, null, null, "WD-1", null),
        schedule(RunScheduleConfig.DAILY, null, null, "CAL-2", null),
        schedule(RunScheduleConfig.MONTHLY, "WD+3", "WD+5", null, null),
        schedule(RunScheduleConfig.MONTHLY, "WD-3", "WD-1", "WD-1", null),
        schedule(RunScheduleConfig.MONTHLY, "WD+0", null, null, RunScheduleConfig.LAST_BUS_DAY_MONTH),
        schedule(RunScheduleConfig.MONTHLY, null, null, null, null),
        schedule(RunScheduleConfig.QUARTERLY, "WD+1", "WD+10", null, null),
        schedule(RunScheduleConfig.QUARTERLY, null, null, "WD-1", null),
        schedule(RunScheduleConfig.ANNUALLY, "WD+1", "WD+20", "CAL-1", null),
        schedule(RunScheduleConfig.ANNUALLY, null, null, null, null));

    @Test
    void everyEvaluableDecisionCarriesAConsistentPeriod_andEligibilityMatchesTheDefinition() {
        for (RunScheduleConfig cfg : SWEEP_SCHEDULES) {
            for (ItemType type : ItemType.values()) {
                for (LocalDate bd = d(2025, 12, 15); !bd.isAfter(d(2026, 12, 31)); bd = bd.plusDays(1)) {
                    ScheduleDecision dec = RunDateCalculator.evaluate(cfg, bd, type, PROVIDER);
                    String ctx = type + " " + cfg + " on " + bd + " → " + dec;

                    assertNotEquals(Status.NOT_EVALUABLE, dec.status, ctx);   // all sweep schedules are valid
                    RunDates dates = dec.dates;
                    assertNotNull(dates, ctx);                                // period stored whatever the status
                    assertEquals(bd, dates.runDate, ctx);
                    assertTrue(dates.periodId > 0, ctx);
                    assertNotNull(dates.periodStart, ctx);
                    assertNotNull(dates.periodEnd, ctx);
                    assertFalse(dates.periodStart.isAfter(dates.periodEnd), ctx);

                    // D6: every frequency needs a business day; non-DAILY also needs to be inside its window
                    boolean eligible = CALENDAR.isBusinessDay(bd);
                    if (RunScheduleConfig.DAILY.equals(cfg.frequency)) {
                        assertNull(dec.freqRunDate, ctx);
                        assertNull(dec.maxFreqRunDate, ctx);
                    } else {
                        eligible = eligible
                                && (dec.freqRunDate == null || !bd.isBefore(dec.freqRunDate))
                                && (dec.maxFreqRunDate == null || !bd.isAfter(dec.maxFreqRunDate));
                    }
                    assertEquals(eligible, dec.shouldRun(), ctx);
                    if (!CALENDAR.isBusinessDay(bd)) {
                        assertEquals(Status.NON_BUSINESS_DAY, dec.status, ctx);
                    }
                }
            }
        }
    }

    @Test
    void theMonthlyPeriodIdOnlyChangesOnTheFirstOfAMonth() {
        RunScheduleConfig s = schedule(RunScheduleConfig.MONTHLY, null, null, null, null);
        int previous = source(s, d(2025, 12, 1)).dates.periodId;
        for (LocalDate bd = d(2025, 12, 2); !bd.isAfter(d(2026, 12, 31)); bd = bd.plusDays(1)) {
            int current = source(s, bd).dates.periodId;
            if (bd.getDayOfMonth() == 1) {
                assertTrue(current > previous, "period advances on " + bd);
            } else {
                assertEquals(previous, current, "period unchanged on " + bd);
            }
            previous = current;
        }
    }

    // ── Report lookback (owner request 2026-10-05; contract Part 3 #9) ────────

    private static List<Integer> lookback(RunScheduleConfig dataSource, RunScheduleConfig report,
                                          LocalDate businessDate, int from, int to) {
        RunDates dates = RunDateCalculator.calculateLastPeriod(report, businessDate, CALENDAR);
        return RunDateCalculator.lookbackPeriodIds(dataSource, report, dates, from, to, CALENDAR);
    }

    @Test
    void monthlyLookbackZeroToMinus11IsTwelveMonthsEndingAtTheReportPeriod() {
        RunScheduleConfig m = schedule("MONTHLY", "WD+3", "WD+5", "", "Calendar_EPS");
        // Business date Sep 2026, blank dayLag → report period Aug 2026
        List<Integer> ids = lookback(m, m, d(2026, 9, 8), 0, -11);
        assertEquals(12, ids.size());
        assertEquals(202608, ids.get(0));      // offset 0 = report's own period
        assertEquals(202607, ids.get(1));
        assertEquals(202512, ids.get(8));      // crosses the year boundary
        assertEquals(202509, ids.get(11));     // offset -11
    }

    @Test
    void lookbackRangeNeedNotStartAtZero() {
        RunScheduleConfig m = schedule("MONTHLY", "WD+3", "WD+5", "", "Calendar_EPS");
        assertEquals(List.of(202607, 202606), lookback(m, m, d(2026, 9, 8), -1, -2));
        assertEquals(List.of(202608), lookback(m, m, d(2026, 9, 8), 0, 0));
    }

    @Test
    void quarterlyLookbackStepsWholeQuartersWithTheYyyyMMddqqId() {
        RunScheduleConfig q = schedule("QUARTERLY", "WD+3", "WD+5", "", "Calendar_EPS");
        // Business date Oct 2026 → previous quarter Q3 2026 = 2026070103
        assertEquals(List.of(2026070103, 2026040102, 2026010101, 2025100104),
            lookback(q, q, d(2026, 10, 8), 0, -3));
    }

    @Test
    void annualLookbackStepsYears() {
        RunScheduleConfig a = schedule("ANNUALLY", "WD+3", "WD+5", "", "Calendar_EPS");
        assertEquals(List.of(2025, 2024, 2023), lookback(a, a, d(2026, 1, 8), 0, -2));
    }

    @Test
    void dailyLookbackStepsBusinessDaysForWdAndCalendarDaysForCal() {
        // Tue 2026-09-08, Mon 09-07 is a holiday. WD-0 → period = 09-08.
        RunScheduleConfig wd = schedule("DAILY", null, null, "WD-0", "Calendar_EPS");
        assertEquals(List.of(20260908, 20260904, 20260903), lookback(wd, wd, d(2026, 9, 8), 0, -2));
        RunScheduleConfig cal = schedule("DAILY", null, null, "CAL-0", "Calendar_EPS");
        assertEquals(List.of(20260908, 20260907, 20260906), lookback(cal, cal, d(2026, 9, 8), 0, -2));
    }

    @Test
    void lookbackIsRefusedWhenDataSourceAndReportFrequenciesDiffer() {
        RunScheduleConfig m = schedule("MONTHLY", "WD+3", "WD+5", "", "Calendar_EPS");
        RunScheduleConfig q = schedule("QUARTERLY", "WD+3", "WD+5", "", "Calendar_EPS");
        RunDates dates = RunDateCalculator.calculateLastPeriod(m, d(2026, 9, 8), CALENDAR);
        assertThrows(IllegalArgumentException.class,
            () -> RunDateCalculator.lookbackPeriodIds(q, m, dates, 0, -3, CALENDAR));
    }

    private static void assertNotEquals(Object unexpected, Object actual, String message) {
        assertFalse(unexpected.equals(actual), message);
    }
}
