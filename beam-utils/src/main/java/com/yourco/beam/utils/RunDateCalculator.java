package com.yourco.beam.utils;

import com.yourco.beam.model.RunScheduleConfig;

import java.time.LocalDate;

/**
 * Computes the effective run date for a data source from its {@link RunScheduleConfig}.
 *
 * <p>This is a thin wrapper around whatever business-date arithmetic an operator's own calendar
 * rules require — it retrieves nothing itself (that's already done by the time a
 * {@link RunScheduleConfig} reaches here) and performs no I/O of its own. What it must do,
 * combining the fields on {@link RunScheduleConfig}:
 * <ol>
 *   <li>Resolve the period containing {@code asOfDate} according to {@link RunScheduleConfig#frequency}
 *       (DAILY/WEEKLY/MONTHLY/QUARTERLY/YEARLY).</li>
 *   <li>Land on the specific date within that period per {@link RunScheduleConfig#dateType}
 *       (e.g. {@code LAST_DAY_OF_MONTH}) and/or {@link RunScheduleConfig#freqRunDay} (the
 *       {@code WD+n}/{@code CD+n} offset DSL — a workday or calendar-day count from the start of
 *       the period).</li>
 *   <li>For {@link RunScheduleConfig#DAILY} sources, instead apply {@link RunScheduleConfig#dayLag}
 *       (the same {@code WD-n}/{@code CD-n} DSL, counted backward from {@code asOfDate}).</li>
 *   <li>Resolve any {@code WD}/business-day-aware offset against the calendar identified by
 *       {@link RunScheduleConfig#calendarKey} — fetched from an external calendar database. This
 *       is a different data source from {@code FrameworkOptions.getCalendarName()}/
 *       {@code CalendarUtils}, which stub a small set of framework-wide named calendars; a
 *       {@code calendarKey} is a per-source lookup key into a separate calendar system.</li>
 *   <li>If {@link RunScheduleConfig#hasMaxRunDayCheck()}, validate the resolved date falls within
 *       {@link RunScheduleConfig#maxFreqRunDay} days of the period start (or whatever "stale run"
 *       definition applies) — the caller decides what to do with a violation (fail the run, log
 *       a warning, etc.); this method's contract is only to compute the date.</li>
 * </ol>
 *
 * <p><b>Not implemented.</b> Integrate with your calendar database and business-date rules —
 * see {@link CalendarUtils} for the equivalent stub pattern already used elsewhere in this
 * framework for named-calendar business-day arithmetic.
 *
 * <h2>Example usage (once implemented)</h2>
 * <pre>{@code
 * RunScheduleConfig schedule = sourceConfig.runScheduleConfig;
 * if (schedule.hasSchedule()) {
 *     LocalDate runDate = RunDateCalculator.calculateRunDate(schedule, LocalDate.now());
 * }
 * }</pre>
 */
public final class RunDateCalculator {

    private RunDateCalculator() {}

    /**
     * Computes the run date for a source given its {@link RunScheduleConfig}, as of a reference
     * date (typically "today", or {@code DateUtils.resolveRunDate(options)}).
     *
     * <p><b>Not implemented.</b> See class Javadoc for what this method needs to do.
     *
     * @param scheduleConfig the source's retrieved run-scheduling values; never null, but may be
     *                       {@link RunScheduleConfig#none()} (no schedule configured) — callers
     *                       should check {@link RunScheduleConfig#hasSchedule()} before calling
     * @param asOfDate       the reference date to resolve the schedule relative to
     * @return the resolved run date
     * @throws UnsupportedOperationException until implemented
     */
    public static LocalDate calculateRunDate(RunScheduleConfig scheduleConfig, LocalDate asOfDate) {
        throw new UnsupportedOperationException(
            "RunDateCalculator.calculateRunDate() is not yet implemented. "
            + "Integrate with your calendar database and business-date rules. "
            + "scheduleConfig=" + scheduleConfig + ", asOfDate=" + asOfDate);
    }
}
