package com.yourco.beam.utils;

import com.yourco.beam.options.FrameworkOptions;

import java.time.LocalDate;
import java.util.List;

/**
 * Business calendar utilities for report pipelines.
 *
 * <p>These methods determine working days, business day offsets, and
 * date ranges relative to a given business calendar. They are used by
 * report pipelines that must only process data on trading/banking days
 * and need to compute "previous business day" or "T-1" dates.
 *
 * <h2>Configuration</h2>
 * The calendar to use is driven by {@code FrameworkOptions.getCalendarName()}.
 * Supported calendar names are defined per implementation, e.g.:
 * <ul>
 *   <li>{@code DEFAULT}    — Monday to Friday, no public holidays</li>
 *   <li>{@code NYSE}       — New York Stock Exchange trading calendar</li>
 *   <li>{@code LSE}        — London Stock Exchange trading calendar</li>
 *   <li>{@code UK_BANKING} — UK banking days (includes Bank Holidays)</li>
 *   <li>{@code IN_NSE}     — National Stock Exchange of India</li>
 * </ul>
 *
 * <h2>Implementation note</h2>
 * These methods are stubs. Implement them by integrating with:
 * <ul>
 *   <li>Your company's internal holiday/calendar service or database table</li>
 *   <li>A public holidays API (e.g., Nager.Date, Holiday API)</li>
 *   <li>A Java calendar library such as <a href="https://www.joda.org/joda-time/">Joda-Time</a>
 *       or <a href="https://github.com/finmath/finmath-lib">finmath-lib</a> which includes
 *       financial business-day conventions</li>
 * </ul>
 *
 * <h2>Example usage</h2>
 * <pre>{@code
 * // In a transform or PipelineFactory:
 * LocalDate runDate   = DateUtils.resolveRunDate(options);
 * LocalDate bizDate   = CalendarUtils.applyOffset(runDate,
 *                           options.getBusinessDayOffset(),
 *                           options.getCalendarName());
 * boolean isValid     = CalendarUtils.isBusinessDay(bizDate, options.getCalendarName());
 * LocalDate prevBizDay = CalendarUtils.previousBusinessDay(bizDate, options.getCalendarName());
 * }</pre>
 */
public final class CalendarUtils {

    private CalendarUtils() {}

    // =========================================================================
    // BUSINESS DAY CHECKS
    // =========================================================================

    /**
     * Returns {@code true} if the given date is a business day in the specified calendar.
     *
     * <p><b>Not implemented.</b> Integrate with your calendar service or holiday database.
     *
     * @param date         the date to check
     * @param calendarName the business calendar to use (see class Javadoc for supported values)
     * @return {@code true} if {@code date} is a working day in this calendar
     * @throws UnsupportedOperationException until implemented
     */
    public static boolean isBusinessDay(LocalDate date, String calendarName) {
        throw new UnsupportedOperationException(
            "CalendarUtils.isBusinessDay() is not yet implemented. "
            + "Integrate with your holiday/calendar service and implement this method. "
            + "Calendar: " + calendarName + ", Date: " + date);
    }

    /**
     * Returns {@code true} if the given date is a weekend day (Saturday or Sunday).
     * This is calendar-independent and fully implemented.
     */
    public static boolean isWeekend(LocalDate date) {
        return switch (date.getDayOfWeek()) {
            case SATURDAY, SUNDAY -> true;
            default               -> false;
        };
    }

    // =========================================================================
    // BUSINESS DAY NAVIGATION
    // =========================================================================

    /**
     * Returns the next business day after the given date in the specified calendar.
     *
     * <p><b>Not implemented.</b> Integrate with your calendar service.
     *
     * @param date         the reference date
     * @param calendarName the business calendar to use
     * @return the first business day strictly after {@code date}
     * @throws UnsupportedOperationException until implemented
     */
    public static LocalDate nextBusinessDay(LocalDate date, String calendarName) {
        throw new UnsupportedOperationException(
            "CalendarUtils.nextBusinessDay() is not yet implemented. "
            + "Calendar: " + calendarName + ", Date: " + date);
    }

    /**
     * Returns the most recent business day at or before the given date.
     *
     * <p>If {@code date} is a business day, returns {@code date} unchanged.
     * If it is a weekend or holiday, returns the preceding business day.
     *
     * <p><b>Not implemented.</b> Integrate with your calendar service.
     *
     * @param date         the reference date (often today's date or the run date)
     * @param calendarName the business calendar to use
     * @return the latest business day on or before {@code date}
     * @throws UnsupportedOperationException until implemented
     */
    public static LocalDate previousBusinessDay(LocalDate date, String calendarName) {
        throw new UnsupportedOperationException(
            "CalendarUtils.previousBusinessDay() is not yet implemented. "
            + "Calendar: " + calendarName + ", Date: " + date);
    }

    /**
     * Moves a date forward or backward by a given number of business days.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code applyOffset(date, -1, "NYSE")} → previous NYSE trading day (T-1)</li>
     *   <li>{@code applyOffset(date,  0, "DEFAULT")} → nearest business day on or before date</li>
     *   <li>{@code applyOffset(date,  5, "UK_BANKING")} → 5 UK banking days forward</li>
     * </ul>
     *
     * <p><b>Not implemented.</b>
     *
     * @param date         the starting date
     * @param offsetDays   number of business days to move (negative = backwards)
     * @param calendarName the business calendar to use
     * @return the resulting business date
     * @throws UnsupportedOperationException until implemented
     */
    public static LocalDate applyOffset(LocalDate date, int offsetDays, String calendarName) {
        throw new UnsupportedOperationException(
            "CalendarUtils.applyOffset() is not yet implemented. "
            + "Calendar: " + calendarName + ", Date: " + date + ", Offset: " + offsetDays);
    }

    // =========================================================================
    // DATE RANGE UTILITIES
    // =========================================================================

    /**
     * Returns all business days (inclusive) between {@code start} and {@code end}.
     *
     * <p>Useful for generating date partitions for backfill pipelines, or for
     * validating that a report covers the expected number of trading days.
     *
     * <p><b>Not implemented.</b>
     *
     * @param start        the first date (inclusive)
     * @param end          the last date (inclusive); must be on or after {@code start}
     * @param calendarName the business calendar to use
     * @return ordered list of business days from {@code start} to {@code end}
     * @throws UnsupportedOperationException until implemented
     */
    public static List<LocalDate> businessDaysInRange(LocalDate start, LocalDate end,
                                                       String calendarName) {
        throw new UnsupportedOperationException(
            "CalendarUtils.businessDaysInRange() is not yet implemented. "
            + "Calendar: " + calendarName + ", Range: " + start + " to " + end);
    }

    /**
     * Returns the number of business days between two dates.
     *
     * <p><b>Not implemented.</b>
     *
     * @param start        the start date (inclusive)
     * @param end          the end date (exclusive)
     * @param calendarName the business calendar to use
     * @return number of business days in [start, end)
     * @throws UnsupportedOperationException until implemented
     */
    public static int countBusinessDays(LocalDate start, LocalDate end, String calendarName) {
        throw new UnsupportedOperationException(
            "CalendarUtils.countBusinessDays() is not yet implemented. "
            + "Calendar: " + calendarName + ", Range: " + start + " to " + end);
    }

    // =========================================================================
    // PIPELINE OPTIONS INTEGRATION
    // =========================================================================

    /**
     * Resolves the effective business date for a pipeline run from its options.
     *
     * <p>Combines {@link FrameworkOptions#getRunDate()},
     * {@link FrameworkOptions#getBusinessDayOffset()}, and
     * {@link FrameworkOptions#getCalendarName()} into a single resolved date.
     *
     * <p><b>Partially implemented.</b> The date parsing is done; the offset application
     * delegates to {@link #applyOffset} which is a stub.
     *
     * @param options pipeline options containing run date and calendar config
     * @return the resolved business date after applying any offset
     */
    public static LocalDate resolveEffectiveDate(FrameworkOptions options) {
        LocalDate runDate = DateUtils.resolveRunDate(options);
        int offset = options.getBusinessDayOffset();
        if (offset == 0) {
            return runDate;
        }
        // Delegates to the stub — implement applyOffset() to make this work
        return applyOffset(runDate, offset, options.getCalendarName());
    }
}
