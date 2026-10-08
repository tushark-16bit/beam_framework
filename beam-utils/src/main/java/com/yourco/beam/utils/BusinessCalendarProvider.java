package com.yourco.beam.utils;

/**
 * Looks up a {@link BusinessCalendar} by {@code calendarKey} in the external calendar DB.
 *
 * <p>The implementation is {@link BigQueryBusinessCalendarProvider}, which reads the calendars
 * from the parameter table. Callers construct it explicitly
 * ({@code new BigQueryBusinessCalendarProvider(options)}) and may hold it in a variable of this
 * interface; nothing is discovered via {@code ServiceLoader} (CLAUDE.md §12). To use a different
 * calendar source, add an implementation and change the one place that constructs it.
 *
 * <p>If the calendar cannot be loaded (no row, bad data, BigQuery failure) the item is <b>not
 * evaluable</b> (BAU: a missing or invalid calendar fails that item's eligibility) — it is not
 * processed and the failure is reported, never run on guessed dates.
 *
 * <p>Called only in the driver JVM, before a job is submitted, so an implementation may query the
 * calendar DB directly. It should cache per key if lookups are expensive — one run evaluates every
 * datasource of a report.
 */
@FunctionalInterface
public interface BusinessCalendarProvider {

    /**
     * @param calendarKey the item's configured {@code calendarKey}; never null
     * @return that calendar
     * @throws IllegalArgumentException if no calendar exists for {@code calendarKey} — the item is
     *         then not evaluable (BAU: invalid calendarKey fails eligibility)
     */
    BusinessCalendar forKey(String calendarKey);

}
