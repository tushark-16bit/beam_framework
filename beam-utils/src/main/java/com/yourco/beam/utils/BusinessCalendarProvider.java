package com.yourco.beam.utils;

import com.yourco.beam.options.FrameworkOptions;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Looks up a {@link BusinessCalendar} by {@code calendarKey} in the external calendar DB.
 *
 * <p>The default implementation is {@link BigQueryBusinessCalendarProvider}, which reads the
 * calendars from the parameter table. A different one (an external calendar service) can be
 * plugged in the same way as {@code EmailSendUtility}: a JAR on the classpath declaring
 * {@code META-INF/services/com.yourco.beam.utils.BusinessCalendarProvider}; {@link #discover}
 * prefers it via {@link ServiceLoader}.
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

    /**
     * The provider to use for this run: one registered via {@code META-INF/services} if present
     * (an override, e.g. for an external calendar service), otherwise the
     * {@link BigQueryBusinessCalendarProvider}, which reads the calendars from the parameter table
     * named by {@code options}. Creating it needs no BigQuery access — that happens on first lookup.
     */
    static BusinessCalendarProvider discover(FrameworkOptions options) {
        Iterator<BusinessCalendarProvider> found =
            ServiceLoader.load(BusinessCalendarProvider.class).iterator();
        if (found.hasNext()) {
            return found.next();
        }
        return new BigQueryBusinessCalendarProvider(options);
    }
}
