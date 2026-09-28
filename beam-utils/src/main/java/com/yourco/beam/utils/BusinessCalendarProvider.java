package com.yourco.beam.utils;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Looks up a {@link BusinessCalendar} by {@code calendarKey} in the external calendar DB.
 *
 * <p><b>No implementation ships in this repository</b> — the calendar DB is organisation-
 * specific. Plug one in the same way as {@code EmailSendUtility}: a JAR on the classpath declaring
 * {@code META-INF/services/com.yourco.beam.utils.BusinessCalendarProvider} with the implementing
 * class name. {@link #discover()} finds it via {@link ServiceLoader}.
 *
 * <p>Without one, every item that has a run schedule is <b>not evaluable</b> (BAU: a missing or
 * invalid calendar fails that item's eligibility) — it is skipped and reported, never run on
 * guessed dates. Items with no run schedule don't need a calendar and are unaffected.
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
     * The provider registered via {@code META-INF/services}, or a provider that throws
     * {@link IllegalStateException} on every lookup when none is registered.
     */
    static BusinessCalendarProvider discover() {
        Iterator<BusinessCalendarProvider> found =
            ServiceLoader.load(BusinessCalendarProvider.class).iterator();
        if (found.hasNext()) {
            return found.next();
        }
        return calendarKey -> {
            throw new IllegalStateException(
                "No BusinessCalendarProvider registered (META-INF/services/"
                + BusinessCalendarProvider.class.getName() + ") — cannot resolve calendarKey '"
                + calendarKey + "'. Implement one against the calendar DB.");
        };
    }
}
