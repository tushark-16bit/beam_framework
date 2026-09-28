package com.yourco.beam.utils;

import java.time.LocalDate;

/**
 * One business calendar from the external calendar DB, identified by a {@code calendarKey}
 * (see {@link BusinessCalendarProvider}). It answers a single question — is this date a business
 * day (not a weekend, not a holiday)? — and every business-day calculation in
 * {@link RunDateCalculator} ({@code WD±n} run days, DAILY eligibility, DAILY {@code WD} lag,
 * {@code lastBusDayMonth}) is built on top of that one answer, so the calendar rules live in one
 * place.
 *
 * <p>Which days count as the weekend is the calendar's decision, not the framework's — BAU
 * calendars define both weekends and holidays.
 */
@FunctionalInterface
public interface BusinessCalendar {

    /** True if {@code date} is a business day in this calendar (not a weekend, not a holiday). */
    boolean isBusinessDay(LocalDate date);
}
