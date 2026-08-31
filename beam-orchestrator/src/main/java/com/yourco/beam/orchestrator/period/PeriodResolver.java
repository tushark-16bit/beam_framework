package com.yourco.beam.orchestrator.period;

import com.yourco.beam.orchestrator.model.ResolvedPeriod;

import java.time.LocalDate;

/**
 * Calculates the period for a given run date and frequency.
 *
 * <p>Implement this interface to change how {@code periodId}, {@code periodStart},
 * and {@code periodEnd} are derived — e.g. to apply business day calendars, fiscal
 * year offsets, or custom period encoding.
 *
 * <p>The default implementation is {@link StandardPeriodResolver}:
 * <pre>
 *   DAILY   → periodId = YYYYMMDD, start = runDate,       end = runDate
 *   MONTHLY → periodId = YYYYMM,   start = first of month, end = last of month
 *   WEEKLY  → periodId = YYYYWW,   start = Monday,         end = Sunday
 * </pre>
 */
@FunctionalInterface
public interface PeriodResolver {

    /**
     * @param runDate   the calendar date the orchestrator was triggered for
     * @param frequency one of {@code DAILY}, {@code MONTHLY}, {@code WEEKLY}
     *                  (custom values are allowed if the impl handles them)
     * @return a fully resolved period for this run
     */
    ResolvedPeriod resolve(LocalDate runDate, String frequency);
}
