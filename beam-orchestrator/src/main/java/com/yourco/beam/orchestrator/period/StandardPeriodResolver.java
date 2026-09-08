package com.yourco.beam.orchestrator.period;

import com.yourco.beam.orchestrator.model.ResolvedPeriod;

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.time.DayOfWeek;

/**
 * Default period resolver for DAILY, MONTHLY, and WEEKLY frequencies.
 *
 * <h2>periodId encoding</h2>
 * <pre>
 *   DAILY   → YYYYMMDD  e.g. 20240115
 *   MONTHLY → YYYYMM    e.g. 202401
 *   WEEKLY  → YYYYWW    e.g. 202402  (ISO week number, zero-padded 2 digits)
 * </pre>
 *
 * <h2>Swapping this implementation</h2>
 * To apply business day offsets, fiscal year logic, or a different periodId scheme,
 * implement {@link PeriodResolver} and inject it via {@link com.yourco.beam.orchestrator.OrchestratorMain}.
 */
public final class StandardPeriodResolver implements PeriodResolver {

    public static final String DAILY   = "DAILY";
    public static final String MONTHLY = "MONTHLY";
    public static final String WEEKLY  = "WEEKLY";

    @Override
    public ResolvedPeriod resolve(LocalDate runDate, String frequency) {
        return switch (frequency.toUpperCase()) {
            case DAILY   -> resolveDaily(runDate);
            case MONTHLY -> resolveMonthly(runDate);
            case WEEKLY  -> resolveWeekly(runDate);
            default -> throw new IllegalArgumentException(
                "Unknown frequency '" + frequency + "'. Supported: DAILY, MONTHLY, WEEKLY. "
                + "Implement PeriodResolver to add custom frequencies.");
        };
    }

    private ResolvedPeriod resolveDaily(LocalDate runDate) {
        int periodId = runDate.getYear() * 10000
                     + runDate.getMonthValue() * 100
                     + runDate.getDayOfMonth();
        return new ResolvedPeriod(periodId, runDate, runDate, runDate, DAILY);
    }

    private ResolvedPeriod resolveMonthly(LocalDate runDate) {
        int periodId = runDate.getYear() * 100 + runDate.getMonthValue();
        LocalDate start = runDate.with(TemporalAdjusters.firstDayOfMonth());
        LocalDate end   = runDate.with(TemporalAdjusters.lastDayOfMonth());
        return new ResolvedPeriod(periodId, start, end, runDate, MONTHLY);
    }

    private ResolvedPeriod resolveWeekly(LocalDate runDate) {
        int isoWeek = runDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        int isoYear = runDate.get(IsoFields.WEEK_BASED_YEAR);
        int periodId = isoYear * 100 + isoWeek;
        LocalDate start = runDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        LocalDate end   = start.plusDays(6);
        return new ResolvedPeriod(periodId, start, end, runDate, WEEKLY);
    }
}
