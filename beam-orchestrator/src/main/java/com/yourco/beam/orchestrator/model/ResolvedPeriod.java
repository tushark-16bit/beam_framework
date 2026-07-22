package com.yourco.beam.orchestrator.model;

import java.time.LocalDate;

/**
 * The period calculated from a run date and frequency.
 *
 * <p>Produced by {@link com.yourco.beam.orchestrator.period.PeriodResolver} and
 * carried on every {@link RunSpec} so that each task knows its exact time window.
 *
 * <h2>Default periodId encoding</h2>
 * <ul>
 *   <li>DAILY   → YYYYMMDD  (e.g. 20240115)</li>
 *   <li>MONTHLY → YYYYMM    (e.g. 202401)</li>
 *   <li>WEEKLY  → YYYYWW    (ISO week, e.g. 202402 = week 2 of 2024)</li>
 * </ul>
 * Swap {@link com.yourco.beam.orchestrator.period.PeriodResolver} to change the encoding.
 */
public final class ResolvedPeriod {

    public final int       periodId;
    public final LocalDate periodStart;
    public final LocalDate periodEnd;
    public final LocalDate runDate;
    public final String    frequency;   // DAILY | MONTHLY | WEEKLY

    public ResolvedPeriod(int periodId, LocalDate periodStart, LocalDate periodEnd,
                          LocalDate runDate, String frequency) {
        this.periodId    = periodId;
        this.periodStart = periodStart;
        this.periodEnd   = periodEnd;
        this.runDate     = runDate;
        this.frequency   = frequency;
    }

    @Override
    public String toString() {
        return "ResolvedPeriod{id=" + periodId
            + ", start=" + periodStart
            + ", end=" + periodEnd
            + ", freq=" + frequency + "}";
    }
}
