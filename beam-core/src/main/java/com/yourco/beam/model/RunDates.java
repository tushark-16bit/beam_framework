package com.yourco.beam.model;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * The dates one data source or report run actually operates on — the single value every flow
 * reads its dates from, instead of reading {@code --runDate}/{@code --periodStart}/
 * {@code --periodEnd}/{@code --periodId} directly.
 *
 * <p>Produced by {@code RunDateCalculator.resolve()} (beam-utils): from the source's/report's
 * {@link RunScheduleConfig} when one is configured, otherwise straight from the CLI options.
 *
 * <h2>Expected formats</h2>
 * <table>
 *   <tr><th>Field</th><th>Meaning</th><th>Rendered as</th></tr>
 *   <tr><td>{@link #runDate}</td><td>the date the run executes as — {@code --runDate} or today,
 *       possibly adjusted to a working day; a source is skipped when this falls outside its
 *       {@code freqRunDay}..{@code maxFreqRunDay} window. Not the period's as-of date — that is
 *       {@link #periodEnd}</td><td>{@code yyyy-MM-dd} for {@code {runDate}}/{@code %runDate%} and a FILE
 *       source's {@code {date}}; {@code yyyyMMdd} for {@code {dateCompact}}; the source's
 *       {@code file_date_pattern} for {@code {fileDate}}</td></tr>
 *   <tr><td>{@link #periodStart}</td><td>first day of the period the data covers</td>
 *       <td>{@code yyyy-MM-dd} for {@code {periodStart}}/{@code %periodStart%}; empty when null</td></tr>
 *   <tr><td>{@link #periodEnd}</td><td>last day of the period the data covers — the business as-of
 *       date ({@code dateType}, e.g. month-end)</td>
 *       <td>{@code yyyy-MM-dd} for {@code {periodEnd}}/{@code %periodEnd%}; empty when null</td></tr>
 *   <tr><td>{@link #periodId}</td><td>DaRefer/RptRefer {@code per_id} key for the period</td>
 *       <td>int, same encoding as {@code --periodId}: DAILY {@code yyyyMMdd}, MONTHLY
 *       {@code yyyyMM}, etc. — a report finds its datasources' DaRefer rows by this value, so a
 *       report and the datasources it reads must resolve to the same {@code periodId}</td></tr>
 * </table>
 */
public final class RunDates implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter ISO     = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter COMPACT = DateTimeFormatter.ofPattern("yyyyMMdd");

    public final LocalDate runDate;
    /** Null when not known (e.g. {@code --periodStart} not passed). */
    public final LocalDate periodStart;
    /** Null when not known (e.g. {@code --periodEnd} not passed). */
    public final LocalDate periodEnd;
    public final int periodId;

    public RunDates(LocalDate runDate, LocalDate periodStart, LocalDate periodEnd, int periodId) {
        if (runDate == null) {
            throw new IllegalArgumentException("runDate is required");
        }
        this.runDate     = runDate;
        this.periodStart = periodStart;
        this.periodEnd   = periodEnd;
        this.periodId    = periodId;
    }

    public String runDateIso()      { return runDate.format(ISO); }
    public String runDateCompact()  { return runDate.format(COMPACT); }
    public String periodStartIso()  { return periodStart != null ? periodStart.format(ISO) : ""; }
    public String periodEndIso()    { return periodEnd   != null ? periodEnd.format(ISO)   : ""; }

    @Override
    public String toString() {
        return "RunDates{runDate=" + runDate + ", periodStart=" + periodStart
            + ", periodEnd=" + periodEnd + ", periodId=" + periodId + "}";
    }
}
