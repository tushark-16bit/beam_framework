package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Optional run-scheduling config for one data source or report — the six Finance Automation
 * scheduling attributes, retrieved exactly as configured. This class only carries the values;
 * every rule that interprets them lives in {@code RunDateCalculator} (beam-utils).
 *
 * <p>Where it comes from:
 * <ul>
 *   <li>Data source — {@code run_details_json} in {@code parameters_val_json}, e.g.
 *       <pre>{@code {"frequency":"MONTHLY","freqRunDay":"WD+3","maxFreqRunDay":"WD+5",
 *   "dayLag":"","dateType":"lastBusDayMonth","calendarKey":"Calendar_EPS"}}</pre></li>
 *   <li>Report — the nested {@code run_details} object in the report config. Reports store the
 *       run-window start as <b>{@code freqDtl}</b>, not {@code freqRunDay} (BAU naming); it is
 *       read into {@link #freqRunDay} here so the evaluation code is shared.</li>
 * </ul>
 *
 * <h2>What each attribute controls (BAU)</h2>
 * <pre>
 *   frequency + freqRunDay + maxFreqRunDay + calendarKey  → WHEN: can it run today?
 *   frequency + dayLag                                    → WHICH period does it process?
 *   dateType  + calendarKey                               → WHAT date represents that period?
 * </pre>
 * <ul>
 *   <li>{@link #frequency} — {@link #DAILY} | {@link #MONTHLY} | {@link #QUARTERLY} |
 *       {@link #ANNUALLY}. Anything else (including {@code WEEKLY}, which BAU never implemented)
 *       makes the item not evaluable.</li>
 *   <li>{@link #freqRunDay} — first day of the run window, {@code WD±n}. Ignored for DAILY.</li>
 *   <li>{@link #maxFreqRunDay} — last day of the run window (inclusive), {@code WD±n}. Blank →
 *       no upper bound. Ignored for DAILY.</li>
 *   <li>{@link #dayLag} — which period: DAILY → {@code WD+n}/{@code CAL+n} days back from the
 *       business date; non-DAILY → only the prefix matters ({@code WD-}/{@code CAL-} → current
 *       period, anything else → previous period).</li>
 *   <li>{@link #dateType} — data source, MONTHLY only: {@link #LAST_BUS_DAY_MONTH} → period end
 *       is the last business day; anything else (normally {@link #LAST_DAY_MONTH}) → last calendar
 *       day. A report's run-level {@code dateType} is not used (BAU).</li>
 *   <li>{@link #calendarKey} — business calendar (weekends + holidays) in the external calendar
 *       DB. Required for every scheduled item.</li>
 * </ul>
 * All values are kept as the raw configured strings; interpretation (and its exact-match /
 * case-sensitivity rules) is in {@code RunDateCalculator}.
 */
public final class RunScheduleConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    // ── frequency values (BAU) ────────────────────────────────────────────────
    public static final String DAILY     = "DAILY";
    public static final String MONTHLY   = "MONTHLY";
    public static final String QUARTERLY = "QUARTERLY";
    public static final String ANNUALLY  = "ANNUALLY";

    // ── data-source dateType values (BAU) ─────────────────────────────────────
    /** MONTHLY period end = last business day of the month per {@link #calendarKey}. */
    public static final String LAST_BUS_DAY_MONTH = "lastBusDayMonth";
    /** MONTHLY period end = last calendar day of the month (also the fallback for any other value). */
    public static final String LAST_DAY_MONTH     = "lastDayMonth";

    /** Null if not configured. */
    public final String frequency;
    /** Run-window start ({@code freqRunDay} for sources, {@code freqDtl} for reports). Null if not configured. */
    public final String freqRunDay;
    /** Run-window end, inclusive. Null = no upper bound. */
    public final String maxFreqRunDay;
    /** Null if not configured. */
    public final String dayLag;
    /** Null if not configured. */
    public final String dateType;
    /** Null if not configured. */
    public final String calendarKey;

    public RunScheduleConfig(String frequency, String freqRunDay, String maxFreqRunDay,
                             String dayLag, String dateType, String calendarKey) {
        this.frequency     = blankToNull(frequency);
        this.freqRunDay    = blankToNull(freqRunDay);
        this.maxFreqRunDay = blankToNull(maxFreqRunDay);
        this.dayLag        = blankToNull(dayLag);
        this.dateType      = blankToNull(dateType);
        this.calendarKey   = blankToNull(calendarKey);
    }

    /**
     * True when any scheduling attribute is configured — the item is then governed by the BAU
     * rules in {@code RunDateCalculator}. False (no run details at all) means the item is not
     * processed — every item needs a run schedule and a calendar.
     */
    public boolean hasSchedule() {
        return frequency != null || freqRunDay != null || maxFreqRunDay != null
            || dayLag != null || dateType != null || calendarKey != null;
    }

    public boolean hasFreqRunDay()    { return freqRunDay != null; }
    public boolean hasMaxFreqRunDay() { return maxFreqRunDay != null; }
    public boolean hasCalendarKey()   { return calendarKey != null; }

    public static RunScheduleConfig none() {
        return new RunScheduleConfig(null, null, null, null, null, null);
    }

    private static String blankToNull(String s) {
        return (s != null && !s.isBlank()) ? s : null;
    }

    @Override
    public String toString() {
        return "RunScheduleConfig{frequency=" + frequency
            + ", freqRunDay=" + freqRunDay
            + ", maxFreqRunDay=" + maxFreqRunDay
            + ", dayLag=" + dayLag
            + ", dateType=" + dateType
            + ", calendarKey=" + calendarKey + "}";
    }
}
