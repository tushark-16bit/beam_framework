package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Optional per-source run-scheduling config: which calendar date this source's run should
 * actually process, expressed declaratively rather than as a fixed {@code --periodId}/
 * {@code --runDate}.
 *
 * <p>Fetched from {@code parameter_store} as a single nested JSON object under
 * {@code run_details_json} in {@code parameters_val_json} — e.g.:
 * <pre>{@code
 * "run_details_json": "{\"dateType\":\"LAST_DAY_OF_MONTH\",\"frequency\":\"MONTHLY\",
 *                        \"freqRunDay\":\"WD+1\",\"maxFreqRunDay\":5,\"dayLag\":\"WD-1\",
 *                        \"calendarKey\":\"Calendar_EPS\"}"
 * }</pre>
 *
 * <p>This class only carries the retrieved values — it does not compute anything. Actual date
 * arithmetic (interpreting {@link #dateType}/{@link #frequency}/{@link #freqRunDay}/
 * {@link #dayLag} together, and resolving {@link #calendarKey} against an external calendar
 * database) is {@code RunDateCalculator} (beam-utils), deliberately
 * left unimplemented in this framework — see that class's Javadoc.
 *
 * <h2>Fields</h2>
 * <ul>
 *   <li>{@link #dateType} — which date within the resolved period to land on, e.g.
 *       {@link #LAST_DAY_OF_MONTH}, {@link #LAST_DAY_OF_QUARTER}, {@link #LAST_DAY_OF_YEAR}.</li>
 *   <li>{@link #frequency} — how often this source runs: {@link #DAILY}, {@link #WEEKLY},
 *       {@link #MONTHLY}, {@link #QUARTERLY}, {@link #YEARLY}.</li>
 *   <li>{@link #freqRunDay} — offset defining which day within the frequency period this source
 *       is actually triggered/expected to run, as a small DSL string: a leading {@code WD} means
 *       a workday/business-day offset, {@code CD} a plain calendar-day offset, followed by a
 *       signed count — e.g. {@code "WD+1"} (first workday of the period), {@code "CD+5"} (fifth
 *       calendar day). Free-form on purpose — the exact vocabulary is defined by whatever
 *       {@code RunDateCalculator} implementation interprets it.</li>
 *   <li>{@link #maxFreqRunDay} — the maximum number of days this process is allowed to run for
 *       before being considered overdue/stale. {@link #NO_MAX} (the default) means no cap.</li>
 *   <li>{@link #dayLag} — for {@link #DAILY} sources only: how many days behind the current date
 *       this run should target, using the same {@code WD}/{@code CD} DSL as {@link #freqRunDay}
 *       (e.g. {@code "WD-1"}, {@code "CAL-1"}).</li>
 *   <li>{@link #calendarKey} — identifies which business calendar (holidays, working days) to
 *       fetch from the external calendar database when resolving {@code WD}/{@code CD} offsets
 *       above. Distinct from {@code FrameworkOptions.getCalendarName()}/{@code CalendarUtils} —
 *       this is a per-source key into a separate calendar data source, not the framework-wide
 *       named calendars {@code CalendarUtils} stubs out.</li>
 * </ul>
 */
public final class RunScheduleConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── dateType values ─────────────────────────────────────────────────────
    public static final String LAST_DAY_OF_MONTH   = "LAST_DAY_OF_MONTH";
    public static final String LAST_DAY_OF_QUARTER  = "LAST_DAY_OF_QUARTER";
    public static final String LAST_DAY_OF_YEAR     = "LAST_DAY_OF_YEAR";

    // ── frequency values ─────────────────────────────────────────────────────
    public static final String DAILY     = "DAILY";
    public static final String WEEKLY    = "WEEKLY";
    public static final String MONTHLY   = "MONTHLY";
    public static final String QUARTERLY = "QUARTERLY";
    public static final String YEARLY    = "YEARLY";

    /** No cap on {@link #maxFreqRunDay}. */
    public static final int NO_MAX = -1;

    /** Which date within the resolved period to land on. Null if not configured. */
    public final String dateType;

    /** How often this source runs. Null if not configured. */
    public final String frequency;

    /** {@code WD}/{@code CD} offset DSL string identifying the run day within the period. */
    public final String freqRunDay;

    /** Max number of days this process may run before being stale. {@link #NO_MAX} = no cap. */
    public final int maxFreqRunDay;

    /** {@code DAILY}-only lag DSL string, e.g. {@code "WD-1"}. Null if not configured. */
    public final String dayLag;

    /** Key into the external calendar database. Null if not configured. */
    public final String calendarKey;

    public RunScheduleConfig(String dateType, String frequency, String freqRunDay,
                             int maxFreqRunDay, String dayLag, String calendarKey) {
        this.dateType      = blankToNull(dateType);
        this.frequency     = blankToNull(frequency);
        this.freqRunDay    = blankToNull(freqRunDay);
        this.maxFreqRunDay = maxFreqRunDay;
        this.dayLag        = blankToNull(dayLag);
        this.calendarKey   = blankToNull(calendarKey);
    }

    public boolean hasSchedule()      { return dateType != null || frequency != null; }
    public boolean hasFreqRunDay()    { return freqRunDay != null; }
    public boolean hasMaxRunDayCheck() { return maxFreqRunDay != NO_MAX; }
    public boolean hasCalendarKey()   { return calendarKey != null; }

    public static RunScheduleConfig none() {
        return new RunScheduleConfig(null, null, null, NO_MAX, null, null);
    }

    private static String blankToNull(String s) {
        return (s != null && !s.isBlank()) ? s : null;
    }

    @Override
    public String toString() {
        return "RunScheduleConfig{dateType=" + dateType
            + ", frequency=" + frequency
            + ", freqRunDay=" + freqRunDay
            + ", maxFreqRunDay=" + maxFreqRunDay
            + ", dayLag=" + dayLag
            + ", calendarKey=" + calendarKey + "}";
    }
}
