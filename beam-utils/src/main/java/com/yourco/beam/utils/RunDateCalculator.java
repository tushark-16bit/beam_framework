package com.yourco.beam.utils;

import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.RunScheduleConfig;
import com.yourco.beam.options.FrameworkOptions;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

/**
 * The one place run dates are decided, for every data source and every report, in every flow
 * ({@code DATA_SOURCE_DOWNLOAD}, {@code REPORT_PROCESSING}, and both halves of {@code PIPELINE}).
 *
 * <p>Flows always call {@link #resolve} and read the returned {@link RunDates} — never
 * {@code --runDate}/{@code --periodStart}/{@code --periodEnd}/{@code --periodId} directly — so
 * date logic can't drift between flows:
 * <ul>
 *   <li>No {@link RunScheduleConfig} configured → {@link #fromOptions}: exactly the CLI options,
 *       i.e. the behaviour before run scheduling existed.</li>
 *   <li>Configured → {@link #calculateRunDates}, which is <b>not implemented</b> — see below.</li>
 * </ul>
 *
 * <h2>Stubs to implement</h2>
 * <ul>
 *   <li>{@link #calculateRunDates} — a data source's dates.</li>
 *   <li>{@link #calculateLastPeriod} — a report's dates: the last period the report covers.</li>
 *   <li>{@link #calculateFreqRunDate} / {@link #calculateMaxFreqRunDate} — the first and last
 *       day a source's period may be run, used by {@link #checkRunWindow} (implemented) to skip
 *       a source outside its window.</li>
 * </ul>
 *
 * <h2>What {@link #calculateRunDates} needs to do</h2>
 * Combine the {@link RunScheduleConfig} fields into a {@link RunDates} (see that class for the
 * expected meaning and format of each field):
 * <ol>
 *   <li>{@code frequency} — which period this run loads (for a period-end finance load, the
 *       period that most recently closed before {@code asOfDate}, e.g. on 2024-02-02 a
 *       {@code MONTHLY} source loads January): gives {@code periodStart}/{@code periodId}.</li>
 *   <li>{@code dateType} (e.g. {@code LAST_DAY_OF_MONTH}) — the business as-of date of that
 *       period → {@code periodEnd}; for {@code DAILY}, {@code dayLag} ({@code WD-n}/{@code CD-n})
 *       counted back from {@code asOfDate} gives the business date instead (period = that day).</li>
 *   <li>{@code runDate} — the date the run executes as: {@code asOfDate}, adjusted to a working
 *       day if your rules require. This is the date {@link #checkRunWindow} compares against
 *       {@code freqRunDay}/{@code maxFreqRunDay}, so it must NOT be the period's as-of date
 *       (that would always fall before the window).</li>
 *   <li>{@code WD} offsets resolve against the calendar named by {@code calendarKey}, fetched from
 *       the external calendar DB — a different system from {@code CalendarUtils}/
 *       {@code --calendarName}.</li>
 * </ol>
 * Every call site runs in the driver JVM before the job is submitted (including
 * {@code PIPELINE}'s report step, whose dates are resolved at submission and carried to the
 * worker), so an implementation may call external systems such as the calendar DB.
 *
 * <h2>Call sites</h2>
 * <ul>
 *   <li>{@code DataSourcePipelineFactory.assembleForConfigs()} — once per data source
 *       ({@code DATA_SOURCE_DOWNLOAD}, and the datasource half of {@code PIPELINE}).</li>
 *   <li>{@code ReportPipelineFactory.execute(options)} — {@link #resolveForReport}, once per
 *       report ({@code REPORT_PROCESSING}).</li>
 *   <li>{@code PipelineSequenceFactory.execute()} — {@link #resolveForReport}, once for the
 *       report half of {@code PIPELINE}.</li>
 *   <li>{@code DataSourcePipelineFactory.assembleForConfigs()} — {@link #checkRunWindow}, once
 *       per source, right after {@link #resolve}.</li>
 * </ul>
 */
public final class RunDateCalculator {

    private RunDateCalculator() {}

    /**
     * Entry point every flow calls. Dispatches on whether a schedule is configured.
     *
     * @param scheduleConfig the source's or report's retrieved schedule; never null
     * @param options        CLI options — the fallback when no schedule is configured, and the
     *                       source of {@code asOfDate} ({@code --runDate}, or today UTC) when one is
     */
    public static RunDates resolve(RunScheduleConfig scheduleConfig, FrameworkOptions options) {
        if (!scheduleConfig.hasSchedule()) {
            return fromOptions(options);
        }
        return calculateRunDates(scheduleConfig, DateUtils.resolveRunDate(options));
    }

    /**
     * Report entry point. A report always runs for a period that has already closed — e.g. a
     * monthly report run in the first days of February reports on January — so with a schedule
     * configured its dates come from {@link #calculateLastPeriod}, not {@link #calculateRunDates}.
     *
     * @param scheduleConfig the report's retrieved schedule ({@code run_details}); never null
     * @param options        CLI options — the fallback when no schedule is configured, and the
     *                       source of {@code asOfDate} ({@code --runDate}, or today UTC) when one is
     */
    public static RunDates resolveForReport(RunScheduleConfig scheduleConfig, FrameworkOptions options) {
        if (!scheduleConfig.hasSchedule()) {
            return fromOptions(options);
        }
        return calculateLastPeriod(scheduleConfig, DateUtils.resolveRunDate(options));
    }

    /**
     * Identifies the last period a report applies to, as of {@code asOfDate}.
     *
     * <p><b>Not implemented.</b> Expected behaviour: using {@code frequency}, find the most recent
     * period that has closed on or before {@code asOfDate} (for {@code MONTHLY} on 2024-02-02 →
     * January 2024) and return it — {@code periodStart}/{@code periodEnd} its first/last day
     * ({@code periodEnd} per {@code dateType}, e.g. {@code LAST_DAY_OF_MONTH} → 2024-01-31),
     * {@code periodId} in the {@code --periodId} encoding (202401), and {@code runDate} the date
     * the report is running as ({@code asOfDate}, adjusted to a working day via
     * {@code calendarKey} if needed). The {@code periodId} must equal the one each datasource the
     * report reads resolved to in {@link #calculateRunDates} for the same run, or the report won't
     * find their DaRefer rows.
     *
     * @param scheduleConfig a configured schedule ({@link RunScheduleConfig#hasSchedule()} is true)
     * @param asOfDate       the reference date — {@code --runDate} if passed, otherwise today UTC
     * @throws UnsupportedOperationException until implemented
     */
    public static RunDates calculateLastPeriod(RunScheduleConfig scheduleConfig, LocalDate asOfDate) {
        throw new UnsupportedOperationException(
            "RunDateCalculator.calculateLastPeriod() is not yet implemented. "
            + "Identify the last closed period for the report's frequency. "
            + "scheduleConfig=" + scheduleConfig + ", asOfDate=" + asOfDate);
    }

    /**
     * Computes a source's run dates from its schedule.
     *
     * <p><b>Not implemented.</b> See class Javadoc.
     *
     * @param scheduleConfig a configured schedule ({@link RunScheduleConfig#hasSchedule()} is true)
     * @param asOfDate       the reference date — {@code --runDate} if passed, otherwise today UTC
     * @throws UnsupportedOperationException until implemented
     */
    public static RunDates calculateRunDates(RunScheduleConfig scheduleConfig, LocalDate asOfDate) {
        throw new UnsupportedOperationException(
            "RunDateCalculator.calculateRunDates() is not yet implemented. "
            + "Integrate with your calendar database and business-date rules. "
            + "scheduleConfig=" + scheduleConfig + ", asOfDate=" + asOfDate);
    }

    // ── Run window: freqRunDay .. maxFreqRunDay ───────────────────────────────

    /**
     * The first day a run for {@code dates}' period is allowed — {@code freqRunDay} applied to
     * the period, e.g. {@code WD+1} for a {@code MONTHLY} January period → the first working day
     * of February per the {@code calendarKey} calendar.
     *
     * <p><b>Not implemented.</b>
     *
     * @param scheduleConfig schedule with {@link RunScheduleConfig#hasFreqRunDay()} true
     * @param dates          the source's resolved dates from {@link #resolve}
     * @throws UnsupportedOperationException until implemented
     */
    public static LocalDate calculateFreqRunDate(RunScheduleConfig scheduleConfig, RunDates dates) {
        throw new UnsupportedOperationException(
            "RunDateCalculator.calculateFreqRunDate() is not yet implemented. "
            + "scheduleConfig=" + scheduleConfig + ", dates=" + dates);
    }

    /**
     * The last day a run for {@code dates}' period is allowed — {@code maxFreqRunDay} applied to
     * the period, e.g. {@code 5} for a {@code MONTHLY} January period → the fifth working day of
     * February. After this day the period is considered closed for this source.
     *
     * <p><b>Not implemented.</b>
     *
     * @param scheduleConfig schedule with {@link RunScheduleConfig#hasMaxRunDayCheck()} true
     * @param dates          the source's resolved dates from {@link #resolve}
     * @throws UnsupportedOperationException until implemented
     */
    public static LocalDate calculateMaxFreqRunDate(RunScheduleConfig scheduleConfig, RunDates dates) {
        throw new UnsupportedOperationException(
            "RunDateCalculator.calculateMaxFreqRunDate() is not yet implemented. "
            + "scheduleConfig=" + scheduleConfig + ", dates=" + dates);
    }

    /**
     * Decides whether a source may run on {@code dates.runDate}: before its freq run date, or
     * after its max freq run date, the source is skipped (not failed). No schedule, or neither
     * {@code freqRunDay} nor {@code maxFreqRunDay} configured → always in window. Only the two
     * boundary dates are stubs; this comparison is the real logic.
     */
    public static RunWindow checkRunWindow(RunScheduleConfig scheduleConfig, RunDates dates) {
        if (!scheduleConfig.hasSchedule()) {
            return new RunWindow(RunWindow.Status.IN_WINDOW, null, null, dates.runDate);
        }
        LocalDate freqRunDate = scheduleConfig.hasFreqRunDay()
            ? calculateFreqRunDate(scheduleConfig, dates) : null;
        LocalDate maxFreqRunDate = scheduleConfig.hasMaxRunDayCheck()
            ? calculateMaxFreqRunDate(scheduleConfig, dates) : null;

        RunWindow.Status status;
        if (freqRunDate != null && dates.runDate.isBefore(freqRunDate)) {
            status = RunWindow.Status.BEFORE_FREQ_RUN_DATE;
        } else if (maxFreqRunDate != null && dates.runDate.isAfter(maxFreqRunDate)) {
            status = RunWindow.Status.AFTER_MAX_FREQ_RUN_DATE;
        } else {
            status = RunWindow.Status.IN_WINDOW;
        }
        return new RunWindow(status, freqRunDate, maxFreqRunDate, dates.runDate);
    }

    /** Result of {@link #checkRunWindow}. Boundary dates are null when not configured. */
    public static final class RunWindow {
        public enum Status { IN_WINDOW, BEFORE_FREQ_RUN_DATE, AFTER_MAX_FREQ_RUN_DATE }

        public final Status    status;
        public final LocalDate freqRunDate;
        public final LocalDate maxFreqRunDate;
        public final LocalDate runDate;

        RunWindow(Status status, LocalDate freqRunDate, LocalDate maxFreqRunDate, LocalDate runDate) {
            this.status         = status;
            this.freqRunDate    = freqRunDate;
            this.maxFreqRunDate = maxFreqRunDate;
            this.runDate        = runDate;
        }

        public boolean shouldRun() { return status == Status.IN_WINDOW; }

        @Override
        public String toString() {
            return status + " (runDate=" + runDate + ", freqRunDate=" + freqRunDate
                + ", maxFreqRunDate=" + maxFreqRunDate + ")";
        }
    }

    /** Run dates taken straight from {@code --runDate}/{@code --periodStart}/{@code --periodEnd}/{@code --periodId}. */
    public static RunDates fromOptions(FrameworkOptions options) {
        return new RunDates(
            DateUtils.resolveRunDate(options),
            parseIsoOrNull(options.getPeriodStart(), "--periodStart"),
            parseIsoOrNull(options.getPeriodEnd(),   "--periodEnd"),
            options.getPeriodId());
    }

    private static LocalDate parseIsoOrNull(String value, String flag) {
        if (value == null || value.isBlank()) return null;
        try {
            return LocalDate.parse(value.trim());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                flag + " '" + value + "' is not a valid ISO-8601 date (yyyy-MM-dd)", e);
        }
    }
}
