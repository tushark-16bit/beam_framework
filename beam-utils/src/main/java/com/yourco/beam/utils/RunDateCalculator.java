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
 * <h2>What {@link #calculateRunDates} needs to do</h2>
 * Combine the {@link RunScheduleConfig} fields into a {@link RunDates} (see that class for the
 * expected meaning and format of each field):
 * <ol>
 *   <li>{@code frequency} — which period contains {@code asOfDate}: gives
 *       {@code periodStart}/{@code periodEnd}/{@code periodId}.</li>
 *   <li>{@code dateType} (e.g. {@code LAST_DAY_OF_MONTH}) — which date within that period is
 *       the business {@code runDate}; for {@code DAILY}, {@code dayLag} ({@code WD-n}/{@code CD-n})
 *       counted back from {@code asOfDate} instead.</li>
 *   <li>{@code freqRunDay}/{@code maxFreqRunDay} — when in the following period the run is
 *       expected/allowed; use them to decide which period {@code asOfDate} is still reporting on
 *       (e.g. on {@code WD+1} of February, a monthly source reports January).</li>
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
 *   <li>{@code ReportPipelineFactory.execute(options)} — once per report
 *       ({@code REPORT_PROCESSING}).</li>
 *   <li>{@code PipelineSequenceFactory.execute()} — once for the report half of
 *       {@code PIPELINE}.</li>
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
     * Computes a source's or report's run dates from its schedule.
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
