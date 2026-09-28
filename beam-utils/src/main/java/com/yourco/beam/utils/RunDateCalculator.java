package com.yourco.beam.utils;

import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.RunScheduleConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finance Automation date and scheduling rules — the one place that decides, for every data
 * source and every report in every flow ({@code DATA_SOURCE_DOWNLOAD}, {@code REPORT_PROCESSING},
 * both halves of {@code PIPELINE}):
 * <ol>
 *   <li><b>WHEN</b> — may this item run on the Business Date?
 *       ({@code frequency + freqRunDay + maxFreqRunDay + calendarKey})</li>
 *   <li><b>WHICH</b> — which reporting period does it process? ({@code frequency + dayLag})</li>
 *   <li><b>WHAT</b> — which actual date represents that period downstream?
 *       ({@code dateType + calendarKey}, data sources only)</li>
 * </ol>
 * The fourth BAU question — <b>does it still need to run?</b> (is item + period already
 * {@code COMPLETED}) — is answered by the callers against DaRefer/RptRefer, after this class says
 * the item is eligible and which period it is for.
 *
 * <p>This is a port of existing BAU behaviour and is meant to stay <b>exactly</b> as BAU behaves,
 * including its quirks (each one is labelled {@code BAU PARITY} below). Points where the BAU
 * description didn't pin the behaviour down are labelled {@code OPEN QUESTION} with the choice
 * made here — confirm or correct them before relying on those paths.
 *
 * <h2>Business Date vs Reporting Period</h2>
 * <ul>
 *   <li><b>Business Date</b> — "should this run today?": {@code --runDate} if passed (BAU
 *       {@code ManualRunDateId}), otherwise today in {@code --businessTimeZone}. Compared against
 *       the run window. Becomes {@link RunDates#runDate}.</li>
 *   <li><b>Reporting Period</b> — "which period's data?": derived from the Business Date. Becomes
 *       {@link RunDates#periodStart}/{@link RunDates#periodEnd}/{@link RunDates#periodId}.
 *       E.g. Business Date 2026-09-03, MONTHLY, blank dayLag → period August 2026 (202608): the
 *       automation runs in September while processing August.</li>
 * </ul>
 *
 * <h2>Decision flow ({@link #evaluate})</h2>
 * <pre>
 *   no run schedule configured ─────────────────────────────► ELIGIBLE, dates = CLI flags
 *                                                              (pre-scheduling behaviour)
 *   frequency missing/unsupported, calendarKey missing,
 *   calendar lookup fails, unparseable WD/lag expression ───► NOT_EVALUABLE  (skip + report;
 *                                                              re-evaluated next execution)
 *   DAILY and Business Date not a business day ─────────────► NON_BUSINESS_DAY (skip; no catch-up)
 *   non-DAILY and Business Date &lt; freqRunDay date ─────────► NOT_YET_ELIGIBLE (skip; recheck)
 *   non-DAILY and Business Date &gt; maxFreqRunDay date ───────► EXPIRED  (skip) — unless
 *                                                              --manualOverrun (BAU ManualForceRun)
 *   otherwise ──────────────────────────────────────────────► ELIGIBLE, dates = calculated period
 * </pre>
 * Nothing is persisted for a skip — BAU re-evaluates every item from scratch on every scheduler
 * execution, so a skipped or failed item is simply considered again next time while its window is
 * open.
 *
 * <h2>{@code --manualOverrun} (BAU {@code ManualForceRun})</h2>
 * Bypasses the {@code maxFreqRunDay} check (here) and the {@code COMPLETED} check (callers). It
 * does <b>not</b> bypass the {@code freqRunDay} check or the DAILY business-day check.
 *
 * <h2>Calendar</h2>
 * Every business-day question goes to the {@link BusinessCalendar} for the item's
 * {@code calendarKey}, supplied by a {@link BusinessCalendarProvider} (external calendar DB; you
 * register the implementation). All arithmetic on top — WD offsets, business-day lag,
 * last business day — is here.
 *
 * <p>Everything here runs in the driver JVM before a job is submitted; {@code PIPELINE}'s report
 * dates are decided at submission and carried to the worker.
 */
public final class RunDateCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(RunDateCalculator.class);

    /** BAU frequencies. {@code WEEKLY} was never implemented in BAU and stays unsupported. */
    private static final Set<String> SUPPORTED_FREQUENCIES = Set.of(
        RunScheduleConfig.DAILY, RunScheduleConfig.MONTHLY,
        RunScheduleConfig.QUARTERLY, RunScheduleConfig.ANNUALLY);

    /**
     * Run-day expression: {@code WD+n} = n-th business day of the window month, {@code WD-n} =
     * n-th-last business day, {@code WD+0} = last calendar day of the previous month.
     * BAU PARITY: case-sensitive and whitespace-sensitive, exactly as configured.
     */
    private static final Pattern RUN_DAY = Pattern.compile("WD([+-])(\\d+)");

    /** DAILY lag expression: {@code WD+n} = n business days back, {@code CAL+n} = n calendar days back. */
    private static final Pattern DAILY_LAG = Pattern.compile("(WD|CAL)([+-])(\\d+)");

    /** Guard against a calendar with no business days sending the day-by-day walks into an endless loop. */
    private static final int MAX_DAYS_SCANNED = 400;

    private RunDateCalculator() {}

    /** Whether the item being evaluated is a data source or a report — they differ only in WHAT date ends the period. */
    public enum ItemType { DATA_SOURCE, REPORT }

    // ── Entry points used by the flows ────────────────────────────────────────

    /**
     * Evaluates a data source: WHEN + WHICH + WHAT ({@code dateType} applies).
     * Called once per data source by {@code DataSourcePipelineFactory}.
     */
    public static ScheduleDecision evaluateDataSource(RunScheduleConfig schedule, FrameworkOptions options) {
        return evaluate(schedule, options, ItemType.DATA_SOURCE, BusinessCalendarProvider.discover());
    }

    /**
     * Evaluates a report: WHEN + WHICH. Reports are scheduled independently of their data
     * sources (BAU) — a data source being skipped never skips its report; the report instead
     * checks its data sources are {@code COMPLETED} when it actually runs.
     * Called by {@code ReportPipelineFactory} and {@code PipelineSequenceFactory}.
     */
    public static ScheduleDecision evaluateReport(RunScheduleConfig schedule, FrameworkOptions options) {
        return evaluate(schedule, options, ItemType.REPORT, BusinessCalendarProvider.discover());
    }

    /** Same as the two entry points above, with an explicit calendar provider. */
    public static ScheduleDecision evaluate(RunScheduleConfig schedule, FrameworkOptions options,
                                            ItemType itemType, BusinessCalendarProvider calendars) {
        if (!schedule.hasSchedule()) {
            // No run details at all → pre-scheduling behaviour: CLI dates, always eligible.
            return ScheduleDecision.unscheduled(fromOptions(options));
        }
        return evaluate(schedule, DateUtils.resolveRunDate(options), options.getManualOverrun(),
                        itemType, calendars);
    }

    /**
     * The full BAU decision for one item on one Business Date. Pure function of its inputs —
     * this is the method the parity tests exercise.
     *
     * @param businessDate   BAU Business Date ({@code --runDate}, or today in the framework zone)
     * @param manualForceRun BAU {@code ManualForceRun} ({@code --manualOverrun}): bypasses only
     *                       the {@code maxFreqRunDay} check here
     */
    public static ScheduleDecision evaluate(RunScheduleConfig schedule, LocalDate businessDate,
                                            boolean manualForceRun, ItemType itemType,
                                            BusinessCalendarProvider calendars) {
        // ── Step 1: frequency — selects DAILY (business-day gate) vs window logic, and the
        //    period grain. Missing/unsupported → no task is created; re-evaluated next execution.
        String frequency = schedule.frequency;
        if (frequency == null || !SUPPORTED_FREQUENCIES.contains(frequency)) {
            return ScheduleDecision.notEvaluable(businessDate,
                "frequency '" + frequency + "' is missing or unsupported (supported: "
                + "DAILY, MONTHLY, QUARTERLY, ANNUALLY)");
        }

        // ── Step 2: calendar — BAU: a missing or invalid calendarKey fails the item's
        //    eligibility. Required for every scheduled item, whether or not this particular
        //    evaluation ends up consulting it.
        if (!schedule.hasCalendarKey()) {
            return ScheduleDecision.notEvaluable(businessDate, "calendarKey is missing");
        }
        BusinessCalendar calendar;
        try {
            calendar = calendars.forKey(schedule.calendarKey);
        } catch (RuntimeException e) {
            return ScheduleDecision.notEvaluable(businessDate,
                "calendarKey '" + schedule.calendarKey + "' could not be resolved: " + e.getMessage());
        }

        try {
            // ── Step 3: WHEN — eligibility on the Business Date.
            LocalDate freqRunDate    = null;
            LocalDate maxFreqRunDate = null;
            if (RunScheduleConfig.DAILY.equals(frequency)) {
                // DAILY: no run window at all (freqRunDay/maxFreqRunDay ignored). The Business Date
                // itself must be a business day. A skipped weekend/holiday is never caught up (BAU).
                // ManualForceRun does NOT bypass this.
                if (!calendar.isBusinessDay(businessDate)) {
                    return ScheduleDecision.skip(ScheduleDecision.Status.NON_BUSINESS_DAY,
                        businessDate, null, null,
                        businessDate + " is not a business day in calendar '" + schedule.calendarKey + "'");
                }
            } else {
                // Non-DAILY: freqRunDay <= Business Date <= maxFreqRunDay, inclusive both ends.
                // BAU PARITY: the Business Date itself does NOT need to be a business day here —
                // a MONTHLY item whose window covers a Saturday runs on that Saturday.
                if (schedule.hasFreqRunDay()) {
                    freqRunDate = calculateFreqRunDate(schedule, businessDate, calendar);
                }
                if (schedule.hasMaxFreqRunDay()) {
                    maxFreqRunDate = calculateMaxFreqRunDate(schedule, businessDate, calendar);
                }
                if (freqRunDate != null && businessDate.isBefore(freqRunDate)) {
                    // Before the window opens → NOT YET ELIGIBLE; checked again next execution.
                    // ManualForceRun does NOT bypass this.
                    return ScheduleDecision.skip(ScheduleDecision.Status.NOT_YET_ELIGIBLE,
                        businessDate, freqRunDate, maxFreqRunDate,
                        "business date " + businessDate + " is before freqRunDay "
                        + schedule.freqRunDay + " (" + freqRunDate + ")");
                }
                if (maxFreqRunDate != null && businessDate.isAfter(maxFreqRunDate)) {
                    if (!manualForceRun) {
                        // After the window closes → EXPIRED; this period is no longer
                        // processed automatically.
                        return ScheduleDecision.skip(ScheduleDecision.Status.EXPIRED,
                            businessDate, freqRunDate, maxFreqRunDate,
                            "business date " + businessDate + " is after maxFreqRunDay "
                            + schedule.maxFreqRunDay + " (" + maxFreqRunDate + ")");
                    }
                    LOG.info("--manualOverrun (ManualForceRun): bypassing expired window — business "
                             + "date {} is after maxFreqRunDay {} ({})",
                             businessDate, schedule.maxFreqRunDay, maxFreqRunDate);
                }
                // No maxFreqRunDay → no explicit upper bound: eligible until the period calculation
                // below rolls over to the next reporting cycle (BAU).
            }

            // ── Step 4: WHICH period (and, for a data source, WHAT date ends it).
            RunDates dates = itemType == ItemType.REPORT
                ? calculateLastPeriod(schedule, businessDate, calendar)
                : calculateDataSourcePeriod(schedule, businessDate, calendar);
            return ScheduleDecision.eligible(dates, freqRunDate, maxFreqRunDate);

        } catch (RuntimeException e) {
            // Unparseable run-day/lag expression, or the calendar failing mid-calculation.
            return ScheduleDecision.notEvaluable(businessDate, e.getMessage());
        }
    }

    // ── WHEN: run window ──────────────────────────────────────────────────────

    /**
     * First day of the run window: {@code freqRunDay} (data source) / {@code freqDtl} (report)
     * applied to the window month for {@code businessDate}. E.g. MONTHLY, {@code WD+3}, business
     * date in September 2026 → the third business day of September 2026.
     */
    public static LocalDate calculateFreqRunDate(RunScheduleConfig schedule, LocalDate businessDate,
                                                 BusinessCalendar calendar) {
        return runDayInMonth(schedule.freqRunDay, windowMonth(schedule.frequency, businessDate), calendar);
    }

    /**
     * Last day of the run window, inclusive: {@code maxFreqRunDay} applied to the same window
     * month. E.g. {@code WD+5} → fifth business day; together with {@code WD+3} the item may run
     * or retry on business days three through five.
     */
    public static LocalDate calculateMaxFreqRunDate(RunScheduleConfig schedule, LocalDate businessDate,
                                                    BusinessCalendar calendar) {
        return runDayInMonth(schedule.maxFreqRunDay, windowMonth(schedule.frequency, businessDate), calendar);
    }

    /**
     * The month a non-DAILY item's {@code WD±n} run days are counted in.
     * <ul>
     *   <li>MONTHLY — the Business Date's own month.</li>
     *   <li>QUARTERLY — OPEN QUESTION: BAU uses a "quarter month lookup" that wasn't described.
     *       Assumed here: the first month of the Business Date's quarter (Jan/Apr/Jul/Oct), i.e.
     *       the window opens in the month after the previous quarter closes.</li>
     *   <li>ANNUALLY — OPEN QUESTION: BAU "annual month lookup" not described. Assumed: January of
     *       the Business Date's year.</li>
     * </ul>
     * Outside that month the window is simply in the past (EXPIRED, if a max is configured) or,
     * with no max, still open until the period rolls over — consistent with BAU's "no max → until
     * rollover" rule.
     */
    static YearMonth windowMonth(String frequency, LocalDate businessDate) {
        return switch (frequency) {
            case RunScheduleConfig.MONTHLY   -> YearMonth.from(businessDate);
            case RunScheduleConfig.QUARTERLY -> YearMonth.of(businessDate.getYear(),
                                                    quarterStartMonth(businessDate.getMonthValue()));
            case RunScheduleConfig.ANNUALLY  -> YearMonth.of(businessDate.getYear(), 1);
            default -> throw new IllegalArgumentException(
                "no run window for frequency " + frequency);
        };
    }

    /**
     * Resolves a {@code WD±n} run-day expression within {@code month}:
     * <ul>
     *   <li>{@code WD+n} (n ≥ 1) — n-th business day, counted forward from the 1st.</li>
     *   <li>{@code WD-n} (n ≥ 1) — n-th-last business day, counted back from the month's last day
     *       ({@code WD-1} = last business day, {@code WD-2} = second-last).</li>
     *   <li>{@code WD+0} — BAU PARITY: the last calendar day of the <b>previous</b> month, which
     *       makes the item eligible from the first day of the current month.</li>
     * </ul>
     * OPEN QUESTION: if n exceeds the business days in the month, the count here carries on into
     * the next (or, for {@code WD-n}, previous) month rather than failing — confirm BAU does the
     * same.
     *
     * @throws IllegalArgumentException if {@code expr} isn't {@code WD±n}
     */
    static LocalDate runDayInMonth(String expr, YearMonth month, BusinessCalendar calendar) {
        Matcher m = RUN_DAY.matcher(expr);
        if (!m.matches()) {
            throw new IllegalArgumentException(
                "run day '" + expr + "' is not a WD±n expression (e.g. WD+3, WD-1, WD+0)");
        }
        int n = Integer.parseInt(m.group(2));
        if (n == 0) {
            return month.atDay(1).minusDays(1);
        }
        return "+".equals(m.group(1))
            ? nthBusinessDayForward(month.atDay(1), n, calendar)
            : nthBusinessDayBackward(month.atEndOfMonth(), n, calendar);
    }

    // ── WHICH period + WHAT date ─────────────────────────────────────────────

    /**
     * A data source's period, with its period end per the data-source {@code dateType}:
     * <ul>
     *   <li>MONTHLY + {@code lastBusDayMonth} → last business day of the period month
     *       (January 2026: Jan 31 is a Saturday → 2026-01-30).</li>
     *   <li>MONTHLY + anything else (normally {@code lastDayMonth}) → last calendar day
     *       (→ 2026-01-31). BAU PARITY: exact, case-sensitive match on {@code lastBusDayMonth}.</li>
     *   <li>DAILY / QUARTERLY / ANNUALLY → {@code dateType} is ignored; period end is the period's
     *       calendar end.</li>
     * </ul>
     * {@code dateType} never affects eligibility. The period end flows downstream as
     * {@code {periodEnd}} (query parameters, B&amp;C, file matching).
     */
    public static RunDates calculateDataSourcePeriod(RunScheduleConfig schedule, LocalDate businessDate,
                                                     BusinessCalendar calendar) {
        Period period = calculatePeriod(schedule, businessDate, calendar);
        LocalDate periodEnd = period.end;
        if (RunScheduleConfig.MONTHLY.equals(schedule.frequency)
                && RunScheduleConfig.LAST_BUS_DAY_MONTH.equals(schedule.dateType)) {
            periodEnd = nthBusinessDayBackward(period.end, 1, calendar);
        }
        return new RunDates(businessDate, period.start, periodEnd, period.id);
    }

    /**
     * A report's period — the reporting period it applies to on {@code businessDate}. Same
     * {@code frequency + dayLag} rule as a data source (so a report and its data sources land on
     * the same {@code periodId}); the period end is always the calendar end, because BAU does not
     * use a report's run-level {@code dateType}.
     *
     * <p>Named for the usual case: with a blank or positive {@code dayLag} this is the last closed
     * period (September business date → August). With a {@code WD-}/{@code CAL-} {@code dayLag}
     * it is the current period, exactly as for a data source.
     *
     * <p>Not covered here: BAU's report {@code paramReplace[].dateType}
     * ({@code periodId}/{@code RunDate}/period end + {@code periodOffset}) — see OPEN QUESTION in
     * {@code beam-utils/README.md}.
     */
    public static RunDates calculateLastPeriod(RunScheduleConfig schedule, LocalDate businessDate,
                                               BusinessCalendar calendar) {
        Period period = calculatePeriod(schedule, businessDate, calendar);
        return new RunDates(businessDate, period.start, period.end, period.id);
    }

    /**
     * {@code frequency + dayLag} → reporting period. Behaves fundamentally differently for DAILY
     * and non-DAILY — preserved exactly (BAU PARITY).
     *
     * <h3>DAILY — lag walks back from the Business Date</h3>
     * <ul>
     *   <li>{@code WD+n} → n business days back. E.g. Business Date Tue 2026-09-08, Mon 09-07 a
     *       holiday, {@code WD+1} → Fri 2026-09-04 (holiday and weekend skipped).</li>
     *   <li>{@code CAL+n} → n calendar days back, no business-day adjustment (Monday with
     *       {@code CAL+1} → Sunday).</li>
     *   <li>OPEN QUESTION: blank {@code dayLag} — assumed lag 0 (period = Business Date).</li>
     *   <li>OPEN QUESTION: {@code WD-n}/{@code CAL-n} on a DAILY item — BAU only describes the
     *       {@code +} form. Assumed: same as {@code +n} (magnitude counted back).</li>
     *   <li>Anything else (e.g. {@code CD+1}) → not evaluable.</li>
     * </ul>
     * periodId {@code yyyyMMdd}; period start = end = that day.
     *
     * <h3>MONTHLY / QUARTERLY / ANNUALLY — only the prefix matters</h3>
     * {@code dayLag} starting with {@code WD-} or {@code CAL-} → <b>current</b> period (the one
     * containing the Business Date); anything else — blank, {@code WD+1}, {@code WD+5},
     * {@code CAL+1}, or any other text — → <b>previous</b> period. BAU PARITY: the numeric amount
     * is ignored ({@code WD-1}, {@code WD-5}, {@code CAL-3} all mean "current"), even though the
     * configuration looks like a numeric lag. Case-sensitive prefix match.
     * <ul>
     *   <li>MONTHLY → periodId {@code yyyyMM}.</li>
     *   <li>QUARTERLY → OPEN QUESTION: BAU "quarter period ID" format not described. Assumed
     *       {@code yyyy * 10 + quarter} (Q1 2026 → 20261).</li>
     *   <li>ANNUALLY → periodId {@code yyyy}.</li>
     * </ul>
     */
    static Period calculatePeriod(RunScheduleConfig schedule, LocalDate businessDate,
                                  BusinessCalendar calendar) {
        if (RunScheduleConfig.DAILY.equals(schedule.frequency)) {
            LocalDate day = applyDailyLag(schedule.dayLag, businessDate, calendar);
            int periodId = day.getYear() * 10000 + day.getMonthValue() * 100 + day.getDayOfMonth();
            return new Period(day, day, periodId);
        }

        boolean currentPeriod = schedule.dayLag != null
            && (schedule.dayLag.startsWith("WD-") || schedule.dayLag.startsWith("CAL-"));

        switch (schedule.frequency) {
            case RunScheduleConfig.MONTHLY -> {
                YearMonth month = YearMonth.from(businessDate);
                if (!currentPeriod) month = month.minusMonths(1);
                return new Period(month.atDay(1), month.atEndOfMonth(),
                                  month.getYear() * 100 + month.getMonthValue());
            }
            case RunScheduleConfig.QUARTERLY -> {
                LocalDate quarterStart = LocalDate.of(businessDate.getYear(),
                    quarterStartMonth(businessDate.getMonthValue()), 1);
                if (!currentPeriod) quarterStart = quarterStart.minusMonths(3);
                LocalDate quarterEnd = YearMonth.from(quarterStart.plusMonths(2)).atEndOfMonth();
                int quarter = (quarterStart.getMonthValue() - 1) / 3 + 1;
                return new Period(quarterStart, quarterEnd, quarterStart.getYear() * 10 + quarter);
            }
            case RunScheduleConfig.ANNUALLY -> {
                int year = currentPeriod ? businessDate.getYear() : businessDate.getYear() - 1;
                return new Period(LocalDate.of(year, 1, 1), LocalDate.of(year, 12, 31), year);
            }
            default -> throw new IllegalArgumentException(
                "unsupported frequency " + schedule.frequency);
        }
    }

    private static LocalDate applyDailyLag(String dayLag, LocalDate businessDate, BusinessCalendar calendar) {
        if (dayLag == null) {
            return businessDate;
        }
        Matcher m = DAILY_LAG.matcher(dayLag);
        if (!m.matches()) {
            throw new IllegalArgumentException(
                "DAILY dayLag '" + dayLag + "' is not WD±n or CAL±n");
        }
        int n = Integer.parseInt(m.group(3));
        return "WD".equals(m.group(1))
            ? minusBusinessDays(businessDate, n, calendar)
            : businessDate.minusDays(n);
    }

    // ── Calendar arithmetic ──────────────────────────────────────────────────

    /** n business days strictly before {@code from}; n = 0 → {@code from} unchanged. */
    static LocalDate minusBusinessDays(LocalDate from, int n, BusinessCalendar calendar) {
        LocalDate day = from;
        int counted = 0;
        int scanned = 0;
        while (counted < n) {
            day = day.minusDays(1);
            if (++scanned > MAX_DAYS_SCANNED) throw noBusinessDays(from);
            if (calendar.isBusinessDay(day)) counted++;
        }
        return day;
    }

    /** The n-th business day on or after {@code start} (n ≥ 1). */
    private static LocalDate nthBusinessDayForward(LocalDate start, int n, BusinessCalendar calendar) {
        LocalDate day = start;
        int counted = 0;
        for (int scanned = 0; scanned <= MAX_DAYS_SCANNED; scanned++, day = day.plusDays(1)) {
            if (calendar.isBusinessDay(day) && ++counted == n) return day;
        }
        throw noBusinessDays(start);
    }

    /** The n-th business day on or before {@code end} (n ≥ 1; n = 1 → last business day). */
    private static LocalDate nthBusinessDayBackward(LocalDate end, int n, BusinessCalendar calendar) {
        LocalDate day = end;
        int counted = 0;
        for (int scanned = 0; scanned <= MAX_DAYS_SCANNED; scanned++, day = day.minusDays(1)) {
            if (calendar.isBusinessDay(day) && ++counted == n) return day;
        }
        throw noBusinessDays(end);
    }

    private static IllegalArgumentException noBusinessDays(LocalDate around) {
        return new IllegalArgumentException(
            "calendar has no business days within " + MAX_DAYS_SCANNED + " days of " + around);
    }

    private static int quarterStartMonth(int month) {
        return ((month - 1) / 3) * 3 + 1;
    }

    // ── Unscheduled items ────────────────────────────────────────────────────

    /**
     * Run dates taken straight from {@code --runDate}/{@code --periodStart}/{@code --periodEnd}/
     * {@code --periodId} — used for any item with no run schedule configured.
     */
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

    // ── Result types ─────────────────────────────────────────────────────────

    /** A reporting period before a data source's {@code dateType} is applied to its end. */
    static final class Period {
        final LocalDate start;
        final LocalDate end;
        final int       id;

        Period(LocalDate start, LocalDate end, int id) {
            this.start = start;
            this.end   = end;
            this.id    = id;
        }
    }

    /** Outcome of {@link #evaluate} for one item on one Business Date. */
    public static final class ScheduleDecision {

        public enum Status {
            /** Run it (subject to the caller's COMPLETED check). {@link #dates} is set. */
            ELIGIBLE,
            /** Non-DAILY, before the freqRunDay date. Skip; recheck next execution. */
            NOT_YET_ELIGIBLE,
            /** Non-DAILY, after the maxFreqRunDay date. Skip; period no longer processed automatically. */
            EXPIRED,
            /** DAILY on a weekend/holiday. Skip; that date is never caught up. */
            NON_BUSINESS_DAY,
            /** Configuration or calendar problem. Skip this item and report it; re-evaluated next execution. */
            NOT_EVALUABLE
        }

        public final Status    status;
        /** Set only when {@link #status} is {@link Status#ELIGIBLE}. */
        public final RunDates  dates;
        public final LocalDate businessDate;
        /** Null when not configured or not reached (DAILY, or evaluation stopped earlier). */
        public final LocalDate freqRunDate;
        /** Null when not configured or not reached. */
        public final LocalDate maxFreqRunDate;
        /** True when the item had a run schedule (false → dates are the CLI flags). */
        public final boolean   scheduled;
        /** Human-readable reason, for logs and failure notifications. */
        public final String    detail;

        private ScheduleDecision(Status status, RunDates dates, LocalDate businessDate,
                                 LocalDate freqRunDate, LocalDate maxFreqRunDate,
                                 boolean scheduled, String detail) {
            this.status         = status;
            this.dates          = dates;
            this.businessDate   = businessDate;
            this.freqRunDate    = freqRunDate;
            this.maxFreqRunDate = maxFreqRunDate;
            this.scheduled      = scheduled;
            this.detail         = detail;
        }

        static ScheduleDecision unscheduled(RunDates dates) {
            return new ScheduleDecision(Status.ELIGIBLE, dates, dates.runDate, null, null, false,
                "no run schedule configured — dates from CLI flags");
        }

        static ScheduleDecision eligible(RunDates dates, LocalDate freqRunDate, LocalDate maxFreqRunDate) {
            return new ScheduleDecision(Status.ELIGIBLE, dates, dates.runDate, freqRunDate,
                maxFreqRunDate, true, "eligible");
        }

        static ScheduleDecision skip(Status status, LocalDate businessDate, LocalDate freqRunDate,
                                     LocalDate maxFreqRunDate, String detail) {
            return new ScheduleDecision(status, null, businessDate, freqRunDate, maxFreqRunDate,
                true, detail);
        }

        static ScheduleDecision notEvaluable(LocalDate businessDate, String detail) {
            return new ScheduleDecision(Status.NOT_EVALUABLE, null, businessDate, null, null,
                true, detail);
        }

        public boolean shouldRun() { return status == Status.ELIGIBLE; }

        @Override
        public String toString() {
            return status + " (businessDate=" + businessDate
                + (freqRunDate    != null ? ", freqRunDate="    + freqRunDate    : "")
                + (maxFreqRunDate != null ? ", maxFreqRunDate=" + maxFreqRunDate : "")
                + (dates          != null ? ", " + dates : "")
                + ") — " + detail;
        }
    }
}
