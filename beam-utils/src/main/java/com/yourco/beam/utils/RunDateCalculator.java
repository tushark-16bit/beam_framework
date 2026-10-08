package com.yourco.beam.utils;

import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.RunScheduleConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
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
 * <p><b>THE CONTRACT IS {@code DATE_SCHEDULING_RULES.md} (repo root). Read it before changing
 * anything in this class, never change behaviour away from it without the owner's explicit
 * approval, and if a change or a test contradicts it, stop and ask the owner.</b> Part 1 of that
 * file is the BAU description verbatim; Part 2 holds the owner's decisions for this framework
 * (manual overrun, optional period id, quarterly id format); Part 3 lists assumptions and pending
 * confirmations.
 *
 * <p>This is a port of existing BAU behaviour and is meant to stay <b>exactly</b> as BAU behaves,
 * including its quirks (each one is labelled {@code BAU PARITY} below). Points where the BAU
 * description didn't pin the behaviour down are labelled {@code OPEN QUESTION} with the choice
 * made here — they are the "Assumptions" in Part 3 of the contract; confirm or correct them
 * before relying on those paths.
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
 *   no run schedule, frequency missing/unsupported,
 *   calendarKey missing or unknown (no calendar), unparseable
 *   WD/lag expression, positive DAILY lag ──────────────────► NOT_EVALUABLE  (not processed + failure
 *                                                              notification; re-evaluated next
 *                                                              execution — owner decisions D4, D5)
 *   Business Date not a business day (ANY frequency, D6) ───► NON_BUSINESS_DAY (skip; DAILY: no catch-up,
 *                                                              non-DAILY: next business day in window runs)
 *   non-DAILY and Business Date &lt; freqRunDay date ─────────► NOT_YET_ELIGIBLE (skip; recheck)
 *   non-DAILY and Business Date &gt; maxFreqRunDay date ───────► EXPIRED  (skip)
 *   otherwise ──────────────────────────────────────────────► ELIGIBLE
 * </pre>
 * The reporting period is calculated <b>first</b> and stored on the decision
 * ({@link ScheduleDecision#dates}) whatever the status — a skipped item still shows which period it
 * would have processed. Only {@code NOT_EVALUABLE} may have no dates. Callers must still use the
 * dates only when {@link ScheduleDecision#shouldRun()}.
 *
 * <p>Nothing is persisted for a skip — BAU re-evaluates every item from scratch on every scheduler
 * execution, so a skipped or failed item is simply considered again next time while its window is
 * open.
 *
 * <h2>{@code --manualOverrun} does NOT affect any of this (owner decision D1)</h2>
 * It is only about storage and overwriting — callers bypass the {@code COMPLETED} check and
 * supersede the stored data. Eligibility and date calculation here never look at it: to force a
 * re-run the caller passes the {@code --runDate} that is eligible under the normal rules. (This
 * deliberately differs from BAU's {@code ManualForceRun}, which also bypassed the max window.)
 *
 * <h2>Period id is calculated, not required (owner decisions D2, D3)</h2>
 * For a scheduled item {@link RunDates#periodId} comes from Business Date + {@code frequency} +
 * {@code dayLag}; a {@code --periodId} passed as well is ignored (with a warning if it differs).
 * Encodings: DAILY {@code yyyyMMdd}, MONTHLY {@code yyyyMM}, QUARTERLY {@code yyyyMMddqq}
 * (first date of the quarter + zero-padded quarter number, Q1 2026 → {@code 2026010101}),
 * ANNUALLY {@code yyyy}.
 *
 * <h2>No schedule or no calendar → not processed (owner decision D5)</h2>
 * Every item needs a run schedule <b>and</b> a calendar that exists. Without them the item is
 * {@code NOT_EVALUABLE} — there is no fallback to command-line dates, so {@code --periodId},
 * {@code --periodStart} and {@code --periodEnd} never run an item. {@link #fromOptions} remains only
 * for callers outside the scheduled flows (e.g. the legacy example workflow).
 *
 * <h2>DAILY dayLag is {@code WD-n}/{@code CAL-n} (owner decision D4)</h2>
 * The calculation is BAU's (go back n business / calendar days); a positive DAILY lag is not
 * expected and is an error ({@code NOT_EVALUABLE}, with the standard failure notification).
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

    /**
     * DAILY lag expression (owner decision D4): {@code WD-n} = n business days back, {@code CAL-n} =
     * n calendar days back. The positive form is rejected as an error.
     */
    private static final Pattern DAILY_LAG = Pattern.compile("(WD|CAL)-(\\d+)");
    private static final Pattern DAILY_LAG_POSITIVE = Pattern.compile("(WD|CAL)\\+(\\d+)");

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
        return evaluate(schedule, options, ItemType.DATA_SOURCE, new BigQueryBusinessCalendarProvider(options));
    }

    /**
     * Evaluates a report: WHEN + WHICH. Reports are scheduled independently of their data
     * sources (BAU) — a data source being skipped never skips its report; the report instead
     * checks its data sources are {@code COMPLETED} when it actually runs.
     * Called by {@code ReportPipelineFactory} and {@code PipelineSequenceFactory}.
     */
    public static ScheduleDecision evaluateReport(RunScheduleConfig schedule, FrameworkOptions options) {
        return evaluate(schedule, options, ItemType.REPORT, new BigQueryBusinessCalendarProvider(options));
    }

    /** Same as the two entry points above, with an explicit calendar provider. */
    public static ScheduleDecision evaluate(RunScheduleConfig schedule, FrameworkOptions options,
                                            ItemType itemType, BusinessCalendarProvider calendars) {
        // Business Date = --runDate or today; the period id is calculated from it. An item with no
        // schedule is rejected by the pure evaluate() below (owner decision D5).
        // --manualOverrun is deliberately not passed down — it never affects eligibility or dates.
        ScheduleDecision decision = evaluate(schedule, DateUtils.resolveRunDate(options),
                                             itemType, calendars);
        if (decision.dates != null && options.getPeriodId() > 0
                && options.getPeriodId() != decision.dates.periodId) {
            LOG.warn("--periodId={} ignored: the calculated period id for business date {} is {}",
                     options.getPeriodId(), decision.businessDate, decision.dates.periodId);
        }
        return decision;
    }

    /**
     * The full BAU decision for one item on one Business Date. Pure function of its inputs —
     * this is the method the parity tests exercise.
     *
     * @param businessDate BAU Business Date ({@code --runDate}, or today in the framework zone)
     */
    public static ScheduleDecision evaluate(RunScheduleConfig schedule, LocalDate businessDate,
                                            ItemType itemType, BusinessCalendarProvider calendars) {
        // ── Step 0: an item must have a run schedule (owner decision D5). No fallback to CLI dates.
        if (!schedule.hasSchedule()) {
            return ScheduleDecision.notEvaluable(businessDate,
                "no run schedule is configured (run_details missing) — an item without a schedule "
                + "and a calendar is not processed");
        }

        // ── Step 1: frequency — selects DAILY (no window) vs window logic, and the
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

        // ── Step 3: WHICH period (and, for a data source, WHAT date ends it). Calculated before the
        //    eligibility checks so the period is stored on every decision, skipped ones included.
        RunDates dates;
        try {
            dates = itemType == ItemType.REPORT
                ? calculateLastPeriod(schedule, businessDate, calendar)
                : calculateDataSourcePeriod(schedule, businessDate, calendar);
        } catch (RuntimeException e) {
            // Unparseable dayLag, or the calendar failing during the calculation.
            return ScheduleDecision.notEvaluable(businessDate, null, e.getMessage());
        }

        // ── Step 4: WHEN — eligibility on the Business Date.
        LocalDate freqRunDate    = null;
        LocalDate maxFreqRunDate = null;
        try {
            boolean daily = RunScheduleConfig.DAILY.equals(frequency);
            if (!daily) {
                // Non-DAILY: locate the run window first, so a malformed freqRunDay/maxFreqRunDay is
                // reported (NOT_EVALUABLE) on any day, including a weekend.
                if (schedule.hasFreqRunDay()) {
                    freqRunDate = calculateFreqRunDate(schedule, businessDate, calendar);
                }
                if (schedule.hasMaxFreqRunDay()) {
                    maxFreqRunDate = calculateMaxFreqRunDate(schedule, businessDate, calendar);
                }
            }

            // Business-day gate — EVERY frequency (owner decision D6; for DAILY this is BAU Part 1 §6,
            // for non-DAILY it deliberately overrides Part 1 §6). The Business Date itself must be a
            // business day in the item's calendar. A skipped day is not "caught up" separately: a
            // non-DAILY item is simply picked up on the next business day that is still inside its
            // window; a DAILY item's missed day is never processed (BAU).
            if (!calendar.isBusinessDay(businessDate)) {
                return ScheduleDecision.skip(ScheduleDecision.Status.NON_BUSINESS_DAY, dates,
                    businessDate, freqRunDate, maxFreqRunDate,
                    businessDate + " is not a business day in calendar '" + schedule.calendarKey + "'");
            }

            if (!daily) {
                // Non-DAILY: freqRunDay <= Business Date <= maxFreqRunDay, inclusive both ends.
                if (freqRunDate != null && businessDate.isBefore(freqRunDate)) {
                    // Before the window opens → NOT YET ELIGIBLE; checked again next execution.
                    return ScheduleDecision.skip(ScheduleDecision.Status.NOT_YET_ELIGIBLE, dates,
                        businessDate, freqRunDate, maxFreqRunDate,
                        "business date " + businessDate + " is before freqRunDay "
                        + schedule.freqRunDay + " (" + freqRunDate + ")");
                }
                if (maxFreqRunDate != null && businessDate.isAfter(maxFreqRunDate)) {
                    // After the window closes → EXPIRED; this period is no longer processed
                    // automatically. (--manualOverrun does not change this — owner decision D1.)
                    return ScheduleDecision.skip(ScheduleDecision.Status.EXPIRED, dates,
                        businessDate, freqRunDate, maxFreqRunDate,
                        "business date " + businessDate + " is after maxFreqRunDay "
                        + schedule.maxFreqRunDay + " (" + maxFreqRunDate + ")");
                }
                // No maxFreqRunDay → no explicit upper bound: eligible until the period calculation
                // above rolls over to the next reporting cycle (BAU).
            }
            return ScheduleDecision.eligible(dates, freqRunDate, maxFreqRunDate);

        } catch (RuntimeException e) {
            // Unparseable run-day expression, or the calendar failing while locating the window.
            return ScheduleDecision.notEvaluable(businessDate, dates, e.getMessage());
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
     *   <li>{@code WD-n} → n business days back. E.g. Business Date Tue 2026-09-08, Mon 09-07 a
     *       holiday, {@code WD-1} → Fri 2026-09-04 (holiday and weekend skipped).</li>
     *   <li>{@code CAL-n} → n calendar days back, no business-day adjustment (Monday with
     *       {@code CAL-1} → Sunday).</li>
     *   <li>OPEN QUESTION: blank {@code dayLag} — assumed lag 0 (period = Business Date).</li>
     *   <li>A <b>positive</b> lag ({@code WD+n}/{@code CAL+n}) is an error → not evaluable
     *       (owner decision D4).</li>
     *   <li>Anything else (e.g. {@code CD-1}, {@code wd-1}) → not evaluable.</li>
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
     *   <li>QUARTERLY → {@code yyyyMMddqq}: first date of the quarter + zero-padded quarter number
     *       (owner decision D3): Q1 2026 → {@code 2026010101}, Q3 2026 → {@code 2026070103}.</li>
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
                return new Period(quarterStart, quarterEnd, quarterPeriodId(quarterStart, quarter));
            }
            case RunScheduleConfig.ANNUALLY -> {
                int year = currentPeriod ? businessDate.getYear() : businessDate.getYear() - 1;
                return new Period(LocalDate.of(year, 1, 1), LocalDate.of(year, 12, 31), year);
            }
            default -> throw new IllegalArgumentException(
                "unsupported frequency " + schedule.frequency);
        }
    }

    /** QUARTERLY period id {@code yyyyMMddqq} — fits an int until the year 2147. */
    static int quarterPeriodId(LocalDate quarterStart, int quarter) {
        long yyyymmdd = quarterStart.getYear() * 10000L + quarterStart.getMonthValue() * 100L
                      + quarterStart.getDayOfMonth();
        return Math.toIntExact(yyyymmdd * 100 + quarter);
    }

    private static LocalDate applyDailyLag(String dayLag, LocalDate businessDate, BusinessCalendar calendar) {
        if (dayLag == null) {
            return businessDate;
        }
        if (DAILY_LAG_POSITIVE.matcher(dayLag).matches()) {
            throw new IllegalArgumentException(
                "DAILY dayLag '" + dayLag + "' is positive; a DAILY lag must be WD-n or CAL-n "
                + "(DATE_SCHEDULING_RULES.md D4) — this should have been filtered out where the "
                + "parameters are stored");
        }
        Matcher m = DAILY_LAG.matcher(dayLag);
        if (!m.matches()) {
            throw new IllegalArgumentException(
                "DAILY dayLag '" + dayLag + "' is not WD-n or CAL-n");
        }
        int n = Integer.parseInt(m.group(2));
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

    // ── Report lookback: period ids of earlier periods of a data source ───────

    /**
     * Period ids a report must find COMPLETED for one of its data sources — the report's own
     * period (offset 0) back through {@code lookbackTo}. Offset {@code k <= 0} is {@code -k}
     * periods before the report's period, stepping in the <b>data source's own frequency</b>:
     * <ul>
     *   <li>MONTHLY — one calendar month per step ({@code yyyyMM}); 0..-11 = 12 months.</li>
     *   <li>QUARTERLY — one calendar quarter per step ({@code yyyyMMddqq}, D3).</li>
     *   <li>ANNUALLY — one year per step ({@code yyyy}).</li>
     *   <li>DAILY — one business day per step ({@code yyyyMMdd}) for a {@code WD-n} or blank
     *       {@code dayLag}, one calendar day for {@code CAL-n}. OPEN QUESTION (contract Part 3
     *       #9): the business-day step for a blank lag is an assumption.</li>
     * </ul>
     * Offset 0 is always {@code reportDates.periodId} itself. The data source must have the same
     * frequency as the report (contract Part 3 #7: no mapping between differing frequencies), else
     * {@link IllegalArgumentException} — a lookback is never computed on a guess.
     *
     * @param dataSource the data source's run schedule (its frequency/dayLag steps the periods)
     * @param report     the report's schedule (only its frequency is compared)
     * @param calendar   the data source's calendar — used only for DAILY business-day steps
     * @return period ids ordered from {@code lookbackFrom} down to {@code lookbackTo}
     */
    public static List<Integer> lookbackPeriodIds(RunScheduleConfig dataSource, RunScheduleConfig report,
                                                  RunDates reportDates, int lookbackFrom, int lookbackTo,
                                                  BusinessCalendar calendar) {
        if (dataSource.frequency == null || !dataSource.frequency.equals(report.frequency)) {
            throw new IllegalArgumentException("lookback needs the data source and the report to have "
                + "the same frequency (data source " + dataSource.frequency + ", report "
                + report.frequency + ")");
        }
        List<Integer> ids = new ArrayList<>();
        for (int offset = lookbackFrom; offset >= lookbackTo; offset--) {
            ids.add(offset == 0 ? reportDates.periodId
                                : periodIdBack(dataSource, reportDates.periodStart, -offset, calendar));
        }
        return ids;
    }

    /** Period id {@code steps} periods before the one starting at {@code anchor}. */
    private static int periodIdBack(RunScheduleConfig schedule, LocalDate anchor, int steps,
                                    BusinessCalendar calendar) {
        switch (schedule.frequency) {
            case RunScheduleConfig.DAILY -> {
                boolean calendarDays = schedule.dayLag != null && schedule.dayLag.startsWith("CAL-");
                LocalDate day = calendarDays ? anchor.minusDays(steps)
                                             : minusBusinessDays(anchor, steps, calendar);
                return day.getYear() * 10000 + day.getMonthValue() * 100 + day.getDayOfMonth();
            }
            case RunScheduleConfig.MONTHLY -> {
                YearMonth month = YearMonth.from(anchor).minusMonths(steps);
                return month.getYear() * 100 + month.getMonthValue();
            }
            case RunScheduleConfig.QUARTERLY -> {
                LocalDate quarterStart = LocalDate.of(anchor.getYear(),
                    quarterStartMonth(anchor.getMonthValue()), 1).minusMonths(3L * steps);
                return quarterPeriodId(quarterStart, (quarterStart.getMonthValue() - 1) / 3 + 1);
            }
            case RunScheduleConfig.ANNUALLY -> {
                return anchor.getYear() - steps;
            }
            default -> throw new IllegalArgumentException("unsupported frequency " + schedule.frequency);
        }
    }

    private static int quarterStartMonth(int month) {
        return ((month - 1) / 3) * 3 + 1;
    }

    // ── Command-line dates (not used by the scheduled flows) ─────────────────

    /**
     * Run dates taken straight from {@code --runDate}/{@code --periodStart}/{@code --periodEnd}/
     * {@code --periodId}. <b>Not used to run a data source or report</b> — an item without a
     * schedule is not processed (owner decision D5). Kept for callers outside the scheduled flows
     * that only need token values from the command line, e.g.
     * {@code QueryParameterResolver.resolve(template, params, options)} in the legacy example.
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
            /** Run it (subject to the caller's COMPLETED check). */
            ELIGIBLE,
            /** Non-DAILY, before the freqRunDay date. Skip; recheck next execution. */
            NOT_YET_ELIGIBLE,
            /** Non-DAILY, after the maxFreqRunDay date. Skip; period no longer processed automatically. */
            EXPIRED,
            /** Business Date is a weekend/holiday in the item's calendar (any frequency). Skip. */
            NON_BUSINESS_DAY,
            /** Configuration or calendar problem. Skip this item and report it; re-evaluated next execution. */
            NOT_EVALUABLE
        }

        public final Status    status;
        /**
         * The calculated Business Date + Reporting Period (including the period id). Set for every
         * status whenever the period could be calculated — skipped statuses carry the period they
         * would have processed. Null only for some {@link Status#NOT_EVALUABLE} decisions. Use
         * it to run only when {@link #shouldRun()}.
         */
        public final RunDates  dates;
        public final LocalDate businessDate;
        /** Null when not configured or not reached (DAILY, or evaluation stopped earlier). */
        public final LocalDate freqRunDate;
        /** Null when not configured or not reached. */
        public final LocalDate maxFreqRunDate;
        /** Human-readable reason, for logs and failure notifications. */
        public final String    detail;

        private ScheduleDecision(Status status, RunDates dates, LocalDate businessDate,
                                 LocalDate freqRunDate, LocalDate maxFreqRunDate,
                                 String detail) {
            this.status         = status;
            this.dates          = dates;
            this.businessDate   = businessDate;
            this.freqRunDate    = freqRunDate;
            this.maxFreqRunDate = maxFreqRunDate;
            this.detail         = detail;
        }

        static ScheduleDecision eligible(RunDates dates, LocalDate freqRunDate, LocalDate maxFreqRunDate) {
            return new ScheduleDecision(Status.ELIGIBLE, dates, dates.runDate, freqRunDate,
                maxFreqRunDate, "eligible");
        }

        static ScheduleDecision skip(Status status, RunDates dates, LocalDate businessDate,
                                     LocalDate freqRunDate, LocalDate maxFreqRunDate, String detail) {
            return new ScheduleDecision(status, dates, businessDate, freqRunDate, maxFreqRunDate,
                detail);
        }

        /** {@code dates} may be null when the period itself could not be calculated. */
        static ScheduleDecision notEvaluable(LocalDate businessDate, RunDates dates, String detail) {
            return new ScheduleDecision(Status.NOT_EVALUABLE, dates, businessDate, null, null,
                detail);
        }

        static ScheduleDecision notEvaluable(LocalDate businessDate, String detail) {
            return notEvaluable(businessDate, null, detail);
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
