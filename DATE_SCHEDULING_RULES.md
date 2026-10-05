# Finance Automation — Date & Scheduling Rules (CONTRACT)

> ## ⛔ READ THIS BEFORE TOUCHING ANY DATE / SCHEDULING CODE
>
> This file is the **owner-approved contract** for how run dates, reporting periods and
> run-eligibility are calculated. It is the source of truth for
> `beam-utils/.../RunDateCalculator.java` and everything that calls it
> (`DataSourcePipelineFactory`, `ReportPipelineFactory`, `PipelineSequenceFactory`,
> `RunScheduleConfig`, `RunDates`, `BigQuerySourceConfigRepository.parseRunSchedule`).
>
> **Rules for anyone (human or AI agent) working in this repo:**
>
> 1. **Read this whole file before changing any date calculation.** Not just the part you think is relevant.
> 2. **Never edit Part 1.** It is the original BAU description, verbatim, as supplied by the owner.
> 3. **Never edit Part 2** (owner decisions) or change code to behave differently from Parts 1–2
>    without the owner's explicit approval in the conversation. Add new decisions to Part 2 only after
>    the owner has stated them, with the date.
> 4. **If a requested change, a test, or a piece of existing code contradicts this file — stop and ask
>    the owner.** Do not resolve the contradiction yourself, and do not "fix" this file to match the code.
> 5. Where this file does not say what BAU does, do **not** guess silently: either fail loudly
>    (`NOT_EVALUABLE`) or record the assumption in Part 3 so the owner can confirm it.
> 6. Every behaviour here must have a test in `RunDateCalculatorTest`.

---

# PART 1 — BAU rules (verbatim, owner-supplied)

# Finance Automation — Date & Scheduling Logic

## 1. Overall Processing Model

The framework uses the six scheduling attributes to answer two separate questions:

**WHEN should this item run?**

Controlled primarily by:

`frequency + freqRunDay + maxFreqRunDay + calendarKey`

**WHICH reporting/data period should it process?**

Controlled primarily by:

`frequency + dayLag + dateType + calendarKey`

The simplified flow is:

**Business Date**
→ determine whether the item is eligible today
→ calculate the reporting period
→ check whether that period is already Completed
→ create/reuse a task
→ process the data source/report
→ mark Completed or Failed.

Every scheduler execution evaluates the rules again from scratch. A skipped item is not permanently recorded as "skipped." Items that are not yet eligible or that fail can therefore be reconsidered on subsequent executions while their run window remains open.

---

# 2. `frequency`

`frequency` defines both the **reporting grain** and the scheduling rules used for the item.

Supported values are:

| Frequency   | Period Produced  | Scheduling Behaviour                                |
| ----------- | ---------------- | --------------------------------------------------- |
| `DAILY`     | `yyyyMMdd`       | Runs only on valid business days                    |
| `MONTHLY`   | `yyyyMM`         | Runs within a configured run window                 |
| `QUARTERLY` | Quarter period ID | Runs within a configured window based on quarter month lookup |
| `ANNUALLY`  | `yyyy`           | Runs within a configured window based on annual month lookup  |

For **DAILY**, there is no `freqRunDay/maxFreqRunDay` window. The current Business Date must be a valid business day according to `calendarKey`.

For all other frequencies, the Business Date must fall inside:

**freqRunDay ≤ Business Date ≤ maxFreqRunDay**

If the frequency is missing or unsupported, the item does not successfully create a task and will be evaluated again on subsequent executions. `WEEKLY` is not implemented.

---

# 3. `freqRunDay`

For reports this configuration is actually stored as **`freqDtl`**; data sources use **`freqRunDay`**.

It defines the **first day on which a non-daily item becomes eligible to run**.

The important format is `WD±n`:

* `WD+1` = first business day of the relevant month
* `WD+5` = fifth business day
* `WD-1` = last business day
* `WD-2` = second-last business day
* `WD+0` = last calendar day of the previous month, effectively making the item eligible from the first day of the current month

Business-day calculations use `calendarKey`, so weekends and holidays are excluded when determining the run date.

For example:

**Monthly + freqRunDay = WD+3**

means:

> Do not process this monthly item until the third business day of the month.

Before that date the item is **NOT YET ELIGIBLE** and is checked again on the next scheduler execution.

Once the run day is reached, it remains eligible until `maxFreqRunDay`, assuming it has not already completed.

For `DAILY`, `freqRunDay` is ignored.

---

# 4. `maxFreqRunDay`

`maxFreqRunDay` defines the **last day on which the item can automatically run or retry**.

The window is inclusive:

**freqRunDay ≤ Business Date ≤ maxFreqRunDay**

Example:

`freqRunDay = WD+3`
`maxFreqRunDay = WD+5`

means the item may run/retry from the **third through fifth business day**.

This creates three important states:

**Before freqRunDay → NOT YET ELIGIBLE**

The item is checked again next execution.

**Inside the window → ELIGIBLE**

Run if it has not already Completed. Failed attempts can also be retried.

**After maxFreqRunDay → EXPIRED**

The period is no longer automatically processed.

If `maxFreqRunDay` is blank, there is no explicit upper bound; eligibility effectively continues until the reporting period rolls over.

`ManualForceRun=true` bypasses the maximum-date and Completed checks, but does not bypass the initial run-day requirement or DAILY business-day check.

---

# 5. `dayLag`

`dayLag` determines **WHICH period is processed**. It does **not determine whether the item is eligible to run today**.

Its behavior differs significantly between DAILY and non-DAILY frequencies.

### DAILY

The lag moves backward from Business Date.

`WD+n` = go backward `n` business days,

`CAL+n` = go backward `n` calendar days,

For example:

Business Date = Tuesday Sep 8
Sep 7 = holiday
`dayLag = WD+1`

produces:

**Period = Friday Sep 4**

because the holiday and weekend are skipped.

A calendar lag does not make this adjustment. For example, Monday with `CAL+1` can produce Sunday as the reporting period.

### MONTHLY / QUARTERLY / ANNUALLY

The numeric amount is effectively ignored.

Only the sign/prefix determines whether the framework uses the **current or previous period**:

`WD-...` or `CAL-...` → **current period**

Anything else, including blank or positive values → **previous period**

Therefore:

`WD-1`, `WD-5`, and `CAL-3`

all effectively mean **current period**.

While:

blank, `WD+1`, `WD+5`, `CAL+1`

all effectively mean **previous period**.

This behavior is important to preserve during migration even though the configuration appears to imply a numeric lag.

---

# 6. `calendarKey`

`calendarKey` identifies the business calendar used by the item.

The calendar defines:

* weekends
* holidays

It influences several calculations:

**DAILY eligibility:** if Business Date is a weekend/holiday, the item is skipped for that date.

**WD-based run days:** `WD+5`, `WD-1`, etc. count only valid business days.

**DAILY WD lag:** business-day lag skips weekends and holidays.

**`lastBusDayMonth`:** determines the final business day of a month.

An important distinction is that **non-DAILY items do not require the Business Date itself to be a business day**.

For example, a MONTHLY item whose eligibility window includes Saturday can run on Saturday if the scheduler executes that day.

An invalid or missing `calendarKey` causes that item to fail eligibility/processing and it will be evaluated again on subsequent executions.

---

# 7. `dateType`

There are actually **two different `dateType` concepts** in the framework.

## A. Data Source `runSettings.dateType`

This determines the **period-end date used by a monthly data source**.

For monthly periods:

`lastBusDayMonth`
→ last business day according to `calendarKey`

Anything else, normally `lastDayMonth`
→ last calendar day.

Example:

Period = January 2026
January 31 = Saturday

`lastDayMonth` → January 31
`lastBusDayMonth` → January 30

This resulting date can then be used in:

* datasource query parameters
* B&C queries
* file matching
* report-to-datasource period resolution

It does **not** control whether the item is eligible to run.

For DAILY, QUARTERLY and ANNUALLY periods, this data-source `dateType` does not alter the period end.

## B. Report `paramReplace[].dateType`

This controls **what date/value gets substituted into report queries, transformations, files and other parameters**.

Important values include:

`periodId`
→ use the calculated reporting period.

`RunDate`
→ use the Business Date when the task is processed.

Other values
→ use the reporting period-end date, optionally adjusted using `periodOffset`.

This `dateType` affects parameters passed downstream; it does **not affect scheduling eligibility**.

The report-level `runSettings.dateType` is not actually used.

---

# 8. Business Date vs Reporting Period

The distinction between these two concepts is fundamental.

### Business Date

The Business Date answers:

> **Should this automation run today?**

Normally it is today's date in the configured framework timezone, although `ManualRunDateId` can override it.

It is compared against the configured run window.

### Reporting Period

The Reporting Period answers:

> **Which period's data should this execution process?**

It is calculated from:

**Business Date + frequency + dayLag + calendar**

For example:

Business Date = September 3
Frequency = MONTHLY
dayLag = blank

may produce:

**Reporting Period = August 2026**

So the automation can **run in September while processing August data**.

This distinction should remain explicit in the new platform.

---

# 9. Run / Skip / Recheck Behaviour

The effective decision model is:

### NOT YET ELIGIBLE

Business Date is before `freqRunDay`.

→ Do nothing now
→ Check again next scheduler execution.

### ELIGIBLE

Business Date is within the run window.

→ Calculate period
→ Check existing status.

If already `Completed`:

→ Do not run again.

If not Completed:

→ Create or reuse a task and RUN.

### FAILED

A processing attempt failed.

→ Status remains non-Completed
→ Retry on a later scheduler execution while still inside the eligibility window.

There is no implemented retry limit.

### EXPIRED

Business Date exceeds `maxFreqRunDay`.

→ Stop attempting that reporting period automatically.

### DAILY NON-BUSINESS DAY

Weekend/holiday according to `calendarKey`.

→ Skip that date.

There is no automatic catch-up of the missed DAILY period.

### PERIOD ROLLOVER

If there is no `maxFreqRunDay`, the previous period effectively stops being considered when calculation moves into the next reporting cycle.

The framework therefore behaves more like repeated **eligibility evaluation** than a persistent skip/retry queue.

---

# 10. Process → Data Source → Report Consequence

Data-source tasks are created before report tasks, but each item has its **own scheduling eligibility**.

A data source being skipped does **not automatically skip its dependent report**.

Instead:

**Data Source**
→ evaluated against its own frequency/window
→ calculates its period
→ loads data
→ becomes `Completed`

**Report**
→ evaluated independently against its own schedule
→ determines which datasource periods it requires
→ checks whether each required datasource is `Completed`

If required data is missing:

→ Report fails
→ Report can be retried on subsequent eligible executions.

Therefore scheduling eligibility does not directly propagate from a datasource to a report; **dependency completion is validated when the report actually executes**.

---

# 11. Simplified Decision Flow

The migration can conceptually model each item as:

**1. Determine Business Date**

↓

**2. Read frequency and calendar**

↓

**3. Check WHEN eligibility**

DAILY:
`Is Business Date a valid business day?`

Non-DAILY:
`freqRunDay ≤ Business Date ≤ maxFreqRunDay?`

↓

**4. Calculate WHICH period**

Using:

`frequency + dayLag`

↓

**5. Check status for Item + Period**

`Completed?`

YES → SKIP / already processed

NO → continue

↓

**6. Create or reuse task**

↓

**7. Process**

Data Source:
calculate period-end using `dateType`
→ load data
→ B&C
→ Completed / Failed

Report:
resolve required datasource periods
→ verify dependencies Completed
→ transformations/report processing
→ Completed / Failed

↓

**8. If Failed**

Retry on a future scheduler execution while still eligible.

---

# 12. Attribute Relationship Summary

| Attribute | Primary Purpose | Controls WHEN? | Controls WHICH period/date? | Main Consequence |
| --- | --- | :---: | :---: | --- |
| `frequency` | Defines scheduling/period grain | ✓ | ✓ | Selects DAILY vs window logic and period format |
| `freqRunDay` / `freqDtl` | Start of run window | ✓ | – | Before it = wait/recheck |
| `maxFreqRunDay` | End of run window | ✓ | – | After it = expired |
| `dayLag` | Select reporting period | – | ✓ | Determines current/previous/day-level period |
| `calendarKey` | Business calendar | ✓ | ✓ | Holidays/weekends affect WD calculations |
| `dateType` | Determine period-end/parameter date | – | ✓ | Controls dates passed into datasource/report processing |

The key relationship is therefore:

**`frequency + freqRunDay + maxFreqRunDay + calendarKey`**
→ **Can it run?**

**`frequency + dayLag`**
→ **Which period should it process?**

**`dateType + calendarKey`**
→ **What actual date represents that period downstream?**

**Status**
→ **Does it still need to run?**

Together these determine:

**WAIT → RUN → COMPLETED**

or

**WAIT → RUN → FAILED → RETRY → COMPLETED**

or

**WAIT → window expires → no automatic processing.**

---

# Migration-Critical Behaviors

The new implementation should deliberately preserve or deliberately change the following behaviors rather than accidentally reinterpret them:

1. Reports use `freqDtl`; data sources use `freqRunDay`.
2. DAILY items use a business-day gate; non-DAILY items do not.
3. `dayLag` behaves fundamentally differently for DAILY versus MONTHLY/QUARTERLY/ANNUALLY.
4. Non-DAILY `dayLag` uses essentially only its sign/prefix, not its numeric magnitude.
5. `dateType` does not control scheduling; it controls downstream date representation.
6. Data-source and report `dateType` are separate concepts.
7. Failed/non-Completed items can retry repeatedly while their eligibility window remains open.
8. Skip state is not persisted; eligibility is recalculated every scheduler execution.
9. Reports are independently scheduled and validate datasource completion at execution time.
10. DAILY missed periods are not automatically caught up.
11. `ManualForceRun` bypasses max-window and Completed checks, but not every eligibility check.
12. Several behaviors identified by the analysis appear to be implementation defects or quirks and should be explicitly classified as **"preserve for parity" or "fix during migration"** rather than unknowingly copied.

---

# PART 2 — Owner decisions for THIS framework (override or extend Part 1)

Each entry is a decision the owner stated explicitly. Part 1 stays untouched; where an entry
conflicts with Part 1, **the entry wins**, and the conflict is named here so nobody "corrects" it.

### D1 — `--manualOverrun` does not affect eligibility or date calculation (2026-10-01)

*Overrides Part 1 §4 and Migration-Critical #11 (`ManualForceRun` bypassing the max window).*

`--manualOverrun` is only about **storage and overwriting**: it bypasses the `COMPLETED` check and
makes the new run supersede the previously stored data. It never changes **whether** an item is
eligible or **which dates** are calculated. When forcing a re-run, the caller passes the Business
Date (`--runDate`) that is eligible for the item under the normal rules; with `--manualOverrun` set,
a date before `freqRunDay`, after `maxFreqRunDay`, or a DAILY non-business day is still skipped.

### D2 — `--periodId` is not required; it is calculated (2026-10-01)

The period ID is calculated from the Business Date + `frequency` + `dayLag` (Part 1 §2, §5, §8)
whenever a Business Date is available (`--runDate`, or today in the framework time zone) and the item
has a run schedule. It is stored alongside the dates (`RunDates.periodId`) and is what `DaRefer` /
`RptRefer` `per_id` are keyed by. If `--periodId` is also passed for a scheduled item, the calculated
value is used.

*(The earlier remark here that an item with no run schedule "still needs `--periodId`" is
superseded by D5: such an item is not processed at all.)*

### D3 — Quarterly period ID format (2026-10-01)

Part 1 §2 says only "Quarter period ID". The format is **`yyyyMMddqq`**: `yyyyMMdd` is the **first
date of the quarter** (e.g. `20260101`) and `qq` the zero-padded quarter number `01`–`04`.
Example: first quarter of 2026 → `2026010101`.

**Confirmed by the owner, 2026-10-01:** quarters are calendar quarters and the fourth quarter of 2026
is `2026100104` (the earlier example "third quarter → `2026100103`" was a slip). So Q1 → `…0101`,
Q2 → `…0402`, Q3 → `…0703`, Q4 → `…1004` for the matching year.

### D4 — DAILY `dayLag` is `WD-n` / `CAL-n`; a positive lag is an error (2026-10-01)

*Overrides Part 1 §5 "DAILY", which writes the DAILY lag as `WD+n` / `CAL+n`.*

For a DAILY item the lag is written with a **minus**: `WD-n` = go back `n` business days, `CAL-n` = go
back `n` calendar days — the calculation itself is exactly the one Part 1 §5 describes (business-day
lag skips weekends and holidays; calendar lag does not), only the sign convention differs.
Example: Business Date Tue Sep 8, Sep 7 a holiday, `dayLag = WD-1` → Fri Sep 4.

A **positive** DAILY lag (`WD+n`, `CAL+n`) is not expected — it should be filtered out where the
parameters are stored — but as a safety check it is an **error**: the item is not processed and the
standard failure notification is raised (an email when `--opsFailureEmail` is configured). Blank
DAILY lag remains an assumption (Part 3 #3). Non-DAILY `dayLag` (the prefix rule) is unchanged.

### D5 — An item with no schedule or no calendar is not processed (2026-10-01)

Every data source and report must have a run schedule **and** an existing calendar. An item with no
run schedule (no `run_details`), a schedule without a `calendarKey`, or a `calendarKey` that cannot be
resolved to a calendar is **not processed** and raises the standard failure notification — there is no
fallback to dates passed on the command line. Consequently `--periodId`, `--periodStart` and
`--periodEnd` are never used to run a data source or report; the period and its dates are always
calculated.

### D6 — Every frequency needs the Business Date to be a business day (2026-10-05)

*Overrides Part 1 §6 ("non-DAILY items do not require the Business Date itself to be a business
day", incl. its MONTHLY-on-a-Saturday example), and Migration-Critical #2 ("DAILY items use a
business-day gate; non-DAILY items do not").*

The business-day gate applies to **every** frequency: if the Business Date is a weekend day or a
holiday in the item's calendar, the item is skipped (`NON_BUSINESS_DAY`) — a MONTHLY, QUARTERLY or
ANNUALLY item is **not** run on a Saturday even if that date lies inside its `freqRunDay`..`maxFreqRunDay`
window. Everything else is unchanged: the window is still compared on calendar dates; nothing is
persisted for a skip, so the item runs on the next business day that is still inside the window (a
DAILY item's missed day is still not caught up, Part 1 §9). Weekends and holidays are not business
days in every `WD±n` calculation (run days, DAILY lag, `lastBusDayMonth`) — as Part 1 already states.

---

# PART 3 — Implementation notes and OPEN items (not part of the contract)

*Safe to update as the implementation changes, but every item under "Assumptions" is a guess that
needs the owner's confirmation. Remove an item only when the owner confirms or corrects it.*

### Pending confirmation

None. (P1 quarterly example and P2 negative DAILY lag, raised 2026-10-01, were resolved by the
owner the same day — see D3 and D4.)

### Assumptions where Part 1 is silent

1. **QUARTERLY/ANNUALLY window month** — Part 1 mentions a "quarter/annual month lookup" without its contents. Assumed: `WD` run days are counted in the first month of the Business Date's calendar quarter (Jan/Apr/Jul/Oct) / in January.
2. **`WD+n` larger than the month's business days** — assumed the count continues into the next month (and `WD-n` into the previous) rather than failing.
3. **DAILY with blank `dayLag`** — assumed lag 0 (period = Business Date). (A positive DAILY lag is an error — D4.)
4. **Non-DAILY with blank `freqRunDay`** — assumed no lower bound.
5. **Exact, case-sensitive matching** of `WD`/`CAL` prefixes, `lastBusDayMonth` and frequency names (`monthly` ≠ `MONTHLY`).
6. **Report `paramReplace[].dateType`** (`periodId` / `RunDate` / period end + `periodOffset`, Part 1 §7B) — not implemented; its config shape and `periodOffset` unit are not described.
7. **A report whose data sources have a different frequency** — a report looks its data sources up by its own `periodId`; mapping between differing frequencies is not implemented.
8. **`maxFreqRunDay`** is read as a `WD±n` string like `freqRunDay`; a bare number (`5`) is NOT_EVALUABLE.

### How the contract maps to code

| Contract | Code |
|---|---|
| Business Date | `DateUtils.resolveRunDate(options)` → `RunDates.runDate` (`--runDate`, else today in `--businessTimeZone`, default UTC) |
| WHEN / WHICH / WHAT | `RunDateCalculator.evaluate*()` → `ScheduleDecision` (`ELIGIBLE`, `NOT_YET_ELIGIBLE`, `EXPIRED`, `NON_BUSINESS_DAY`, `NOT_EVALUABLE`) |
| Calendar | `BusinessCalendarProvider.forKey(calendarKey)` → default `BigQueryBusinessCalendarProvider` (parameter table, group `FINACOE_Calendars`; SPI can override) |
| Status check ("Completed?") | callers: `DataSourcePipelineFactory.filterByCheckpoint`, `ReportPipelineFactory.isAlreadyCompleted` |
| Skips not persisted | a skipped item writes nothing; the next run re-evaluates |
| Period carried on every decision | `ScheduleDecision.dates` is set whenever the period could be calculated — **including skipped** statuses — so the period is visible next to the reason it was skipped. Only `NOT_EVALUABLE` may have `dates == null`. Callers must still only *use* the dates when `shouldRun()` |
