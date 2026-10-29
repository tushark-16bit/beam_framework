# BnC (Balance & Control) — rules and migration reference

This file is the reference for how **Balance & Control (BnC)** works for data sources, and how it is
implemented in this framework.

* **Part 1** is the legacy behaviour, transcribed from the owner's reverse-engineering document
  *"BnC (Balance & Control) Framework: Reverse-Engineering and Migration Specification"*
  (repository `finacoe-reporting-framework` v1.3.8), supplied as photographs. It is the source of truth
  for *what BnC means*. Nothing in Part 1 is invented; where a photo was cut off or illegible that is said
  explicitly.
* **Part 2** is how this BigQuery/Beam framework maps those rules. Every decision there is *ours*, listed
  with its reasoning so the owner can overrule it. Items marked **OPEN** need an owner answer.
* **Part 3** lists what the photographs did not contain.

Evidence labels used by the source document: **[V]** confirmed in code · **[I]** strongly implied by code but
not proven · **[U]** not enough evidence.

> Rule for agents: do not change BnC behaviour away from Part 1 without the owner's approval. If a request or
> test contradicts Part 1, stop and ask. Behaviour Part 1 does not define goes in Part 2 as an assumption
> (never a silent guess).

---

# Part 1 — Legacy behaviour (source document)

## 1.0 Scope and evidence limits

Production data-source configurations live in the database table `PrcsParmDtl`. They are not in the legacy
repository (`docs/`, the README and `bootstrap/` contain no BnC configuration), so the per-data-source check
inventory (section D) covers only the two configurations that were supplied. `ParametersUtil` / `DBUtils`
come from the external `ace.utils` jar, so their exact behaviour is [I].

## 1.1 Three things to know first

1. **There is no single BnC engine.** There are four separate implementations that behave differently. Which
   one runs depends on the source type and on two framework switches: `dbPaginated` (query sources) and
   `LargeFileFlag` (file sources).
2. **Each data source runs at most one BnC check.** That check compares a count, plus optionally one amount,
   or reads one SQL-provided status. Arrays of checks are not supported. [V]
3. **Reports never read BnC results.** They only require `DaRefer.StaCd = 'Completed'`. BnC affects reports
   only through that status value. [V]

## A. How BnC works, end to end

### A.1 Which implementation runs

Routing happens in `ProcessWorkItemService.java`. Configuration is loaded on every task run through
`ParameterUtil.GetParameterParent("dataSource", keyName)`, which reads `PrcsParmDtl` where
`ParmNm LIKE 'Rpt_Framework_DataSources%'`. [V]

| sourceType | Switch | Implementation | BnC code |
|---|---|---|---|
| query | `dbPaginated ≠ true` | **Q1 – legacy query** | `ReportDBUtils.LoadQueryToDB` → private `checkSQLBalanceAndControlValues` (L278, L916) |
| query | `dbPaginated = true` | **Q2 – paginated query** | `DBSourcer.ingestData` → `performBalanceAndControls` → `ReportDBUtils.checkSQLBalanceAndControlValue` (DBSourcer L1079) |
| file | `LargeFileFlag ≠ Yes` | **F1 – legacy file** (xlsx/xlsm, xls, csv/tsv) | `RptFileUtil.checkExcelFileBalanceAndControlValues` / `checkXlsFileBalanceAndControlValues` (L669, L1945) |
| file | `LargeFileFlag = Yes` | **F2 – streaming file** | `FileSource` → `Xlsx/Xls/CsvFileChunkHandler.performBnC` → `BalanceAndControlUtil.balAndCtrlSmbXlsx` / `balAndCtrlXls` / `balAndCtrlXlsCsv` |
| api | – | **No BnC** | `ApiUtil.callDataSource` has no BnC or DaRefer logic [V] |

### A.2 Actual execution sequences

**Q1 – legacy query [V]**
1. `createDaReferRecord` sets status **Loading**.
2. `paramReplace` is applied to `queryText`, and the query runs on `dataLocation`.
3. If zero rows are returned and `allowZeroRows=false`, the run fails.
4. Rows are saved to DaRec.
5. Status is set to **Completed**.
6. BnC runs. On a mismatch, status becomes **Failed B&C**; whether an exception is thrown depends on
   `errorOnBnCFailure`. On a technical error, status becomes **Failed B&C** and an exception is always thrown.

**Q2 – paginated query [V]**
1. Status **Loading**.
2. Rows are streamed in chunks of `maxFetchSize` into DaRec.
3. Status **Loaded**.
4. BnC returns `{BnC_Status, BnC_Text}` and never throws for mismatches or errors. The result sets status
   **Completed** or **Failed B&C**, and `BalAndCntlSmryTx` is written.
5. If `errorOnBnCFailure` is set and BnC failed, a `BusinessException` is thrown.

**F1 – legacy file [V]**
1. The file is located, then `createDaReferRecord(filePath)` sets status **Loading**.
2. Rows are parsed and saved to DaRec. Status **Completed**.
3. BnC runs. Mismatch and error handling is the same as Q1, with different tolerance rules (see B and E).
4. For CSV only, `"There is no BnC for CSV Files."` is written first, and then BnC runs anyway.

**F2 – streaming file [V]**
1. Status **Loading**. Data is streamed to DaRec. In `flushRemaining`, xlsx/xls briefly set status Completed.
2. Status **Loaded**.
3. `performBnC` sets **Completed** or **Failed B&C**, writes the text, and throws if `errorOnBnCFailure` is set
   and BnC failed.
4. Afterwards the file is archived (`fileArchiveDirectory`).
5. On any exception, the outer XLS handler overwrites the status with **Failed**. See defect **D-6**.

In every path, BnC runs **after the destination load and before the ACE post-processing step**.
Transformations are report-level and run later. [V]

## B. Configuration attribute reference

Where the attributes live: query sources read them from `querySettings`; file sources read them from
`fileSettings`. Neither block inherits from, or overrides, the other. Configuration is evaluated on every task
execution. [V]

| Attribute (constant) | Used by | Meaning and valid values | Default / missing / invalid | Effect |
|---|---|---|---|---|
| `bncType` (`B_C_TYPE`) | F1, F2 only. Ignored by query paths | Case-insensitive: `SQL Query Totals`, `Count Only`, `None`. Any other value, including blank and `Excel BnC Sheet`, selects sheet mode (fall-through else) | Blank = sheet mode | Selects the source-extraction strategy. CSV with exactly `Excel BnC Sheet` is rejected as a failure |
| `B&C_Column` (`FILE_IMPORT_B_C_Column`) | F1, F2 | Destination column. Matched case-insensitively against the header `ColumnName`, then against the short `DataColName` (A, B, …) | Blank: in sheet mode = `"BnC not Configured!"` (see F). In Count Only it defaults to A | The destination SUM is taken from this column |
| `Dest_BnC_Column` (`QUERY_DEST_B_C_Column`) | Q1, Q2 | Destination column. Matched against result-set column names only (no `DataColName` fallback) | Blank: Q1 + flag false = skip; Q1 + flag true = error; Q2 = FAIL | Same as above |
| `Src_BnC_Query` (`QUERY_SRC_B_C_QUERY`) | Q1, Q2 (source totals); F1, F2 (only when `bncType=SQL Query Totals`) | Query paths: SQL run on `querySettings.dataLocation`. **Column 1 = count, column 2 = amount (by position). If only one column is returned, it is treated as the amount.** File paths: SQL run on `APPBPDWANALYTICS`. **Every `?` is replaced by the DaId.** Must return columns named `bnctext` and `bncstatus` | See E and F | Source control values |
| `bncVarianceAllowed` (`B_C_VARIANCE_ALLOWED`) | All | Absolute tolerance on the **amount only** | Blank = 0. Non-numeric: Q1/F1/F2 log it and use 0; **Q2 throws while building the run, before the load** (status stays/becomes Loading) | Q1/Q2: `abs(diff) ≤ V + 0.001`. F1/F2: `abs(diff) ≤ V` |
| `errorOnBnCFailure` (`ERROR_ON_BNC_FAILURE`) | All | `"true"` or anything else | **Blank or missing = true.** Any value other than `true` (case-insensitive) = false | Only controls whether an exception is raised, i.e. the task outcome. It never changes the validation result |
| `paramReplace[] {replaceText, dateFormat}` | Q1, Q2 (both `queryText` and `Src_BnC_Query`) | Replaces each `replaceText` with the **end-of-period date** formatted with `dateFormat` | Required (missing makes `GetParameterValue_JsonArray(...,true)` fail) [I] | Provides `%periodid%` and similar placeholders. They are not built in |
| `dataLocation` | Q1, Q2 | ACE datasource name used for both the data query and the source BnC query | Required | – |
| `allowZeroRows` | Q1, Q2 | `"false"` makes a zero-row load fail. Anything else allows zero rows | – | With zero rows: Q1 continues to BnC. Q2: see E-3 |
| `runSettings.dateType`, `runSettings.calendarKey` | All | Used by `findLastDateOfPeriod(periodId, dateType, calendarKey)` | Required | Sets the date used for placeholder substitution and the file date pattern |
| `workSheetNum` | F1, F2 Excel | **1-based data sheet. The BnC sheet is always the next sheet** (0-based index = `workSheetNum`) | Required for Excel | Sheet location. Not separately configurable |
| `LargeFileFlag`, `dbPaginated` | Framework parameters (`this.parameters`) | – | Default = legacy | Switch F1/F2 and Q1/Q2. Selects the implementation |
| Hard-coded values | – | `B_C_VARIANCE_ZERO_OFFSET = 0.001`; `DEFAULT_BNC_COLUMN = "A"`; SQL-mode PASS marker `"SUCCESS"`; BnC cells **A2 = count** and **B2 = amount** | – | – |

There are no other BnC attributes in this codebase. [V] (all constants are in `ApplicationConstants.java`)

## C. Implementation matrix

| Impl. | BnC type | Source control value | Destination value | Comparison | Failure handling |
|---|---|---|---|---|---|
| **Q1** | (not used) | `Src_BnC_Query` on `dataLocation`: column 1 = count, column 2 = amount; **last row wins**; SQL NULL → 0 | `GET_COUNT_AMOUNT_VALIDATION` over DaRec for this DaId: `COUNT(*)` rows, `SUM(CAST(col AS FLOAT))` with non-numeric → 0 | count strings (0 decimals) equal **and** `abs(sA−dA) ≤ V+0.001` **and** both sides read successfully | Status Completed is set **before** BnC. Mismatch: Failed B&C + throw (flag true) or Failed B&C and the task succeeds (flag false). Technical error: always throws |
| **Q2** | (not used) | same | same | same; any configuration or technical problem = FAIL | Loaded, then Completed or Failed B&C; throw only if flag is true |
| **F1 / F2** | sheet (default / `Excel BnC Sheet`) | Sheet `workSheetNum+1`, row 2: A = count, B = amount | Same DaRec aggregation on `B&C_Column` | count equal and `abs(sA−dA) ≤ V` (**no 0.001 offset**) | As Q1 (F1) or as Q2 (F2), except XLS-F2 where the outer catch writes Failed |
| **F1 / F2** | `Count Only` | Rows loaded by the parser (`totalRowCnt`) | `COUNT(*)` of DaRec rows | count equal (amount ignored) | same |
| **F1 / F2** | `SQL Query Totals` | `Src_BnC_Query` with `?` → DaId, run on `APPBPDWANALYTICS`; reads `bnctext` and `bncstatus` | Built into the SQL by the author | PASS iff `upper(bncstatus)` contains `"SUCCESS"`; the framework applies no variance | same |
| **F1 / F2** | `None` | – | – | Skipped | F1 writes `"No BnC check required."` and status stays Completed. **F2 fails (defect D-1)** |
| **API** | – | – | – | – | – |

**What is shared:** the destination SQL (`QueryStrings.GET_COUNT_AMOUNT_VALIDATION`); the count rule (format
with `NumberFormat`, 0 fraction digits, default **HALF_EVEN** rounding, compare as strings); the result text
format, status values, the `errorOnBnCFailure` default, and the persistence column.
**What differs by source:** the variance offset, skip-vs-fail handling, status before BnC, and exception
propagation.

## D. BnC check inventory (from the two supplied configurations only)

| Data source | Check | Inputs | Formula | Threshold | PASS when | FAIL consequence |
|---|---|---|---|---|---|---|
| RRDD Investments (query) | Count + `N_SETTLEMENT_MONTHLY_USD_AMT` | `Src_BnC_Query`: column 1 `TotalCount`, column 2 `TotalBalAmt` (the alias names do not matter) | `fmt0(srcCnt)==fmt0(COUNT(DaRec rows)) ∧ abs(TotalBalAmt − SUM(dest col)) ≤ 1.0 + 0.001` | 1.0 (absolute currency) | both conditions hold | `errorOnBnCFailure` not supplied, so it counts as true: status Failed B&C, task cancelled, failure email, reports blocked |
| Excel source (file) | Count + `TotalRevenueAmt` | BnC sheet A2 (count), B2 (amount) | `fmt0(A2)==fmt0(COUNT rows) ∧ abs(B2 − SUM(TotalRevenueAmt)) ≤ 1.0` | 1.0 | both hold, **and A2 ≠ 0** | true: as above |

Notes from the source document:

* For the RRDD example, the latest-feed/latest-version selection (`max(N_FEED_KEY)`,
  `max(N_RRDD_VERSION_ID)`, `RRDD_LOAD_COMPLETED`) is entirely in the configured SQL. The engine does not do it. [V]
* `%periodid%` only works if `querySettings.paramReplace` contains e.g.
  `{"replaceText":"%periodid%","dateFormat":"yyyyMMdd"}` [I]. **The value substituted is the end date of the
  period, not the PeriodId itself.** [V]
* The SQL uses `dual`/`to_date`, so `dataLocation` must be an Oracle DSN. [I]
* Within a data source, the count and amount always run together and produce one combined outcome. They cannot
  be disabled independently (Count Only drops the amount). [V]

## E. BnC execution algorithm (pseudocode, matches legacy behaviour)

```
cfg      = PrcsParmDtl[dataSource keyName]                      # every run
lastDate = findLastDateOfPeriod(periodId, runSettings.dateType, runSettings.calendarKey)
errFlag  = isBlank(cfg.errorOnBnCFailure) OR equalsIgnoreCase(cfg.errorOnBnCFailure, "true")
V        = parseDouble(cfg.bncVarianceAllowed) else 0           # Q2: throw if unparsable

## ---------- QUERY (Q1 / Q2) ----------
srcSql  = applyParamReplace(cfg.Src_BnC_Query, lastDate)        # no '?' substitution
destCol = cfg.Dest_BnC_Column
if srcSql blank OR dataLocation blank OR destCol blank:
    Q1: if errFlag -> THROW (-> Failed B&C) ; else write "BnC not Configured! ..." ; status stays Completed (SKIP)
    Q2: FAIL ("BnC not Configured correctly! ...")
dataCol = header.find(ColumnName ~= destCol).DataColName        # result-set names only
    if not found -> error / FAIL
(src cnt, src amt) = runOnDsn(dataLocation, srcSql): last row; col1=cnt, col2=amt (1 col => amt only); NULL->0
(dst cnt, dst amt) = runOnDsn(APPBPDWANALYTICS, GET_COUNT_AMOUNT_VALIDATION(dataCol, daId)); NULL->0
pass = checkedSrc AND checkedDst
       AND fmt0(srcCnt)==fmt0(dstCnt)
       AND abs(srcAmt-dstAmt) <= V + 0.001

## ---------- FILE (F1 / F2) ----------
type = cfg.bncType (case-insensitive)
if csv AND type=="Excel BnC Sheet": ERROR -> Failed B&C, throw (both F1 & F2)
switch type:
  "None":            F1 -> text "No BnC check required.", status unchanged (Completed), return
                     F2 -> FAIL (D-1)
  "SQL Query Totals": if query blank: F1 errFlag?THROW:skip ; F2 FAIL
                     row  = runOnDsn(APPBPDWANALYTICS, query.replace("?", daId))   # last row
                     text = row.bnctext ?: "bnctext column missing in SQL output"
                     pass = upper(row.bncstatus ?: "").contains("SUCCESS")
                     (F1 additionally computes dest if B&C_Column set; errors there THROW)
  "Count Only":      srcCnt = rowsLoaded ; dst = COUNT(*) ; pass = fmt0 equal
  else (sheet):      if B&C_Column blank: F1 errFlag?THROW:skip ; F2 FAIL
                     sheet = workbook[workSheetNum]             # 0-based => next sheet
                     missing sheet / row2 / A2 / B2 -> FAIL (+error)
                     srcCnt = num(A2), srcAmt = num(B2)         # F1: formulas evaluated; F2-xlsx: cached
                     srcCnt == 0 -> FAIL "do not contain valid B&C data"
                     dataCol = ColumnName match else DataColName match
                     dst  = DaRec COUNT/SUM(dataCol)
                     pass = fmt0(srcCnt)==fmt0(dstCnt) AND abs(srcAmt-dstAmt) <= V    # no 0.001

## ---------- PERSIST + DECIDE ----------
text = (pass ? "Status: Matched, " : "Status: Failed, ")
       + "Source Count: x, Source Amount: y, Destination Count: z, Destination Amount: w"
       + "[ (Amt variance allowed +/- V)]"
Q2/F2:  status = pass ? Completed : Failed B&C ; write BalAndCntlSmryTx = text ; if !pass && errFlag THROW
Q1/F1:  (status already Completed)
        if !pass:
            if errFlag THROW("Status: Failed, ...")  (text NOT persisted) -> catch sets Failed B&C
            else status = Failed B&C ; write text
        else write text
        if technical errorString non-empty -> THROW -> Failed B&C
THROW    => ProcessWorkItemService: failure email (if configured), workManager.cancelTask(taskId)
no THROW => post-ACE process, finalizeTask, completion email (even if Failed B&C)
```

## F. Validation decision matrix

| Condition | Q1 (legacy query) | Q2 (paginated) | F1 (legacy file) | F2 (streaming file) |
|---|---|---|---|---|
| **PASS** | Completed, text "Matched"; task done; reports OK | same | same | same |
| **Mismatch, flag true or blank** | Failed B&C, text not written, task cancelled, failure email | Failed B&C, text written, task cancelled | as Q1 | as Q2; XLS ends as Failed (D-6) |
| **Mismatch, flag false** | Failed B&C, text written, task completes, completion email; **reports blocked** | same | same | same |
| **Source SQL or destination error** | Failed B&C, always throws, task cancelled | Failed B&C (shown as mismatch, error text lost, D-4); throws only if flag | Failed B&C, always throws | as Q2 |
| **Missing column / query** | flag true: Failed B&C + cancel. flag false: SKIP, stays Completed | FAIL as a mismatch | as Q1 (sheet mode / SQL mode) | FAIL as a mismatch |
| **`bncType=None`** | n/a | n/a | SKIP, Completed | FAIL (D-1) |
| **Only count or only amount differs** | FAIL (combined outcome) | FAIL | FAIL | FAIL |
| **`abs(diff) = V` exactly** | PASS (also PASS up to V+0.001) | PASS | PASS (inclusive, no offset) | PASS |
| **Source query returns no rows** | src = 0/0, `checkedSource = true` → PASS only if destination is 0/0 | same | SQL mode: `bncstatus` missing → FAIL | same |
| **Destination has no rows** | `COUNT 0, SUM NULL → 0` | same (if `allowZeroRows`) | CSV: BnC skipped, Completed. Excel: compared with 0 | CSV: BnC skipped, stays Loaded → reports blocked (D-7). Excel: `getDouble` on missing key → FAIL |
| **Load fails before BnC** | Failed | Failed | Failed | Failed |

There is no separate SKIP or ERROR status value. The only statuses are **Loading, Loaded, Completed, Failed,
Failed B&C, Reloaded**. [V]

## G. Result persistence and status model

Single table: `DaRefer` (`DaId, SrceNm, VsnNo, PerId, FlNm, BalAndCntlSmryTx, StaCd, CreatTs, LstUpdtTs`).
No per-check rows, no numeric columns, no audit table. [V] (`QueryStrings.java`)

* **Result storage:** the result is free text in `BalAndCntlSmryTx` (`UPDATE_DAREFER_BAL_CNTLSUM`).
  Source/destination values and the variance are embedded in that text. Process step results go into the ACE
  task `processResults` (not a table).
* **Status updates:** `UPDATE_DAREFER_STATUS`.
* **`createDaReferRecord` (insert vs. update): [V]**
  * If a **Completed** row exists for the source and period (forced re-run): its DaRec rows are deleted, the
    **same DaId is reused**, and status is set to Loading. The old BnC text stays until it is overwritten.
  * Otherwise: the DaRec rows of the latest DaId are deleted, and a new row is inserted with
    `VsnNo = count+1` and status Loading.
  * This means failed versions are kept in DaRefer as history, but only the latest version keeps its data rows.

**Update sequences**

| Scenario | Q1 / F1 | Q2 / F2 |
|---|---|---|
| PASS | Loading → Completed → text "Matched" | Loading → Loaded → Completed → text |
| FAIL (flag false) | Loading → Completed → Failed B&C → text | Loading → Loaded → Failed B&C → text |
| FAIL (flag true) / ERROR | Loading → Completed → Failed B&C (text not updated) | Loading → Loaded → Failed B&C → text (→ Failed for XLS-F2) |
| RETRY | New DaRefer version (Loading …); earlier failed version keeps its status and text | same |
| Trigger reload | `PreProcessingService`: Completed → Reloaded; RptRefer → READY; the next load inserts a new version | same |

## H. Retry and revalidation rules

BnC is **never re-run on its own**. Every retry is a full reload: new DaRefer version, previous DaRec deleted,
data re-extracted, then BnC. [V]

How retries happen (`WorkItemCreationService`):

* On each scheduler run, if the source is inside its run window (frequency, `freqRunDay`, `maxFreqRunDay`,
  `dayLag`, `calendarKey`, holidays) and `GET_DAREFER_STACD ≠ Completed`, a task is reused or created. [V]
* Cancelled (EXITED) tasks are not reused, so a new task is created. [V]
* A task that completed with Failed B&C (flag false) also leads to a new task on the next run. [V]
* **No retry limit.** There is a `// TODO` at the place where a limit would go. `recFailureCnt` (the number of
  DaRefer versions) is only passed to the failure email. Retries stop only when the date passes
  `maxFreqRunDay` or the status becomes Completed. [V]
* Completed blocks re-runs unless both `ManualRunKey` and `ManualForceRun` are set. A forced run reuses the
  Completed DaId. If that run then fails BnC, the previously good source becomes Failed B&C, which blocks
  reports. [V]
* Trigger reload (`dataTriggerReloadSettings`) compares the source summary with the data-source summary. On a
  difference, it sets the status to Reloaded, which causes a full reload and BnC on the next run. [V]
* No check results are reused between runs. [V]

## I. Data source → report dependency flow

```
Data load → BnC → DaRefer.StaCd
Report task (scheduled independently; skipped only if RptRefer=Completed)
  → processReport → getReportDataSourceStatus:
      for each dataSources[] entry, for each period in lookBackFrom..lookBackTo:
          GET_COMPLETED_DAID_VALUE (StaCd='Completed' AND SrceNm AND PerId)
  → any missing → BusinessException "is missing data sources: …" → report task cancelled
  → otherwise RptDaMap rows created, transformations run                       [V] (BusinessLogicUtil.java)
```

A report is not skipped as soon as BnC fails. It runs on its own schedule and fails its dependency check. On a
later scheduler run, the report task is recreated or released while `RptRefer ≠ Completed`, so it recovers once
the source becomes Completed. [I] `RptRefer` status on report failure was not traced. [U]

## J. Worked examples (values are illustrative)

### J.1 Database SQL BnC (RRDD, Q1/Q2, V = 1.0, flag blank = true)

Setup: period 20260930. `paramReplace` `%periodid%→yyyyMMdd` resolves the placeholder to `'20260930'`. The query
runs on `dataLocation` and returns `TotalCount=1200`, `TotalBalAmt=5,000,000.40`. Destination: DaRec for the
DaId gives `COUNT = 1200`, `SUM(N_SETTLEMENT_MONTHLY_USD_AMT short column) = 4,999,999.60`.

| Case | Calculation | Result | Status / consequence |
|---|---|---|---|
| PASS | diff 0.80 ≤ 1.001 | Matched | Completed; report can map the DaId |
| FAIL | destination amount 4,999,998.90 → diff 1.50 | Failed | Failed B&C; exception; task cancelled; failure email; report fails "missing data sources"; retried on next schedule within the window |
| ERROR | ORA error in `Src_BnC_Query` | – | Q1: Failed B&C + exception. Q2: `checkedSource=false` → FAIL, text `"Source Count: NA…"`, then the same consequence |

### J.2 Excel BnC Sheet (xlsx, workSheetNum=1, V = 1.0, flag true)

Setup: sheet 2, row 2: `A2 = 350`, `B2 = =SUM(...)` → 12,345.67. The `TotalRevenueAmt` header maps to column C.
DaRec `COUNT = 350`, `SUM(C) = 12,345.00`.

| Case | Calculation | Result | Status / consequence |
|---|---|---|---|
| PASS | diff 0.67 ≤ 1.0 | Matched | Completed |
| FAIL | SUM = 12,344.50 → diff 1.17 | Failed | Failed B&C, cancelled (in F1, text not written) |
| ERROR | sheet 2 missing; A2 blank or zero; B2 = "1,234" (parse error); or `TotalRevenueAmt` not in header | Failed | Failed B&C, exception, cancelled |

## K. Migration rules (testable statements)

**Common rules**

* **K1.** IF `errorOnBnCFailure` is blank or `true` (case-insensitive) THEN the flag counts as true; any other
  value counts as false.
* **K2.** IF counts are equal after rounding to 0 decimals (HALF_EVEN) AND the amount condition holds AND both
  sides were read THEN PASS AND status Completed.
* **K3.** IF PASS = false AND flag = false THEN status Failed B&C AND the task completes successfully
  (completion email) AND dependent reports fail their dependency check.
* **K4.** IF PASS = false AND flag = true THEN status Failed B&C AND the task is cancelled AND a failure email
  is sent (if `emailSettings` is configured).
* **K5.** IF status ≠ Completed AND inside the run window THEN the next scheduler run does a full reload with a
  new DaRefer version; there is no retry cap.
* **K6.** A report needs a Completed DaRefer for every (source, look-back period) pair; otherwise the report
  fails.

**Source-specific rules**

* **Q1/Q2.** Amount PASS iff `abs(src−dst) ≤ V + 0.001`. Source column 1 = count, column 2 = amount, last row
  wins, NULL → 0.
* **F1/F2.** Amount PASS iff `abs(src−dst) ≤ V`. Sheet = `workSheetNum+1`, A2/B2, **A2 = 0 → FAIL**.
* **SQL Query Totals (files).** PASS iff `bncstatus` (upper-cased) contains `"SUCCESS"`. `?` → DaId. Runs on
  `APPBPDWANALYTICS`. `paramReplace` is not applied.
* **Q1/F1, flag false, configuration missing:** SKIP; status stays Completed. **Q2/F2, configuration
  missing:** FAIL.
* **Q1/F1, flag true, failure:** `BalAndCntlSmryTx` is not updated.
* **CSV + Excel BnC Sheet:** FAIL with an exception (regardless of the flag).

## L. Regression tests (minimum set)

Columns: source · configuration · source values (count / amount) · destination values · expected result ·
status · downstream. Rows 2 and 3 were photographed out of focus; their digits are marked. In rows 16–18 the
"configuration" column holds the scenario; the photographed cell layout of rows 12 and 16–18 is ambiguous, so
each is read by its meaning.

| # | Source | Configuration | Source values | Destination values | Expected result | Status | Downstream |
|---|---|---|---|---|---|---|---|
| 1 | Q1 | V=1, flag blank | 100 / 1000.00 | 100 / 998.999 | PASS (1.001 ≤ 1.001) | Completed | report runs |
| 2 | Q1 | V=1 | 100 / 1000 | 100 / 998.99x *(last digit(s) illegible)* | FAIL | Failed B&C | cancelled, report fails |
| 3 | F1 xlsx | V=1 | 100 / 1000 *(illegible)* | 100 / 9xx.xxxx *(illegible; the photo reads "…0005")* | FAIL (no offset) | Failed B&C | – |
| 4 | Q2 | V=0 | 100.4 / 0 | 100 / 0 | PASS (both round to "100") | Completed | – |
| 5 | any | flag=false | count 100 vs 99 | – | FAIL | Failed B&C | task COMPLETED, report blocked |
| 6 | Q1 | `Dest_BnC_Column` blank, flag=false | – | – | SKIP | Completed | report runs |
| 7 | Q2 | same (blank column) | – | – | FAIL | Failed B&C | – |
| 8 | F1 | `bncType=None` | – | – | SKIP | Completed | – |
| 9 | F2 | `bncType=None` | – | – | FAIL (see D-1 — [U]) | Failed B&C | – |
| 10 | F2 xls | mismatch, flag true | – | – | FAIL | Failed *(D-6)* | – |
| 11 | CSV | `Excel BnC Sheet` | – | – | ERROR | Failed B&C | cancelled |
| 12 | F2 CSV | 0 rows | – | – | no BnC *(D-7)* | Loaded | report blocked |
| 13 | F SQL | `bncstatus='UNSUCCESSFUL'` | – | – | PASS *(D-8)* | Completed | – |
| 14 | Excel | `A2 = 0` | – | – | FAIL | Failed B&C | – |
| 15 | Excel | `B2 = "1,234"` | – | – | ERROR / FAIL | Failed B&C | – |
| 16 | any | prior failure, rerun inside the window, data fixed | – | – | PASS | new VsnNo Completed | report succeeds on next run |
| 17 | forced run on a Completed source | mismatch | – | – | FAIL | same DaId becomes Failed B&C | report blocked |
| 18 | after `maxFreqRunDay` | – | – | – | no retry | stays failed | – |

## M. Open questions, defects and migration risks

**Defects to verify. Do not fix them silently; decide whether to keep each one.**

* **D-1.** In F2, `None` is passed through `bnCPassed` from `preValidationForBnCCheck`, so it returns FAIL. In F1 it
  is a SKIP. (`BalanceAndControlUtil.preValidationForBnCCheck` sets `bnCDone=true`, `bnCPassed=true`, but
  `checkExpostCheckBnCControlVariables` is never called and the text is `"BnC Type is 'None'…"`. Recheck:
  `checkExcelFileBalanceAndControlValues` takes the else branch with `bnCPassed=true`, so F2 `None` may actually
  PASS and set Completed.) Evidence needed: a unit test of `balAndCtrlXlsCsv` with `None`. Treat as **[U]** until
  tested; the matrix above assumes FAIL.
* **D-2.** The tolerance offset differs between paths (+0.001 for query, none for file).
* **D-3.** In Q1/F1 the status is Completed before BnC runs, so there is a window where a report could map the
  source. Also, a flag=false configuration gap leaves the source Completed.
* **D-4.** In Q2 (`checkSQLBalanceAndControlValue`), technical error text is overwritten by the `"Source Count:
  NA"` text.
* **D-5.** With flag true in Q1/F1, the BnC text is never persisted.
* **D-6.** The outer catch in the XLS streaming handler overwrites Failed B&C with Failed.
* **D-7.** In F2, a zero-row CSV stays Loaded permanently.
* **D-8.** `contains("SUCCESS")` treats `"UNSUCCESSFUL"` as a pass.
* **D-9.** `GET_DAREFER_STACD` has no `ORDER BY`, so with several versions the eligibility status read is
  unpredictable.
* **D-10.** `query.replace("?", daId)` replaces every `?` in the SQL.
* **D-11.** `ISNUMERIC` accepts values such as `$`, which `CAST AS FLOAT` then rejects, so the destination query
  errors.
* **D-12.** In F2, `getDouble` is called on a missing amount key (destination SUM NULL), which errors.
* **D-13.** A forced re-run that fails BnC overwrites a previously Completed source.
* **D-14.** In Q2, an invalid variance throws before the load. Q1 uses 0 instead.

**Unknowns and the evidence needed**

* Production configurations (all `Rpt_Framework_DataSources%` rows): needed to build the real inventory, see
  whether any data source uses SQL Query Totals, Count Only or flag=false, and see the actual `paramReplace`
  entries. Source: an export of `PrcsParmDtl`.
* The `dbPaginated` and `LargeFileFlag` values in production: these decide which implementation actually runs.
* `ace.utils` behaviour: whether `ParametersUtil.GetParameterValue(..., false)` returns `""` for a missing key,
  and whether `DBUtils.retrieveOneColumnOneRow` returns the first row. Source: the library's code.
* ACE `cancelTask` → EXITED behaviour, and `RptRefer` status on report failure. Source: ACE engine documentation,
  and the `processReport` catch block.
* Driver column-name case for `bnctext`/`bncstatus` (`JSONObject.has` is case-sensitive). Source: a SQL Server
  JDBC test.

**Migration design (recommended, separate from current behaviour).** Split the work into: Resolver →
SourceExtractor (SQL-positional / SQL-status / ExcelSheet / RowCount) → DestinationExtractor (DaRec COUNT/SUM) →
Validator → Persistence (DaRefer text plus status) → WorkflowDecision (flag → throw/cancel). To keep behaviour
identical, the Validator needs a configurable `toleranceOffset` (0.001 for query, 0 for file) and a
per-implementation `onMissingConfig` (SKIP vs FAIL). Optional improvements, such as per-check result rows,
numeric columns, a retry cap and an exact `bncstatus` match, would change behaviour and need sign-off first.

---

# Part 2 — How this framework implements BnC

This framework runs on BigQuery + Beam instead of Oracle + ACE tasks. The concepts map one-to-one; the
mechanics do not. Everything below is a decision made here, not a statement from Part 1.

## 2.1 Mapping of concepts

| Legacy | This framework |
|---|---|
| `DaRec` for a DaId | `DaRec` (BigQuery) for a `da_id`; one row per page of ≤250 source rows |
| `DaRefer.StaCd` | `da_refer.sta_cd` |
| `Completed` | `COMPLETED` |
| `Failed B&C` | `FAILED_BNC` |
| `Failed` | `FAILED` (anything that is not a BnC outcome, e.g. a crashed finalize step) |
| `Loading` | `LOADING` |
| `Loaded`, `Reloaded` | not used. The finalize step runs right after the last insert, so there is no gap to represent |
| `BalAndCntlSmryTx` free text | `bal_and_cntl_smry_tx`, now a JSON object (existing keys kept) with an added `text` key holding the legacy-format sentence from section E |
| Task cancelled / exception | no equivalent — see decision B4 |
| Source query on `dataLocation` | a BigQuery query run by the finalize step (see `bnc.source_query`) |
| `paramReplace[]` | the framework's own tokens: standard `{periodStart}`/`{periodEnd}`/`{periodId}`/`{runDate}` plus the source's `date_params` (see B5) |
| `workSheetNum` | `file_details.sheet_index` (see B6) |
| `allowZeroRows=false` | `min_row_count` ≥ 1 (existing key; no new BnC key) |
| `runSettings.dateType/calendarKey` | `run_details_json` (existing) |
| `bncVarianceAllowed` | `bnc.variance_allowed` |
| `errorOnBnCFailure` | `bnc.error_on_failure` |
| `bncType`, `Src_BnC_Query`, `B&C_Column` / `Dest_BnC_Column` | `bnc.type`, `bnc.source_query`, `bnc.dest_column` |

## 2.2 Configuration (the `bnc` object in `parameters_val_json`)

```json
"bnc": {
  "type":             "SOURCE_QUERY",
  "source_query":     "SELECT COUNT(*) AS total_count, SUM(amt) AS total_amt FROM `p.d.t` WHERE per = '{periodLabel}'",
  "dest_column":      "N_SETTLEMENT_MONTHLY_USD_AMT",
  "variance_allowed": 1.0,
  "error_on_failure": true
}
```

| Key | Values | Default |
|---|---|---|
| `type` | `SOURCE_QUERY` (query sources: Q1/Q2), `EXCEL_SHEET` (xlsx/xls: sheet mode), `COUNT_ONLY`, `SQL_STATUS` ("SQL Query Totals"), `NONE` | none: **no `bnc` object = no BnC check** |
| `source_query` | SQL text. `SOURCE_QUERY`: column 1 = count, column 2 = amount, last row wins, NULL → 0, one column = amount only. `SQL_STATUS`: every `?` replaced by the `da_id`; must return `bnctext` and `bncstatus` | – |
| `dest_column` | column whose SUM is taken from the stored rows | `COUNT_ONLY` ignores it; otherwise required |
| `variance_allowed` | number ≥ 0, absolute tolerance on the amount. Non-numeric → the config load **fails** (Q2 rule) | `0` |
| `error_on_failure` | `true` / `false` (anything other than `true`, case-insensitive, = false; blank = true — K1) | `true` |

## 2.3 Decisions

**B1 — One engine, not four.** The legacy differences that are *accidents* (see B15 for each defect) are
not carried over, except where B15 says a defect is kept. The differences that are *deliberate* are kept, keyed on the source kind, not on a
switch:

* query sources (`SOURCE_QUERY`): amount PASS iff `abs(src−dst) ≤ V + 0.001` (K: Q1/Q2);
* file sources (`EXCEL_SHEET`): amount PASS iff `abs(src−dst) ≤ V` (K: F1/F2).

The comparison is done in `BigDecimal` (not `double`) so the inclusive boundaries in K and regression test 1
(`1.001 ≤ 1.001`) are exact.

**B2 — Missing configuration.** An absent `bnc` object = no BnC (as an absent `bnc_rules_json` today). A
`bnc` object that is present but incomplete (e.g. `SOURCE_QUERY` without `source_query` or `dest_column`)
**FAILS** (Q2/F2 behaviour, not Q1/F1's SKIP), because this framework streams like Q2/F2 and a half-written
check silently skipping is the worse failure. `type: NONE` is a SKIP for every source (D-1: the legacy F2 behaviour is
unverified — [U] — see B15). **OPEN:** confirm FAIL-not-SKIP for a missing configuration, and SKIP for `NONE`.

**B3 — Technical errors.** A failing source query, destination query, unreadable sheet, etc. gives
`FAILED_BNC` with an `ERROR:` message in the text (not disguised as a mismatch — fixes D-4). *There is no
separate ERROR status* (Part 1, F). The existing `bnc_rules_json` check keeps its current behaviour
(an infrastructure error in it records `FAILED`).

**B4 — `error_on_failure`.** There is no task to cancel and no exception to throw: the finalize step runs on a
Dataflow worker after `main()` has returned, and rethrowing would only trigger bundle retries (CLAUDE.md §18
invariant 14). The flag therefore controls only the notification: `true`/blank → the failure email (K4) and an
ERROR log line; `false` → no failure email, WARN log line (K3). In **both** cases the status is `FAILED_BNC`
and dependent reports are blocked (K3), and the result text is always persisted (Q2/F2 behaviour). The flag
never changes the validation result (B). **OPEN:** confirm.

**B5 — `paramReplace`.** Not reimplemented. `source_query` takes the same tokens as the data query —
standard `{periodStart}`/`{periodEnd}`/`{periodId}`/`{runDate}`, the source's `date_params`, and custom
tokens — resolved in the driver JVM before submission (the finalize step runs on a worker). This applies to
`SQL_STATUS` queries too, where the legacy framework applied no `paramReplace` (a legacy limitation, not a
rule anyone relied on; a query without `{…}` placeholders is unaffected). A `date_params` entry can be limited
to the BnC query with `"apply_to": ["bnc_query"]` (the data query is `bq_query`). To keep the legacy meaning of
`%periodid%` (the **end date** of the period, not the period id — section D note) configure
`{"token": "%periodid%", "source": "PERIOD_END", "format": "yyyyMMdd"}`. Token matching is case-sensitive, so
this does not collide with the built-in `%periodId%` (the integer id).

**B6 — Excel sheet.** The data sheet is `file_details.sheet_index` (0-based in this framework); the BnC sheet
is the next sheet (`sheet_index + 1`), cells A2 = count and B2 = amount, formulas evaluated, `A2 = 0` → FAIL —
all as K/E. Not separately configurable. `EXCEL_SHEET` on a CSV file is a FAIL (K: regardless of the flag).

**B7 — Destination.** `COUNT(*)` of the stored rows and `SUM` of `dest_column` over them, scoped by `da_id`.
Non-numeric values count as 0. `dest_column` is matched case-insensitively: for query sources against the
stored row keys; for file sources against the header names, then the column letter (A, B, …).

**B8 — `COUNT_ONLY`.** Rows the pipeline read vs `COUNT(*)` of stored rows, formatted to 0 decimals HALF_EVEN
and compared as strings; amount ignored. This is the always-on `row_count_mismatch` check; `COUNT_ONLY` simply
makes it the only BnC check.

**B9 — Retry and status.** Unchanged by BnC: a source that is not `COMPLETED` is retried by the scheduler
under the existing run-window rules (K5), and a report needs `COMPLETED` for every period it reads (K6) —
already how `ReportPipelineFactory` works.

**B10 — Coexistence.** `bnc_rules_json` (a fixed expected total with a percentage tolerance) is a different,
framework-native check and is unchanged. It may be used alongside `bnc`.

**B11 — When it runs.** BnC compares the source's control values with the rows **as stored**, so it runs
right after the row-count checks and **before** `data_transform_query` (legacy: BnC after the load,
transformations later). A failed BnC therefore also stops the transform from running. `bnc_rules_json`, which
runs after the transform, keeps checking whatever is stored at that point.

**B12 — Where the source's location comes from.** The legacy `dataLocation` (which database the source query
runs on) has no per-source equivalent: the source query is a BigQuery query, and its job location is
`bq_settings.fetch_options.location` (default: BigQuery's own default). Fully qualified table names in the SQL
select the project.

**B13 — One-column source query.** Following Part 1 literally: if `source_query` returns one column it is the
**amount** and the source count is 0. Because the count and amount are one combined check (Part 1, section D),
such a check can only pass for an empty source. Return two columns.

**B14 — Row order.** "Last row wins" is applied to the order BigQuery returns; a source query should return one
row.

**B15 — Each legacy defect (M), and what this framework does.** Section M says to decide, not fix silently.
These are the decisions taken; **OPEN** ones need the owner.

| Defect | This framework | Parity with legacy? |
|---|---|---|
| D-1 `None` in F2 (FAIL vs PASS, [U]) | `type: NONE` is a SKIP for every source (status unaffected) | **differs** from the matrix (FAIL) — OPEN: confirm |
| D-2 offset +0.001 query / 0 file | kept (decision B1) | yes |
| D-3 Completed before BnC / config gap leaves Completed | `COMPLETED` is written only after BnC passes (no window); an incomplete `bnc` fails (B2) | differs — deliberate |
| D-4 error text overwritten by "Source Count: NA" | the error text is kept inside the NA sentence (`ERROR: …`) | differs — deliberate |
| D-5 flag true: text never persisted | text is always persisted (B4) | differs — deliberate |
| D-6 XLS outer catch overwrites with Failed | none: a failed check is `FAILED_BNC` | differs — deliberate |
| D-7 zero-row CSV stays Loaded | `COUNT_ONLY` works with 0 rows (0 = 0 passes); no `Loaded` status exists | differs — deliberate |
| D-8 `contains("SUCCESS")` passes `"UNSUCCESSFUL"` | **kept** — `SQL_STATUS` still uses `contains` (test `evaluateStatus` pins it) | yes — OPEN: switch to an exact match? |
| D-9 status read without `ORDER BY` | n/a: `getLatest`/`isCompleted` order by `lst_updt_ts DESC` | n/a |
| D-10 every `?` replaced | **kept** (also inside string literals) | yes |
| D-11 `ISNUMERIC` accepts `$` then the CAST errors | `SAFE_CAST` makes a non-number NULL, which the SUM ignores (counts as 0, no error) | differs — deliberate |
| D-12 F2 missing amount key errors | an empty destination sums to 0 | differs — deliberate |
| D-13 forced re-run failing BnC overwrites a Completed source | a re-run is a new DaRefer row; the previous COMPLETED row and (until the new run completes) its DaRec rows are kept; reports read the *latest* row, so they are blocked while it is `FAILED_BNC` | same outcome (report blocked), different mechanism |
| D-14 Q2 throws on a bad variance, Q1 uses 0 | throws at config load (Q2 rule, `BncConfig`) | yes (Q2) |

Regression rows 9–18 against this framework: 9 differs (D-1); 10, 12 differ (D-6, D-7 not carried); 11, 14, 15 match
(failed check, `FAILED_BNC`); 13 matches (D-8 kept); 16 matches (a retry is a new `DaRefer` version); 17 matches
in outcome (D-13); 18 matches (`EXPIRED` — no run after `maxFreqRunDay`).

**Not done, on purpose** (the document's list of improvements needing sign-off): per-check result rows, numeric
result columns, a retry cap, an exact `bncstatus` match. The result stays in the existing `bal_and_cntl_smry_tx`
column, as JSON.

## 2.4 Where it lives in code

| Piece | Class |
|---|---|
| config model (`bnc` object) | `beam-core/.../model/validation/BncConfig.java` |
| default BigQuery settings (`bq_settings`: `fetch_options`, `storage_options`) | `beam-core/.../model/bq/BqSettings.java` |
| pure comparison logic (all of K and the F matrix) | `beam-utils/.../BncEvaluator.java` (tests: `BncEvaluatorTest`, covering regression rows 1, 4, 5 and the boundaries of 2, 3; rows 6–8 are config-level and covered by `SourceConfigStructureTest`) |
| count + sum over stored rows, destination-column resolution | `beam-io/.../records/BigQueryDataSourceRecordAdapter.java` (`countAndSum`, `resolveColumnKey`) |
| source query: last row, location | `beam-io/.../bq/BigQueryJobService.java` (`queryLastRow`) |
| Excel BnC sheet cells | `beam-io/.../source/file/FileSourceAdapter.java` (`readBncSheet`), `FileBncSheetReader.java` |
| token resolution of `source_query` | `beam-runner/.../DataSourcePipelineFactory.java` (`resolveQueryTokens`) |
| orchestration, persistence, notification | `beam-runner/.../PostDownloadFinalizeTransform.java` (`evaluateBnc`) |

---

# Part 3 — What the photographs did not contain

Complete as of the second batch of photographs (regression rows 8–18 and section M). Still not seen:

* The exact digits of regression rows 2 and 3 (photographed out of focus).
* The cell layout of rows 12 and 16–18 is ambiguous in the photographs; they are read by their meaning (Part 1,
  section L).
* The text of section E-3 (the "Q2: see E-3" cross-reference for a zero-row load) was not seen as a separate
  rule. Section F's rows for an empty source and an empty destination were used instead.
* Anything before the title page or after the closing footer ("BnC Framework | Reverse-Engineering and
  Migration Specification").
