# Agent Guide — beam-pipeline-framework

This file is the primary reference for any AI agent working in this repository.
Read it fully before making any changes. Written for any capable language model (Claude, GPT, Gemini, etc.).

---

> ## ⚠️ SELF-ENFORCEMENT — READ BEFORE TOUCHING ANY FILE
>
> **Documentation is part of every task. Update it in the same response as the code change.
> Do not wait to be asked. The user should never need to say "document that".**
>
> ### After every code change, before responding to the user:
>
> | Changed | Must also update |
> |---|---|
> | Any `.java` file in `beam-io/` | `beam-io/README.md` — add/edit the class entry |
> | Any `.java` file in `beam-utils/` | `beam-utils/README.md` |
> | Any `.java` file in `beam-transforms/` | `beam-transforms/README.md` |
> | Any `.java` file in `beam-runner/` | `beam-runner/README.md` |
> | New class added anywhere | Section 4 file map in this file (`CLAUDE.md`) |
> | New CLI flag in `FrameworkOptions` | Section 17 build reference + `beam-core/README.md` + root `README.md` |
> | Execution path changed | Section 8 or 9 in this file + `WALKTHROUGH.md` sequence diagram |
> | New BQ table or schema | Section 10 in this file + root `README.md` |
> | Architecture rule changed | Section 5 in this file + root `README.md` |
> | New example or runnable | `EXAMPLE.md` if it teaches the BQ param pattern |
>
> ### Enforcement mechanisms (automatically active in this repo)
>
> - **Stop hook** (`.claude/settings.json`) — fires at the end of every Claude response and
>   prints a warning if code files changed without any doc files being touched.
> - **git pre-commit hook** (`.git/hooks/pre-commit`) — **blocks the commit** if `.java` files
>   are staged but no `README.md` / `CLAUDE.md` / `WALKTHROUGH.md` is staged alongside them.
>   Override only for genuine doc-free changes: `git commit --no-verify`.
>
> ### What "documented" means
>
> - The class appears in Section 4 of this file with a one-line description.
> - Its module README has an entry or updated entry.
> - If it changes an execution path, the execution path diagram (Section 8/9) reflects it.
> - If it adds a CLI flag, the flag appears in Section 17 and `beam-core/README.md`.
> - The commit message is descriptive enough that a reader can understand the change without
>   reading the diff.

---

For human-readable documentation, see [`README.md`](README.md), [`WALKTHROUGH.md`](WALKTHROUGH.md),
and the per-module `README.md` files. [`docs/field-guide.html`](docs/field-guide.html) is a
browsable, narrative walkthrough of the same material — open it in a browser rather than reading
it as markdown.

---

## 1. What this project is

A configurable Apache Beam ETL pipeline framework in Java 17, running on GCP Dataflow and
triggered by Apache Airflow. It supports two process types:

- **DATA_SOURCE_DOWNLOAD** — fetches raw data from external sources (API, file, BigQuery),
  applies per-source transforms (lookup, group-by, sort), validates output, and writes to
  per-source BQ tables. Source config fetched from the **BigQuery** `source_config` table via
  `BigQuerySourceConfigRepository` — no JDBC, no code changes for new sources.
- **REPORT_PROCESSING** — reads downloaded data, applies a chained BigQuery transformation
  sequence, exports results to GCS files, and sends email with attachments. When `--reportName`
  is set, runs entirely in the driver JVM (no Dataflow job). All report configuration
  (stored as a nested JSON blob in `parameter_store`) is fetched from **BigQuery** — no JDBC.
  Falls back to a generic source → transform chain → sink Beam pipeline when `--reportName` is blank.

---

## 2. Mandatory directive — README updates

> **Every code change MUST be reflected in the README files. This is non-negotiable.**

| Type of change | READMEs to update |
|---|---|
| New transform | `beam-transforms/README.md` + root `README.md` |
| New source or sink connector | `beam-io/README.md` |
| New utility class | `beam-utils/README.md` |
| New pipeline option / CLI flag | `beam-core/README.md` + root `README.md` |
| Change to pipeline assembly logic | `beam-runner/README.md` |
| New module | Root `README.md` (module structure + dependency diagram) |
| Any architectural change | Root `README.md` + `WALKTHROUGH.md` (update the relevant diagram) |
| New BQ param/config table | `beam-io/README.md` (BQ config section) + root `README.md` |
| New model class | `beam-core/README.md` (model table) + this file (section 4 file map) |

If you add a class → describe it.
If you add a flag → add it to the config table.
If you change behavior → update the section that describes it.
If a README becomes wrong → fix it. Do not leave it stale.

---

## 3. File reading order — fastest path to understanding

Read in this order for a complete mental model:

```
1.  WALKTHROUGH.md                                    — UML diagrams + execution flows (read this first)
2.  EXAMPLE.md                                        — end-to-end BQ param store example with DDL + run command
3.  beam-core/.../options/FrameworkOptions.java       — all CLI flags; the config contract
4.  beam-runner/.../runner/Main.java                  — entry point; how process type routes
5.  beam-runner/.../runner/DataSourcePipelineFactory  — DATA_SOURCE_DOWNLOAD orchestration
6.  beam-runner/.../runner/ReportPipelineFactory      — REPORT_PROCESSING orchestration (BQ-based)
7.  beam-io/.../io/params/BigQueryParameterAdapter    — key-value BQ param store interface + impl
8.  beam-io/.../io/config/BigQueryReportRepository    — report config fetched from parameter_store nested JSON
9.  beam-core/.../transform/BeamTransform.java        — SPI interface; the extension contract
10. beam-runner/.../runner/PipelineFactory.java       — legacy REPORT_PROCESSING (transform chain)
11. beam-io/.../io/config/BigQuerySourceConfigRepository — source config rows fetched from BQ (DATA_SOURCE_DOWNLOAD)
12. beam-io/.../io/source/SourceRouter.java            — source type → connector mapping
13. beam-runner/.../runner/SourceTransformChainAssembler — LOOKUP/GROUP_BY/SORT_BY assembly
14. beam-io/.../io/report/BigQueryJobService.java      — how BQ jobs run for reports
15. beam-io/.../io/checkpoint/DataSourceCheckpointAdapter — checkpoint lifecycle (LOADING→COMPLETED/FAILED)
```

---

## 4. Complete file map

Every source file, one line each.

### beam-core — contracts only, no GCP code

```
options/FrameworkOptions.java         All CLI flags. Every pipeline option. Read this first.
options/ProcessType.java              Enum: DATA_SOURCE_DOWNLOAD | REPORT_PROCESSING | PIPELINE. PIPELINE composes the
                                       other two (ordered DATA_SOURCE steps batched into one job, then a terminal
                                       REPORT step) rather than replacing either — see PipelineSequenceFactory.
options/SourceType.java               Enum: GCS | BQ | PUBSUB | API | FILE
options/SinkType.java                 Enum: GCS | BQ | PUBSUB
options/RetryPolicyType.java          Enum: NONE | FIXED | EXPONENTIAL
options/WriteDispositionType.java     Enum: APPEND | TRUNCATE

transform/BeamTransform.java          SPI interface. name() + toComposite(). SUCCESS_TAG + DEAD_LETTER_TAG.
transform/TransformRegistry.java      ServiceLoader discovery. resolve(chainSpec) → List<BeamTransform>.

retry/RetryPolicy.java                Interface: shouldRetry(attempt, cause), delayMs(attempt).
retry/ExponentialRetryPolicy.java     Exponential back-off, ThreadLocalRandom jitter, 200ms cap.
retry/FixedRetryPolicy.java           Fixed delay, 200ms cap.
retry/RetryingDoFn.java               Generic retry + DLQ routing via TupleTag.

model/FailedRecord.java               DLQ envelope. @DefaultCoder(SerializableCoder.class).
model/Schemas.java                    RAW_JSON schema constant.
model/DataSourceCheckpoint.java       Checkpoint row: daId, srceNm, vsnNo, perId (int), flNm, balAndCntlSmryTx, staCd. BQ cols: da_id INT64, srce_nm, vsn_no, per_id INT64, fl_nm, bal_and_cntl_smry_tx, sta_cd. Timestamps: DATETIME (LocalDateTime).

-- DATA_SOURCE_DOWNLOAD models --
model/SourceConfig.java               Per-source config with Builder. Carries ALL per-source config.
model/ApiSourceConfig.java            REST API config: endpoint, auth, pagination.
model/FileSourceConfig.java           File config: CSV/Excel, GCS location, delimiter, header. firstRow (1-based, default 1)
                                       skips leading rows before the header/first data row. lastColumn (Excel-style letter,
                                       optional) fixes column width; unset auto-detects from the widest row seen.
model/BqFetchConfig.java              BQ source: project, dataset, table, query, queryParams map, schema (List<SourceSchemaField>, optional, from bq_schema_json).
model/SourceSchemaField.java          One declared column (columnName + bqType) for BqFetchConfig.schema. bqType is a real BQ SQL type name (STRING/INT64/FLOAT64/BOOLEAN/BYTES/DATE/DATETIME/TIME/TIMESTAMP/NUMERIC/BIGNUMERIC).
model/QueryConfig.java                Query template + paramMappings for token injection.
model/SourceTransformConfig.java      One transform step: GROUP_BY | SORT_BY | LOOKUP.
model/AggregationConfig.java          SUM/COUNT/AVG/MIN/MAX per field (used by GROUP_BY).
model/LookupConfig.java               Lookup table config: BQ source, key fields.
model/ValidationConfig.java           Post-fetch validation: header check, row count, BnC rules.
model/BncRule.java                    One Balance-and-Control check: SUM(field) within tolerance %.
model/SourceFailureEmailConfig.java   Optional failure-notification email config on SourceConfig. Populated from failure_email_* keys in parameters_val_json. isPresent() guards send.
model/DataTransformConfig.java        Optional post-storage SQL transform (query + min/max output row bounds), run within the same DATA_SOURCE_DOWNLOAD run by PostDownloadFinalizeTransform, before COMPLETED. A `WITH data AS (...)` UNNEST(DaRec) reunification CTE is always prepended to query before it runs — unconditional, not opt-in — so the operator's SQL just references `data` as a plain table. From data_transform_query/data_transform_min_row_count/data_transform_max_row_count.

-- REPORT_PROCESSING models --
model/ReportConfig.java               Full report config assembled from parameter_store nested JSON blob. periodId is int.
model/ReportDatasourceRef.java        Required DS for a report + transform alias.
model/ReportPreprocessingStep.java    Pre-run step: BQ_QUERY or API_ENRICHMENT.
model/ReportTransformStep.java        One BQ query in the chain: inputAlias → outputAlias.
model/ReportOutputConfig.java         File output: CSV/JSON, GCS path, prefix, suffix.
model/ReportEmailConfig.java          Email: to, cc, subject/body templates with tokens, fromAddress (from_address key), encrypted (default false).
model/EmailParams.java                Sender/recipient envelope for EmailSendUtility: fromEmailAddress, subject, toList, ccList, encryptedOrNot. Built by SetEmailParams(), passed into CreateEmailRequest().
model/EmailAttachment.java            EmailSendUtility's attachment shape: fileName, content (InputStream), type (MIME string). Distinct from io/email/EmailAttachment.java (the older ReportEmailAdapter's shape) — never import both unqualified in the same file.
model/ReportCheckpoint.java           RptRefer row: rptId, rptNm, perId (int), rptDs, staCd, creatTs, lstUpdtTs. sta_cd: LOADING → COMPLETED / FAILED.
model/RptDaMap.java                   RptDaMap row: mapId, rptId, daId, lstUpdtTs. Links a report run to a data source da_id.
model/RptStageDa.java                 RptStageDa row: stageId, mapId, stageDsJsonTx, queryConfigTx, loadDt (DATE), lstUpdtTs. One row per DaRec page (batched, ≤250 records/JSON array — mirrors DaRec's own pagination), not one row per record; un-nested back to individual records on read by stagedDataSubquery(). Transient staging; deleted after transforms.
model/RptOutput.java                  RptOutput row: outptCd, rptDt, vsnNo, outputDs, lineReferCd, schedTx, balAm, rptTypeCd, rptId, lstUpdtTs. One per output step.
model/PipelineRunConfig.java          Per-datasource runtime config from parameter_store. Replaces FrameworkOptions flags for source, sink, transforms, and retry/DLQ. Typed getters (getSourceType, getSinkType, getTransformChain, getPiiFields, getRetryPolicy, getDeadLetterSink, etc.) + generic get(key)/get(key, default) escape hatch. Calendar (--calendarName) and per-source failure email (SourceFailureEmailConfig) are configured elsewhere, not here.
```

There is no separate PIPELINE config model. `ProcessType.PIPELINE` reuses `ReportConfig.datasources[]`
(`model/ReportDatasourceRef.java`, already listed above under REPORT_PROCESSING models) directly —
see `runner/PipelineSequenceFactory.java` in the beam-runner section below.

### beam-io — connectors and I/O adapters

```
source/SourceRouter.java              Stateless factory: route() (REPORT_PROCESSING) + routeFromConfig() (DATA_SOURCE_DOWNLOAD). Both have overloads with nullable Schema that pass a pre-fetched schema to BigQuerySourceTransform; schema fetched by caller in beam-runner.
source/BigQuerySourceTransform.java   BigQueryIO.read() with two modes: typed (pre-fetched Schema → custom TableRow conversion: INT64/DOUBLE/BOOLEAN as native types, temporal and STRING as String) and generic fallback (null schema → expand() runs a SELECT * LIMIT 1 preview query in the driver JVM to learn real column names — needs only query-execution rights, not bigquery.tables.get — builds one nullable-STRING field per column, applied consistently to setRowSchema() and every Row; falls back further to Schemas.RAW_JSON blob if even the preview query fails). Does NOT use BigQueryUtils.toBeamRow() — that assumes Avro encoding and throws NumberFormatException on ISO temporal strings.
source/GcsSourceTransform.java        GCS glob → newline-delimited JSON rows.
source/PubSubSourceTransform.java     Pub/Sub subscription → streaming rows.
source/ApiSourceAdapter.java          Pure HTTP adapter: auth, PAGE_NUMBER/CURSOR/OFFSET pagination.
source/ApiSourceTransform.java        Beam wrapper for ApiSourceAdapter. @Setup/@Teardown for HttpClient.
source/FileSourceAdapter.java         CSV (Commons CSV) + Excel (Apache POI) from GCS bytes. Each data row is keyed by Excel-style
                                       column letters (A, B, ..., Z, AA, ...), never real header text. If the file has a header row,
                                       the real names go into a separate header-legend content object (letter→name), wrapped via
                                       FileHeaderLegend.wrapLegend() for transit, instead of being used as row keys; no legend when
                                       there's no header. Returns FileParseResult(rows, headerLegendJson).
                                       FileSourceConfig.firstRow skips leading rows before the header/first data row (1-based).
                                       Column width (letters generated, and row padding/truncation) is FileSourceConfig.lastColumn
                                       when set, else auto-detected as the widest row seen (header or data) — a data row wider than
                                       the header is never truncated for lacking a header name. columnIndexFromLetter() is the
                                       inverse of columnLetter(), used to resolve lastColumn. parseCsv/parseExcel share this
                                       width logic via resolveColumnCount() rather than each computing it separately.
source/FileSourceTransform.java       Beam wrapper for FileSourceAdapter. Emits one Row per data row, then one extra Row for the
                                       marker-wrapped header-legend JSON if present (same Schemas.RAW_JSON schema either way).

sink/SinkRouter.java                  Stateless factory: route(data, options).
sink/BigQuerySinkTransform.java       Writes PCollection<Row> to BQ. Returns WriteResult.
sink/GcsSinkTransform.java            Writes PCollection<Row> as newline-delimited JSON.
sink/PubSubSinkTransform.java         Publishes each Row as JSON to Pub/Sub.
sink/DeadLetterSinkTransform.java     Writes FailedRecord objects to GCS DLQ path.
sink/DataSourceRecordSinkTransform.java  Collects all source rows globally, paginates at 250 rows/page, writes one DaRec row per page with row_da_json_tx = flat JSON array (BQ/API, headerless FILE) or {"Data":[...],"DataHeaders":[...]} (FILE source with a header). Streaming inserts. Returns PCollection<Long> = total source rows (held until all inserts commit) for PostDownloadFinalizeTransform Wait.on(). DaRec gains page_no INT64.
                                          PaginateAndBuildDoFn detects a FILE-source header-legend element (marker-wrapped via
                                          FileHeaderLegend.wrapLegend), unwraps it, excludes it from totalRows/page-size accounting,
                                          and appends a copy of it into EVERY page's DataHeaders array.

checkpoint/DataSourceCheckpointAdapter.java         Interface: createCheckpoint(), updateStatus(), isCompleted(), getLatest(), fetchLatestCompletedDaId(). perId is int.
checkpoint/BigQueryDataSourceCheckpointAdapter.java BQ DML impl. MAX(da_id)+1 sequence. MAX(vsn_no)+1 per (srce_nm, per_id). All timestamps DATETIME. Has String-tableRef constructor for in-worker use.
checkpoint/ReportCheckpointAdapter.java             Interface for all 4 REPORT_PROCESSING tables: RptRefer, RptDaMap, RptStageDa, RptOutput.
checkpoint/BigQueryReportCheckpointAdapter.java     BQ DML impl. Reads table names from --rptReferTable/--rptDaMapTable/--rptStageDaTable/--rptOutputTable flags.
                                                     stageFromDaRec copies DaRec's pages into RptStageDa verbatim (one RptStageDa row per DaRec
                                                     page, batched like DaRec itself — not one row per source record). stagedDataSubquery()
                                                     un-nests those pages back into individual source-row JSON objects on read, via
                                                     FileHeaderLegend.dataArrayExpr() (handles both a plain array page and a FILE source's
                                                     {"Data":[...],"DataHeaders":[...]} page), aliasing the result back to stage_ds_json_tx —
                                                     so report SQL always sees one row per record, same as before batching, and any FILE-source
                                                     header legend — in the separate DataHeaders array — is never staged into a report's input data.

records/DataSourceRecordAdapter.java          Interface: countRecords(daId), sumField(daId, field), deleteRecords(daId).
records/BigQueryDataSourceRecordAdapter.java  countRecords: CROSS JOIN UNNEST(FileHeaderLegend.dataArrayExpr(row_da_json_tx)) then COUNT(*) individual rows — the expression extracts just the row array whether a page is a flat array or a FILE source's {"Data":[...],"DataHeaders":[...]}, so the header legend (in the separate DataHeaders array) is never counted as a row. sumField: same UNNEST then SUM(CAST(JSON_VALUE(row_json, '$.field') AS FLOAT64)) — the legend is likewise never reached. deleteRecords: DELETE FROM DaRec WHERE da_id=@daId; best-effort (logs+swallows); used as-is for the --manualOverrun cleanup of an older, already-flushed run. For the data_transform_query replace (same-run, just-streamed rows), PostDownloadFinalizeTransform.replaceStoredRows() does NOT use this method — it runs DELETE+INSERT as a single atomic BigQuery multi-statement transaction (BEGIN TRANSACTION...COMMIT, with ROLLBACK on error) instead, since those rows can still be in BigQuery's streaming buffer (DML-ineligible despite being SELECT-visible) and two separate unverified statements could leave a partial delete (some pages gone, some not) or, worse, a successful delete followed by a failed insert with nothing to restore the originals.

email/EmailAttachment.java            Attachment model: InputStream + fileName + contentType. Used by ReportEmailAdapter (below) — a
                                       different type from model/EmailAttachment.java (EmailSendUtility's own attachment shape).
email/ReportEmailAdapter.java         Interface: send(subject, body, to, cc, List<EmailAttachment>). Used only by
                                       PostDownloadFinalizeTransform's DATA_SOURCE_DOWNLOAD failure email now — see
                                       EmailSendUtility below for ReportPipelineFactory's report-completion email.
email/EmailSendUtility.java           Interface: SetEmailParams(fromAddress, subject, toList, ccList, encryptedOrNot) → EmailParams;
                                       CreateEmailRequest(EmailParams, bodyHtml, List<model.EmailAttachment>); default method
                                       FetchFileFromGcs(fileLocation) fetches a GCS object as an InputStream via the GCS client
                                       directly (beam-io can't depend on beam-utils' GcsUtils). No implementation ships in this
                                       repo — ReportPipelineFactory discovers one via ServiceLoader SPI (same mechanism as
                                       TransformRegistry for BeamTransform) or accepts one via constructor injection; if neither
                                       is available, report-completion email is skipped with a warning, not a failure.

report/BigQueryJobService.java        BQ jobs: runQueryToTable(), exportToCsv(), exportToJson(), countRows() (live COUNT(*), not metadata-based), dropTableIfExists() (best-effort). No-arg constructor holds no FrameworkOptions, so also safe inside a Beam worker DoFn (PostDownloadFinalizeTransform uses it this way).

params/BigQueryParameterAdapter.java     Interface: fetchRequiredKeys(), fetchParameters(), fetchRequiredParameters().
params/BigQueryParameterAdapterImpl.java BQ client impl. Named query params (@key). Reads --paramBqProject/Dataset/StoreTable/RequiredTable.
                                         fetchRequiredParameters() = look up index → fetch values → validate all present.

config/BigQueryReportRepository.java       Queries parameter_store for report config nested JSON.
                                           fetchReportConfig() parses datasources/preprocessing/transforms/outputs/email,
                                           plus top-level output_bq_table and output_bq_input_alias.
config/BigQuerySourceConfigRepository.java Queries parameter_store for DATA_SOURCE_DOWNLOAD source configs.
                                           fetchSourceConfigs(). Row → SourceConfig mapping. Also parses
                                           data_transform_query/data_transform_min_row_count/
                                           data_transform_max_row_count into DataTransformConfig.

util/JsonUtils.java                   Row → JSON with correct type handling.
util/FileHeaderLegend.java            Helpers for the FILE-source column-letter storage convention: wrapLegend()/isMarkerWrapped()/
                                       unwrapLegend() thread a header-legend content object through the same PCollection<Row> as
                                       data rows; buildPage() builds a FILE-with-header DaRec page as {"Data":[...],"DataHeaders":[...]};
                                       dataArrayExpr() is the one SQL expression every DaRec reader uses to extract just the row array,
                                       whether a page is that shape or a plain array.
```

### beam-utils — stateless helpers, no pipeline graph code

```
BigQuerySchemaUtils.java    fetchBeamSchema(). Call in driver JVM only. Type mapping: INTEGER/INT64→INT64, FLOAT/FLOAT64→DOUBLE, BOOLEAN/BOOL→BOOLEAN, BYTES→BYTES, TIMESTAMP/DATE/DATETIME/TIME→STRING (ISO strings preserved as-is from TableRow JSON encoding).
                             toBeamSchema(List<SourceSchemaField>) builds a Schema from an operator-declared bq_schema_json list — no BQ call, no tables.get permission needed. Throws IllegalArgumentException on an unrecognised bqType (fail loudly on a config typo, unlike fetchBeamSchema()'s permissive STRING default for unmapped BQ-reported types).
GcsUtils.java               pathHasFiles(), listFiles(), writeTextFile(), readTextFile(), readBytes(), deletePrefix().
SecretManagerUtils.java     fetchSecret(secretId). Never log result. Never store in options value.
RowValidationUtils.java     requireFields(), matchesPattern(), inRange(), oneOf(). Thread-safe.
MetricsUtils.java           transformCounter(), pipelineDlqTotal(). Consistent naming for Dataflow UI.
CalendarUtils.java          STUBS — isBusinessDay(), nextBusinessDay(), applyOffset(). Must be implemented.
DateUtils.java              resolveRunDate(), partitionedPath(), shardedTable(), toDisplayString().
QueryParameterResolver.java resolve(template, paramMappings, options). Two-pass: standard then custom tokens.
                             Custom tokens merge paramMappings (a step's query_params_json) with
                             options.getCustomParamsJson() (--customParamsJson CLI flag) — the CLI value
                             wins on a key collision. Malformed/non-object --customParamsJson throws.

```

### beam-transforms — pluggable transform implementations

```
FilterNullsTransform.java           Token: filter-nulls. Drops null rows → DLQ. Counter metric.
MaskPiiTransform.java               Token: mask-pii. SHA-256 hashes --piiFields list.
EnrichFromExternalApiTransform.java Token: enrich-from-api. SAMPLE — shows @Setup/@Teardown pattern.

source/GroupByTransform.java        MapElements → GroupByKey → AggregateDoFn. SUM/COUNT/AVG/MIN/MAX.
source/SortByTransform.java         Per-bundle sort (@StartBundle/@FinishBundle). NOT global. Logs warning.
source/LookupEnrichTransform.java   Left-join via PCollectionView<Map<String,String>> (key → JSON blob).

side/SideEffectEmailTransform.java  Sends SMTP email per Row. No attachments. Best-effort (logs on fail).

META-INF/services/...BeamTransform  SPI manifest. One class name per line.
```

### beam-runner — entry point and orchestrators

```
Main.java                       Parses CLI → routes by processType + reportName.
PipelineFactory.java            Legacy REPORT_PROCESSING: source → transform chain → sink. fetchBqSchema() fetches typed Schema at driver-JVM time and passes it to SourceRouter.route().
DataSourcePipelineFactory.java  DATA_SOURCE_DOWNLOAD: per-source branches; creates LOADING checkpoint per source in driver JVM, wires RecordSink → PostDownloadFinalizeTransform in graph. fetchBqSchema() calls BigQuerySchemaUtils (beam-utils) at driver-JVM time.
                                fetchBqSchema() prefers BqFetchConfig.schema (operator-declared bq_schema_json) via
                                BigQuerySchemaUtils.toBeamSchema() over table-metadata fetch when present — a bad
                                declared type throws IllegalArgumentException uncaught, failing the run before any
                                data moves.
                                Under --manualOverrun, fetchLatestCompletedDaId() per source BEFORE createCheckpoint()
                                captures the superseded previous da_id, passed into PostDownloadFinalizeTransform.
                                createCheckpoint() is always a fresh INSERT — DaRefer only ever gains new rows.
                                assemble(options) (single --datasourceName) now delegates to the public
                                assembleForConfigs(options, List<SourceConfig>) — checkpoint filtering,
                                manualOverrun capture, LOADING checkpoint creation, per-source branch assembly —
                                so PipelineSequenceFactory can batch several explicitly-fetched SourceConfigs
                                (one per PIPELINE DATA_SOURCE step) into the same single Dataflow job.
PostDownloadFinalizeTransform.java  Final worker-side step for each source branch: always-on row count equality check (storedRowCount vs pipelineRowCount), optional min/max bounds, optional data_transform_query (post-storage SQL transform; a WITH data AS (...) UNNEST(DaRec) CTE is always prepended before it runs, unconditionally — the operator's SQL just references `data` as a plain table; validates output row count before replacing stored rows; original rows untouched on failure), optional BnC sum rules (against transformed rows if applied), checkpoint update (COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED), manualOverrun cleanup (deletes the superseded previous da_id's DaRec rows, only on COMPLETED), failure email. Runs inside Beam worker.
ReportPipelineFactory.java      REPORT_PROCESSING (BQ-configured): driver-JVM BQ jobs + email.
                                Uses BigQueryReportRepository (not JDBC) for all config loading.
                                After transform chain, writes final result to per-report BQ table
                                (output_bq_table from config) if set; resolves source alias from
                                output_bq_input_alias → last transform alias → first datasource alias.
                                Report-completion email uses EmailSendUtility (io/email/), not
                                SmtpReportEmailAdapter: an EmailSendUtility is passed to the
                                3-arg constructor, or discovered via ServiceLoader SPI in the
                                no-arg/2-arg constructors (discoverEmailUtility()) — this repo
                                ships no implementation, so it's null unless the deployment's
                                classpath provides one; sendEmail() logs a warning and skips
                                sending rather than failing the report when it's null.
SourceTransformChainAssembler.java Assembles LOOKUP/GROUP_BY/SORT_BY per source; loads lookup views.
SmtpReportEmailAdapter.java     SMTP impl of ReportEmailAdapter. MimeMultipart for attachments.
PipelineSequenceFactory.java    PIPELINE: takes the SAME --reportName/--reportSubprocess as REPORT_PROCESSING — no
                                separate pipeline config. BigQueryReportRepository.fetchReportConfig() → the report's
                                own datasources[] (List<ReportDatasourceRef>) IS the pipeline: it already declares
                                which datasources feed the report and each one's is_required flag, so nothing
                                redeclares that as a second, separately-maintained sequence.
                                Every declared datasource's SourceConfig fetched by name (BigQuerySourceConfigRepository),
                                then all of them batched into ONE Dataflow job via
                                DataSourcePipelineFactory.assembleForConfigs() (skips any already COMPLETED, same
                                as standalone DATA_SOURCE_DOWNLOAD) — never one job per datasource. After
                                waitUntilFinish(), re-checks each datasource's checkpoint status; an incomplete one
                                only aborts the whole PIPELINE (PipelineAbortedException) if its ReportDatasourceRef.required
                                says so (this IS the report's own datasources[].is_required — nothing separate to
                                keep in sync). options.reportName/reportSubprocess are never touched — PIPELINE differs
                                from plain REPORT_PROCESSING only in running missing datasources first instead of
                                failing immediately; the terminal report then runs via the unchanged
                                ReportPipelineFactory.execute(). Composes both existing factories rather than
                                reimplementing either.
                                --manualOverrun applies uniformly across the whole sequence with no PIPELINE-specific
                                code: the same options instance is passed straight into both assembleForConfigs()
                                and ReportPipelineFactory.execute(), so every declared datasource gets the same
                                bypass-COMPLETED-guard-and-supersede treatment DataSourcePipelineFactory already
                                gives it standalone. The REPORT step needs nothing extra — it has no COMPLETED
                                guard of its own and always re-runs, manualOverrun or not.

example/ExampleWorkflow.java    Self-contained end-to-end example. Shows: BigQueryParameterAdapter
                                → fetchRequiredParameters → resolve tokens → BigQueryJobService
                                → exportToCsv → GCS. See EXAMPLE.md for BQ setup + run command.
```

### beam-orchestrator — standalone orchestration JAR (no Beam dependency)

Triggered by an Airflow DAG. Reads parameter_store from BigQuery, creates task records in BQ,
and writes a manifest JSON to GCS so the DAG can fan out to individual pipeline JAR invocations.
Zero dependency on beam-core or any sibling beam-* module — it is a fully independent JAR.

```
OrchestratorMain.java           Entry point. Wires concrete impls → Orchestrator and runs it.
OrchestratorOptions.java        --key=value CLI parser. No Beam PipelineOptions dependency.
Orchestrator.java               Core logic: resolve period → schedule → build tasks → save → manifest.

model/ResolvedPeriod.java       Period value: periodId (int), periodStart, periodEnd, runDate, frequency.
model/RunSpec.java              One schedulable unit: runType, parentId, name, subprocess, period, runOrder, extraParams.
model/TaskItem.java             Persisted task: taskId (UUID), runId, RunSpec, status, createdAt, metadata.

period/PeriodResolver.java      @FunctionalInterface: resolve(runDate, frequency) → ResolvedPeriod.
period/StandardPeriodResolver.java DAILY (YYYYMMDD), MONTHLY (YYYYMM), WEEKLY (YYYYWW ISO week).

schedule/RunScheduleResolver.java  @FunctionalInterface: resolve(parentId, frequency, period) → List<RunSpec>.
schedule/BigQueryRunScheduleResolver.java Queries parameter_store; opts in via run_type/enabled/frequency/run_order fields.

task/TaskRepository.java        Interface: save(List<TaskItem>).
task/BigQueryTaskRepository.java BQ streaming insert impl. taskId as deduplication key.

manifest/ManifestWriter.java    @FunctionalInterface: write(runId, parentId, frequency, runDate, tasks) → location.
manifest/GcsManifestWriter.java Writes JSON manifest to GCS. Default path: manifests/{runId}/tasks.json.
```

---

## 5. Architecture rules — non-negotiable

### Module dependency direction

```
beam-runner → beam-core, beam-io, beam-utils, beam-transforms
beam-transforms → beam-core, beam-utils
beam-io → beam-core   (NOT beam-utils, NOT beam-transforms)
beam-utils → beam-core
beam-core → (nothing internal)
beam-orchestrator → (nothing internal — standalone, no sibling module deps)
```

**Violations**: if `beam-io` imports from `beam-utils`, it breaks this rule. The compiler will
not catch it — but it creates a circular risk and violates the isolation contract.
`SmtpReportEmailAdapter` is in `beam-runner` (not `beam-io`) precisely because it needs
`SecretManagerUtils` from `beam-utils` and `angus-mail` from `beam-transforms`. Contrast
`EmailSendUtility.FetchFileFromGcs()` in `beam-io`, whose default method only needs the GCS
client `beam-io` already depends on directly (no `beam-utils`/`beam-transforms` needed) — that's
the difference that decides where each one is allowed to live.

### Wire type

All Beam transforms communicate via `PCollection<Row>` with a declared `Schema`.
Call `.setRowSchema()` on every output. Do not use raw bytes, Strings, or Avro.

### Output contract (BeamTransform SPI)

Every `BeamTransform.toComposite()` returns `PTransform<PCollection<Row>, PCollectionTuple>`.
The tuple MUST include both:
- `BeamTransform.SUCCESS_TAG` — `TupleTag<Row>`
- `BeamTransform.DEAD_LETTER_TAG` — `TupleTag<FailedRecord>`

### Per-source independence (DATA_SOURCE_DOWNLOAD)

Sources are **never merged**. Each `SourceConfig` is an independent Beam DAG branch.
`Flatten.pCollections()` across different sources is **forbidden**.

---

## 6. Serialization rules

| Rule | Correct | Wrong |
|---|---|---|
| DoFn class type | Named `static final` inner class | Anonymous class or lambda |
| DoFn field types | All `Serializable` (String, int, List, Map) | Non-serializable (HttpClient, Connection) |
| Non-serializable resources | `transient` field, create in `@Setup`, close in `@Teardown` | Non-transient field |
| TupleTag instances | `static final` on the DoFn class | Created inside `@ProcessElement` |
| Function interfaces | `SerializableFunction<A,B>` (Beam) | `java.util.function.Function<A,B>` |
| Models in `PCollection` | `@DefaultCoder(SerializableCoder.class)` on the class | No coder annotation |

---

## 7. How to make each type of change

### Add a new data source type (DATA_SOURCE_DOWNLOAD)

1. Insert a row in `source_config` with `source_type = MY_TYPE`
2. Add `MY_TYPE` to `SourceType` enum in `beam-core`
3. Create `MySourceAdapter` (pure Java, no Beam) in `beam-io/source/`
4. Create `MySourceTransform` (thin Beam wrapper) in `beam-io/source/`
5. Add a case to `SourceRouter.routeFromConfig()` switch
6. Add required config fields to `SourceConfig` model and `BigQuerySourceConfigRepository.rowToSourceConfig()`
7. Update `beam-io/README.md` and root `README.md`

### Add a new per-source transform type

1. Create the transform class in `beam-transforms/source/` extending `PTransform<PCollection<Row>, PCollection<Row>>`
2. Add the type constant to `SourceTransformConfig` (e.g., `public static final String MY_TYPE = "MY_TYPE"`)
3. Add a case to `SourceTransformChainAssembler.assemble()` switch
4. Add config fields to `SourceTransformConfig` and its JSON parsing in `BigQuerySourceConfigRepository.toSourceTransforms()`
5. Update `beam-transforms/README.md`

### Add a new report transformation step type

The report transformation chain uses raw BQ SQL — no new Java code needed.
Add a new object to the `transforms` array in the `parameter_store` `parameters_val_json` for
the report, with `query_template` referencing any alias in the registry. Custom tokens go in
`query_params_json`.

### Add a new BeamTransform (pluggable, SPI-registered)

1. Create class implementing `BeamTransform` in `beam-transforms/`
2. Use named `static final` inner classes for composite and DoFn
3. Output to both `SUCCESS_TAG` and `DEAD_LETTER_TAG`
4. Add to `META-INF/services/com.yourco.beam.transform.BeamTransform`
5. Update `beam-transforms/README.md`

### Add a new CLI flag

1. Add getter + setter in `FrameworkOptions.java` with `@Description`
2. Add `@Default.*` if it has a sensible default; `@Validation.Required` if mandatory
3. Update `beam-core/README.md` and root `README.md`

### Add a new report type

1. Insert one row in `parameter_store` with `parameter_name=reportName`, `parameter_data_source=reportSubprocess`,
   `parameter_group_name=parentId`, and the full nested JSON config in `parameters_val_json`
   (keys: `override_key`, `datasources`, `preprocessing`, `transforms`, `outputs`, `email`)
2. No Java code changes needed unless a new preprocessing step type is required
3. For new preprocessing types, add a `case` in `ReportPipelineFactory.runPreprocessing()`

---

## 8. DATA_SOURCE_DOWNLOAD — execution path

**Flex Template / DirectRunner (inline post-pipeline):**
```
Main.runDataSourceDownload(options)
│
├─ DataSourcePipelineFactory.assemble(options)   [driver JVM]
│   ├─ BigQuerySourceConfigRepository.fetchSourceConfigs()    load SourceConfig from BQ; throws if row missing
│   ├─ BigQueryDataSourceCheckpointAdapter.isCompleted()      skip COMPLETED sources (bypassed under
│   │                                                          --manualOverrun / --overrideDownload)
│   ├─ Under --manualOverrun only: fetchLatestCompletedDaId() per source, BEFORE createCheckpoint()
│   │   → captured as previousDaId for PostDownloadFinalizeTransform's later cleanup
│   ├─ BigQueryDataSourceCheckpointAdapter.createCheckpoint() → da_id per source (LOADING row)
│   │   Always a fresh INSERT — DaRefer only ever gains new rows, never overwritten
│   │
│   └─ for each SourceConfig (graph assembly — no data moves yet):
│       ├─ DataSourcePipelineFactory.resolveQueryTokens()     BQ only: inject {periodStart} etc.
│       ├─ DataSourcePipelineFactory.fetchBqSchema()          BQ sources, in order:
│       │   ├─ 1. BqFetchConfig.schema (operator-declared bq_schema_json) →
│       │   │      BigQuerySchemaUtils.toBeamSchema() — no BQ call, throws on a bad type name
│       │   ├─ 2. else BigQuerySchemaUtils.fetchBeamSchema() (table metadata)
│       │   └─ 3. else null → BigQuerySourceTransform.expand() resolves real column names
│       │          itself via a SELECT * LIMIT 1 preview query (no tables.get)
│       ├─ SourceRouter.routeFromConfig(schema)               API / FILE / BQ → PCollection<Row>
│       ├─ SourceTransformChainAssembler.assemble()           LOOKUP → GROUP_BY → SORT_BY chain
│       ├─ DataSourceRecordSinkTransform(da_id)               rows → paginated JSON arrays → DaRec
│       │   ├─ GroupByKey collects all rows, paginate at 250 rows/page, 1 DaRec row per page
│       │   └─ returns PCollection<Long> = total source rows (after all streaming inserts commit)
│       └─ PostDownloadFinalizeTransform(da_id)               [wired here; runs in Beam worker]
│
├─ pipeline.run()                                submit to Dataflow (or DirectRunner)
└─ result.waitUntilFinish()
   (When the job reaches DONE, PostDownloadFinalizeTransform has already run in a worker:)
       ├─ BigQueryDataSourceRecordAdapter.countRecords(daId)  → storedRowCount (UNNEST + COUNT(*), excludes any FILE header-legend row)
       ├─ row_count_mismatch check: storedRowCount == pipelineRowCount (always-on, no config needed)
       ├─ min/max row count bounds check (optional; from min_row_count / max_row_count config)
       ├─ data_transform_query (optional; only if the checks above passed):
       │   ├─ a `WITH data AS (...)` CTE — CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(row_da_json_tx)),
       │   │   reunifying every paginated DaRec page for this da_id into one flat rowset of JSON
       │   │   row strings — is always prepended before the query runs; never opt-in, so the
       │   │   operator's SQL just references `data` as a plain table, then
       │   │   BigQueryJobService.runQueryToTable() runs the combined query
       │   ├─ BigQueryJobService.countRows() validates the output against data_transform_min/max_row_count
       │   └─ only if valid: replaceStoredRows() runs DELETE + INSERT (re-paginated at 250 rows/page)
       │       as ONE atomic BigQuery multi-statement transaction (BEGIN TRANSACTION...COMMIT, with
       │       ROLLBACK on error) — never as two separate jobs. Retried as a whole with backoff
       │       (~30s total) since this run's own rows were just streamed in and can still be in
       │       BigQuery's streaming buffer (DML-ineligible even though already SELECT-visible); a
       │       failed attempt is guaranteed to have changed nothing, so retrying is always safe.
       │       (query failure or bounds failure or exhausted retries → original stored rows
       │       completely untouched — the delete and insert can never partially apply)
       ├─ BigQueryDataSourceRecordAdapter.sumField(daId, field) per BnC rule (optional; skipped if bnc_rules_json
       │   absent; runs against transformed rows if data_transform_query applied)
       ├─ BigQueryDataSourceCheckpointAdapter.updateStatus(daId, COMPLETED/FAILED_BNC/FAILED_TRANSFORM/FAILED, bncJson)
       ├─ Only on COMPLETED, only if previousDaId was captured: recordAdapter.deleteRecords(previousDaId)
       │   — manualOverrun cleanup; DaRefer itself is never touched, only the superseded DaRec rows
       └─ SmtpReportEmailAdapter.send() if SourceFailureEmailConfig.isPresent()
```

---

## 9. REPORT_PROCESSING — execution path (DB-configured mode)

Triggered when `--reportName` is set. Runs entirely in driver JVM — **no Beam pipeline**.

```
Main.runReportProcessing(options)
│
└─ ReportPipelineFactory.execute(options)
    ├─ BigQueryReportRepository.fetchReportConfig()    SELECT parameters_val_json FROM parameter_store
    │                                                  WHERE parameter_group_name=parentId
    │                                                    AND parameter_data_source=reportSubprocess
    │                                                    AND parameter_name=reportName
    │                                                  → parse nested JSON → ReportConfig (periodId: int)
    ├─ BigQueryReportCheckpointAdapter.createCheckpoint(reportName, periodId, reportName)
    │   → rpt_id (LOADING row in RptRefer)
    │
    ├─ Phase 1: Preprocessing (optional)
    │   └─ for each ReportPreprocessingStep (by step_order):
    │       └─ BigQueryJobService.runQueryToTable(resolvedSQL, outputTable)
    │
    ├─ Phase 2: Datasource availability check
    │   └─ for each required ReportDatasourceRef:
    │       └─ BigQueryDataSourceCheckpointAdapter.isCompleted(srceNm, perId) → must be true
    │
    ├─ Phase 3: Build alias registry (stage datasource rows)
    │   └─ for each ReportDatasourceRef:
    │       ├─ BigQueryDataSourceCheckpointAdapter.fetchLatestCompletedDaId(srceNm, perId) → da_id
    │       ├─ BigQueryReportCheckpointAdapter.addDaMapping(rptId, daId)   → map_id (RptDaMap row)
    │       ├─ BigQueryReportCheckpointAdapter.stageFromDaRec(mapId, daId) → copies DaRec's pages into RptStageDa verbatim (one RptStageDa row per page, not per record — batched like DaRec)
    │       └─ aliasRegistry[alias] = stagedDataSubquery(mapId)           → (SELECT row_json AS stage_ds_json_tx FROM RptStageDa CROSS JOIN UNNEST(...) AS row_json WHERE map_id=X) — un-nests RptStageDa's pages back into individual records on read, so report SQL still sees one row per record
    │
    ├─ Phase 4: Transformation chain
    │   └─ for each ReportTransformStep (by step_order):
    │       ├─ resolveAliasTokens({alias} → subquery or `project.dataset.table`)
    │       ├─ QueryParameterResolver.resolve(sql, step.queryParams, options)
    │       ├─ BigQueryJobService.runQueryToTable(resolvedSQL, step.outputBqTable)
    │       └─ aliasRegistry.put(step.outputAlias, step.outputBqTable)
    │
    ├─ Phase 4b: Write per-report BQ table (if output_bq_table is set)
    │   ├─ Resolve source alias: output_bq_input_alias → last transform outputAlias → first datasource alias
    │   └─ BigQueryJobService.runQueryToTable("SELECT * FROM <alias>", config.outputBqTable)
    │
    ├─ Phase 5: File export
    │   └─ for each ReportOutputConfig:
    │       ├─ BigQueryJobService.exportToCsv() or exportToJson()
    │       └─ record ExportedFile(gcsUri, fileName, contentType)
    │
    ├─ Phase 6: Write RptOutput rows + clear staged data
    │   ├─ BigQueryReportCheckpointAdapter.writeOutput(rptId, outptCd, outputDs, ...) per output
    │   └─ BigQueryReportCheckpointAdapter.clearStagedData(rptId)  → DELETE FROM RptStageDa WHERE map_id IN (...)
    │
    ├─ Phase 7: Email (optional; skipped with a warning if no EmailSendUtility is available)
    │   ├─ resolve subject/body templates ({reportName}, {periodId}, etc.)
    │   ├─ emailUtility.FetchFileFromGcs(gcsUri) for each exported file → EmailAttachment
    │   ├─ emailUtility.SetEmailParams(fromAddress, subject, toList, ccList, encrypted) → EmailParams
    │   └─ emailUtility.CreateEmailRequest(emailParams, body, attachments)
    │
    └─ BigQueryReportCheckpointAdapter.updateStatus(rptId, COMPLETED)
       or updateStatus(rptId, FAILED) if any phase threw
```

---

## 10. BigQuery parameter store — how config is fetched

All pipeline configuration lives in BigQuery, in the dataset specified by
`--paramBqProject` + `--paramBqDataset`. A single `parameter_store` table holds all
configuration — DATA_SOURCE_DOWNLOAD source configs and REPORT_PROCESSING report configs.
PIPELINE has no config of its own: it reads the same report config as REPORT_PROCESSING
(`--reportName`/`--reportSubprocess`) and runs the datasources that report's own
`datasources[]` already declares.

| BQ Table | Contents | Key columns |
|---|---|---|
| `parameter_store` | All pipeline params (source configs + report configs) | parameter_group_name (--parentId), parameter_data_source (--subprocessName / --reportSubprocess), parameter_name (--datasourceName / --reportName) |

`schema_of_json` declares required top-level fields. `parameters_val_json` holds the config JSON.

### Source configs (DATA_SOURCE_DOWNLOAD) — flat JSON in parameters_val_json

```json
{
  "source_type":    "BQ",
  "bq_project_id":  "my-project",
  "bq_dataset":     "raw_data",
  "bq_table":       "trades",
  "bq_query":       "SELECT * FROM ... WHERE trade_date BETWEEN '{periodStart}' AND '{periodEnd}'",
  "bq_schema_json": "[{\"name\":\"trade_id\",\"type\":\"STRING\"},{\"name\":\"amount\",\"type\":\"FLOAT64\"},{\"name\":\"trade_date\",\"type\":\"DATE\"}]",
  "min_row_count":  "1",
  "bnc_rules_json": "[{\"field\":\"amount\",\"expectedTotal\":635000}]",
  "data_transform_query":         "SELECT JSON_VALUE(row_json,'$.trade_id') AS trade_id, ROUND(CAST(JSON_VALUE(row_json,'$.amount') AS FLOAT64) * 1.1, 2) AS amount_with_tax FROM data",
  "data_transform_min_row_count": "1"
}
```

`data_transform_query` is optional — see section 8. Real BigQuery Standard SQL, run beneath a
`WITH data AS (...)` CTE that the framework always prepends — unconditionally, not contingent on
the query referencing any placeholder token — reunifying every DaRec page for this run into a flat
rowset of JSON row strings; the operator's query just does `FROM data` like any other table, then
extracts fields with `JSON_VALUE(row_json, '$.field')`. Runs after storage integrity checks pass
and before the checkpoint is marked `COMPLETED`; its output row count is validated against
`data_transform_min_row_count`/`data_transform_max_row_count` before it replaces the stored rows —
a failure at either step leaves the original rows untouched and sets `sta_cd=FAILED_TRANSFORM`.

`bq_schema_json` is optional and BQ-only. When present, it is the authoritative schema —
`DataSourcePipelineFactory.fetchBqSchema()` uses it directly (no `bigquery.tables.get` call at
all) and every fetched row is converted strictly against it: a value that doesn't match its
declared type fails the run with a message naming the column, declared type, and offending
value, instead of a bare parse exception or a silent fallback. `type` must be a real BigQuery
SQL type name — `STRING`, `INT64`, `FLOAT64`, `BOOLEAN`, `BYTES`, `DATE`, `DATETIME`, `TIME`,
`TIMESTAMP`, `NUMERIC`, `BIGNUMERIC` — so the person editing `parameter_store` recognises it
directly. When absent,
schema resolution falls back to `BigQuerySchemaUtils.fetchBeamSchema()` (table metadata), then
to `BigQuerySourceTransform`'s own name-only preview-query fallback.

### Report configs (REPORT_PROCESSING and PIPELINE) — nested JSON in parameters_val_json

```json
{
  "override_key": false,
  "datasources":  [{"datasource_name": "trades", "datasource_subprocess": "eod",
                    "transform_alias": "raw_trades", "is_required": true}],
  "preprocessing": [],
  "transforms":   [{"step_order": 1, "step_name": "aggregate", "input_alias": "raw_trades",
                    "output_alias": "summary",
                    "query_template": "SELECT ... FROM {raw_trades}",
                    "output_bq_table": "project.dataset.table", "query_params_json": {}}],
  "outputs":      [{"output_order": 1, "input_alias": "summary", "sink_type": "GCS",
                    "output_format": "CSV", "gcs_path": "gs://bucket/reports/",
                    "file_prefix": "", "file_suffix": ".csv", "include_header": true}],
  "email":        {"to_list": ["analyst@example.com"], "cc_list": [],
                   "subject_template": "Report {periodId}", "body_template": "Attached.",
                   "from_address": "pipeline-alerts@example.com", "encrypted": false},
  "output_bq_table":       "project.dataset.daily_trades_report",
  "output_bq_input_alias": "summary"
}
```

The `datasources[]` array (with each entry's `is_required`) is the **same config PIPELINE reads**
— there is no separate pipeline config. `--processType=PIPELINE` takes the identical
`--reportName`/`--reportSubprocess` as `REPORT_PROCESSING` and runs whichever declared
datasources aren't yet `COMPLETED` before running the report; `is_required` decides whether a
still-incomplete one aborts the run. See section 4's `PipelineSequenceFactory` entry.

`periodId` is not part of the lookup key for either config type — both are period-agnostic.
Period tokens (`{periodStart}`, `{periodEnd}`) are resolved at runtime by `QueryParameterResolver`.

**Typical call sequence (report config — REPORT_PROCESSING or PIPELINE):**
```java
BigQueryReportRepository repo = new BigQueryReportRepository(options);
ReportConfig config = repo.fetchReportConfig(reportName, reportSubprocess, periodId);
```

**Typical call sequence (source config):**
```java
BigQuerySourceConfigRepository repo = new BigQuerySourceConfigRepository(options);
List<SourceConfig> configs = repo.fetchSourceConfigs(parentId, datasourceName, subprocess, periodId);
```

### CLI flags for the parameter store

| Flag | Default | Purpose |
|---|---|---|
| `--paramBqProject` | `--project` | GCP project for all config BQ tables |
| `--paramBqDataset` | `dw` | BQ dataset |
| `--paramStoreTable` | `parameter_store` | Used by DATA_SOURCE_DOWNLOAD, REPORT_PROCESSING, and PIPELINE |

**Note**: All configuration — for both DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING — is fetched
from **BigQuery** via `BigQuerySourceConfigRepository` (source configs, flat JSON) or
`BigQueryReportRepository` (report configs, nested JSON). Both read `parameter_store`. No JDBC.

---

## 11. Query token resolution — three layers in order

For both DATA_SOURCE_DOWNLOAD (BQ queries) and REPORT_PROCESSING (transform chain):

```
Layer 1 — Alias tokens (REPORT_PROCESSING only)
    resolveAliasTokens(template, aliasRegistry)
    {trades} → `project.dataset.trades_output`

Layer 2 — Standard tokens (both process types)
    QueryParameterResolver.resolve() — pass 1
    {periodStart} → options.getPeriodStart()
    {periodEnd}   → options.getPeriodEnd()
    {periodId}    → options.getPeriodId()
    {runDate}     → DateUtils.resolveRunDate(options).toString()

Layer 3 — Custom tokens (both process types, from query_params_json column,
          plus --customParamsJson from the CLI on top)
    QueryParameterResolver.resolve() — pass 2
    {exchange}  → "NYSE"    (from query_params_json)
    {threshold} → "10000"   (from query_params_json)
    Note: param values may reference standard tokens — those are resolved first.

    --customParamsJson='{"exchange":"NASDAQ"}' is the CLI-supplied equivalent of
    query_params_json, for a value that should come from the invocation itself
    (Airflow DAG conf, ad-hoc CLI run) rather than be hard-coded into the stored
    parameter_store row. On a key collision it wins over the step's own
    query_params_json — e.g. the override above makes {exchange} resolve to
    "NASDAQ" for this run only, no parameter_store edit needed. Malformed JSON
    or a non-object root throws immediately rather than silently resolving to
    nothing.

Any number of custom tokens are supported. Unknown tokens are left unchanged.
```

---

## 12. Things you must never do

| Never | Do instead |
|---|---|
| Anonymous DoFn (lambda or anon class) | Named `static final` inner class |
| `java.util.function.Function` as DoFn field | `SerializableFunction` (Beam) |
| Import from `beam-utils` or `beam-transforms` inside `beam-io` | Keep `beam-io → beam-core` only |
| Import from `beam-io` inside `beam-utils` | `beam-utils → beam-core` only |
| Merge per-source outputs with `Flatten.pCollections()` | Keep each source as an independent branch |
| Hold secrets in FrameworkOptions values | Pass Secret Manager ID, fetch value at runtime |
| Call `BigQuerySchemaUtils`, `GcsUtils`, `BigQueryReportRepository` inside a DoFn | Call in driver JVM only |
| Make any JDBC / SQL database connection | All config is in BigQuery — use BigQuerySourceConfigRepository or BigQueryReportRepository |
| Hard-code param key names in Java for REPORT_PROCESSING | Fetch required keys from schema_of_json in parameter_store |
| Create a separate source_config table | Store source connector config in parameter_store (parameters_val_json) |
| Create `TupleTag` inside `@ProcessElement` | `static final` field on the DoFn |
| Hardcode a new transform in `PipelineFactory` | Register via SPI manifest |
| Call `result.waitUntilFinish()` for streaming | Check source type first |
| Leave READMEs stale after a code change | Update in the same commit |
| Add `query_params_json` custom tokens that shadow alias names | Use distinct token names |
| Put SMTP credentials in pipeline options | Use `--smtpPasswordSecretId` + Secret Manager |

---

## 13. DataSourceCheckpointAdapter — lifecycle contract

One row per run in `DaRefer`. Used by `DATA_SOURCE_DOWNLOAD` for source run lifecycle.
`REPORT_PROCESSING` reads DaRefer (via `isCompleted()` and `fetchLatestCompletedDaId()`) to
check datasource availability, but writes its own checkpoint to `RptRefer` via `ReportCheckpointAdapter`.
`perId` is always `int`.

```
// Before pipeline.run():
long dsId = adapter.createCheckpoint(srceNm, perId, flNm)
    — inserts LOADING row (perId: int → per_id INT64 in BQ)
    — da_id = SELECT MAX(da_id)+1 FROM DaRefer  (BQ sequence)
    — vsn_no = SELECT MAX(vsn_no)+1 WHERE srce_nm=X AND per_id=Y  (per-source version)
    — returns da_id for use in all record rows and final updateStatus()

// After waitUntilFinish() / report completes:
adapter.updateStatus(daId, DataSourceCheckpoint.STA_COMPLETED, bncJson)
adapter.updateStatus(daId, DataSourceCheckpoint.STA_FAILED_BNC, bncJson)
adapter.updateStatus(daId, DataSourceCheckpoint.STA_FAILED_TRANSFORM, bncJson)
adapter.updateStatus(daId, DataSourceCheckpoint.STA_FAILED, null)

// Skip-logic check (DATA_SOURCE_DOWNLOAD):
adapter.isCompleted(srceNm, perId) — true if latest sta_cd == 'COMPLETED'
// Bypassed entirely under --manualOverrun / --overrideDownload.

// --manualOverrun: fetchLatestCompletedDaId(srceNm, perId) is called BEFORE createCheckpoint()
// to capture the run being superseded. createCheckpoint() always INSERTs a fresh DaRefer row
// (new da_id, incremented vsn_no) regardless — a re-run never overwrites or reuses a prior row.
// Once the NEW run reaches COMPLETED, PostDownloadFinalizeTransform deletes the OLD da_id's
// DaRec rows (recordAdapter.deleteRecords(previousDaId)) — DaRefer itself is never touched.

// DataSourceRecordAdapter — validates written records:
recordAdapter.countRecords(daId)               — COUNT(*) for row-count check
recordAdapter.sumField(daId, "amount")         — SUM(JSON_VALUE(row_da_json_tx, '$.amount'))
recordAdapter.deleteRecords(daId)              — DELETE FROM DaRec WHERE da_id=@daId (best-effort)

// bal_and_cntl_smry_tx JSON written on COMPLETED, FAILED_BNC, or FAILED_TRANSFORM:
{ "status": "Matched", "pipelineRowCount": 1000, "storedRowCount": 1000, "transformOutputRowCount": 950, ... }
```

---

## 14. BigQueryJobService — BQ job contract

Used in the driver JVM (`ReportPipelineFactory`) and — since its no-arg constructor holds only a
plain `BigQuery` client, no `FrameworkOptions` — also safe to instantiate in `@Setup` inside a
Beam worker DoFn: `PostDownloadFinalizeTransform.FinalizeDoFn` does this to run
`data_transform_query`.

```java
// Run a query and materialise result to a BQ table
bqJobService.runQueryToTable(resolvedSql, "project.dataset.table");

// Run a query with no destination (DDL, DML)
bqJobService.runQuery(resolvedSql);

// Exact live row count (SELECT COUNT(*), not table-metadata based)
bqJobService.countRows("project.dataset.table");

// Best-effort cleanup of a temp/staging table — logs and swallows failure, never throws
bqJobService.dropTableIfExists("project.dataset.tmp_table");

// Export BQ table to GCS as CSV
bqJobService.exportToCsv("project.dataset.table", "gs://bucket/path/file.csv", includeHeader);

// Export BQ table to GCS as newline-delimited JSON
bqJobService.exportToJson("project.dataset.table", "gs://bucket/path/file.json");
```

Table refs use `project.dataset.table` (dot-separated, 3 parts) or `dataset.table` (2 parts).
All methods block until the BQ job completes. Failures throw `RuntimeException`
(except `dropTableIfExists`, which is intentionally best-effort).

---

## 15. Email adapter contracts

Two separate, unrelated email contracts exist, used by two different callers:

### ReportEmailAdapter — DATA_SOURCE_DOWNLOAD failure email only

```java
// Interface (beam-io)
ReportEmailAdapter.send(subject, body, to, cc, attachments);

// io/email/EmailAttachment — InputStream is consumed exactly once by the adapter
EmailAttachment.csv(inputStream, "report.csv")    // contentType=text/csv
EmailAttachment.json(inputStream, "report.json")  // contentType=application/json

// Concrete impl (beam-runner — needs jakarta.mail from beam-transforms transitive dep)
new SmtpReportEmailAdapter(smtpHost, smtpPort, smtpPasswordSecretId, fromAddress)
```

Used only by `PostDownloadFinalizeTransform`'s `SourceFailureEmailConfig`-driven failure email —
constructed there as `new SmtpReportEmailAdapter(emailConfig.smtpHost, emailConfig.smtpPort,
emailConfig.smtpPasswordSecretId, emailConfig.fromAddress)`, all four values from
`SourceFailureEmailConfig`. To add a different provider (SendGrid, SES) for this path, implement
`ReportEmailAdapter` and construct it in place of `SmtpReportEmailAdapter`.

### EmailSendUtility — REPORT_PROCESSING/PIPELINE report-completion email

```java
// Interface (beam-io) — no implementation ships in this repo; see below
EmailParams params = emailUtility.SetEmailParams(fromAddress, subject, toList, ccList, encrypted);

// model/EmailAttachment — a different type from io/email/EmailAttachment above
List<EmailAttachment> attachments = files.stream()
    .map(f -> new EmailAttachment(f.fileName(), emailUtility.FetchFileFromGcs(f.gcsUri()), f.contentType()))
    .toList();

emailUtility.CreateEmailRequest(params, bodyHtml, attachments);
```

Used only by `ReportPipelineFactory`'s report-completion email. This repository defines the
contract but ships no implementation — the real one is expected to be an organization's own
existing email-gateway client, supplied at runtime rather than committed here. Two ways to plug
one in:
1. **Java SPI** (preferred, zero code change here) — a JAR on the classpath declaring
   `META-INF/services/com.yourco.beam.io.email.EmailSendUtility` with the implementation's
   fully-qualified class name. `ReportPipelineFactory.discoverEmailUtility()` finds it via
   `ServiceLoader.load(EmailSendUtility.class)`, the same mechanism `TransformRegistry` uses for
   `BeamTransform`.
2. **Constructor injection** — pass an `EmailSendUtility` instance to
   `ReportPipelineFactory`'s 3-arg constructor directly (useful for tests).

If neither is available, `sendEmail()` logs a warning and skips sending — it does not fail the
report.

---

## 16. Checkpoint and record tables — unified tracking

| Concept | Table | Written by | Read by |
|---|---|---|---|
| "Start/end of a DATA_SOURCE_DOWNLOAD run" | `DaRefer` | `DataSourcePipelineFactory` | `DataSourcePipelineFactory` (skip logic), `ReportPipelineFactory` (DS availability check + da_id lookup), `PipelineSequenceFactory` (post-batch-job required/optional gate, via the same `isCompleted()`) |
| "Loaded rows from any source" | `DaRec` | `DataSourceRecordSinkTransform` (Beam workers); also `PostDownloadFinalizeTransform` — replaces a run's own rows once a validated `data_transform_query` applies, and deletes a superseded run's rows under `--manualOverrun` | `BigQueryDataSourceRecordAdapter` (BnC validation), `ReportCheckpointAdapter` (staging into RptStageDa) |
| "Start/end of a REPORT_PROCESSING run" | `RptRefer` | `ReportPipelineFactory` via `ReportCheckpointAdapter` | — |
| "Report run → datasource mapping" | `RptDaMap` | `ReportPipelineFactory` via `ReportCheckpointAdapter` | `ReportCheckpointAdapter.clearStagedData()` |
| "Staged datasource pages for current report" | `RptStageDa` | `ReportCheckpointAdapter.stageFromDaRec()` — one row per `DaRec` page, batched, not per record | Transform chain (as subquery alias, un-nested back to individual records by `stagedDataSubquery()`) |
| "One row per output step of a report" | `RptOutput` | `ReportPipelineFactory` via `ReportCheckpointAdapter.writeOutput()` | — |

DATA_SOURCE_DOWNLOAD lifecycle: `LOADING → COMPLETED / FAILED_BNC / FAILED_TRANSFORM / FAILED`.
REPORT_PROCESSING lifecycle: `LOADING → COMPLETED / FAILED`.
All rows from one DATA_SOURCE_DOWNLOAD run share the same `da_id` — shard-safe.
`vsn_no` in DaRefer increments each time `(srce_nm, per_id)` is re-run.
`vsn_no` in RptOutput increments each time `(rpt_id, outpt_cd)` produces another version.
`perId` is stored as `INT64` in all tables (Java `int`).
Under `--manualOverrun`, a re-run's DaRefer row is always a fresh INSERT — the previous run's
DaRefer row is never modified or deleted, only its `DaRec` rows are removed, and only after the
new run reaches `COMPLETED`.

---

## 17. Build and run reference

```bash
# Run unit tests (currently: beam-io and beam-utils pure-logic classes only — see their READMEs)
mvn -pl beam-io,beam-utils -am test

# Build fat JAR from project root
mvn package -pl beam-runner -am -DskipTests

# Run DATA_SOURCE_DOWNLOAD locally (DirectRunner) — source config read from parameter_store
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --processType=DATA_SOURCE_DOWNLOAD \
  --parentId=TRADING \
  --datasourceName=trades \
  --subprocessName=eod \
  --periodId=202401 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=dw \
  --checkpointBqProject=my-gcp-project \
  --checkpointBqDataset=pipeline_metadata

# Force re-run when DaRefer already shows COMPLETED (explicit operator override)
# --manualOverrun=true

# Run REPORT_PROCESSING (BQ-configured) — no JDBC required
# NOTE: --emailSmtpHost and --smtpPasswordSecretId are no longer CLI flags.
#       Add them as rows in parameter_store (keys: email_smtp_host, smtp_password_secret_id).
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --processType=REPORT_PROCESSING \
  --reportName=daily_trades_report \
  --reportSubprocess=eod \
  --periodId=2024-01 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=dw

# Run PIPELINE (same --reportName/--reportSubprocess as REPORT_PROCESSING — no separate pipeline
# config; runs whichever datasources the report's own datasources[] declares, then the report)
java -jar beam-runner/target/beam-runner-1.0.0-SNAPSHOT-bundled.jar \
  --runner=DirectRunner \
  --processType=PIPELINE \
  --parentId=TRADING \
  --reportName=daily_trades_report \
  --reportSubprocess=eod \
  --periodId=202401 \
  --periodStart=2024-01-01 \
  --periodEnd=2024-01-31 \
  --paramBqProject=my-gcp-project \
  --paramBqDataset=dw \
  --checkpointBqProject=my-gcp-project \
  --checkpointBqDataset=pipeline_metadata

# Run ExampleWorkflow (BQ params → BQ transform → GCS CSV)
# See EXAMPLE.md for required BQ table setup
mvn -pl beam-runner exec:java \
  -Dexec.mainClass=com.yourco.beam.runner.example.ExampleWorkflow \
  "-Dexec.args=--project=my-gcp-project --paramBqProject=my-gcp-project \
    --paramBqDataset=dw --reportName=daily_trades_summary \
    --reportSubprocess=eod --periodId=2024-01 \
    --periodStart=2024-01-01 --periodEnd=2024-01-31 \
    --processType=REPORT_PROCESSING"
```

---

## 18. Key invariants to preserve

1. `beam-core` has zero dependencies on sibling modules — it is the root.
2. `beam-io` depends only on `beam-core` — never on `beam-utils` or `beam-transforms`.
3. All Beam sources return `PCollection<Row>` with `.setRowSchema()` called.
4. Each `SourceConfig` produces exactly one independent Beam branch — never merged.
5. `BeamTransform` implementations always output to both `SUCCESS_TAG` and `DEAD_LETTER_TAG`.
6. Secrets are never stored in `FrameworkOptions` values — only Secret Manager IDs.
7. `BigQueryJobService`, `GcsUtils`, `BigQueryReportRepository`, `BigQuerySourceConfigRepository`, and `BigQueryParameterAdapter` are driver-JVM only — never inside DoFns.
11. No JDBC. No SQL database connections. All config and data flow through BigQuery.
12. No separate source_config table. All source connector config is stored in `parameter_store` as JSON in `parameters_val_json`.
8. Query token resolution order is always: alias tokens → standard tokens → custom tokens.
9. Every code change is accompanied by a README update in the same commit.
10. A `data_source_checkpoints` LOADING row is created before every source download or report run, and updated to COMPLETED / FAILED_BNC / FAILED after. All data rows go to `data_source_records` as JSON blobs.
