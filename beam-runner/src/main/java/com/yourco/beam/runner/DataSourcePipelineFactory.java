package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.sink.DataSourceRecordSinkTransform;
import com.yourco.beam.io.source.SourceRouter;
import com.yourco.beam.model.BqFetchConfig;
import com.yourco.beam.model.RunDates;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import com.yourco.beam.utils.BigQuerySchemaUtils;
import com.yourco.beam.utils.RunDateCalculator;
import com.yourco.beam.utils.QueryParameterResolver;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Assembles a {@code DATA_SOURCE_DOWNLOAD} pipeline.
 *
 * <h2>Per-source independent branches</h2>
 * Each {@link SourceConfig} produces an independent Beam DAG branch:
 * <pre>
 *   source read → transform chain → DataSourceRecordSinkTransform (streaming inserts → DaRec)
 *                                           ↓
 *                               PostDownloadFinalizeTransform
 *                    (BnC validation + checkpoint update + failure email — runs in worker)
 * </pre>
 *
 * <h2>Single-flow design</h2>
 * Checkpoint creation (LOADING) happens in the driver JVM before the pipeline is assembled.
 * All post-write steps — row-count validation, BnC checks, and the terminal checkpoint update
 * (COMPLETED / FAILED_BNC / FAILED) — run inside {@link PostDownloadFinalizeTransform} as the
 * last step of each source branch. No external post-pipeline invocation is needed.
 */
public final class DataSourcePipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourcePipelineFactory.class);

    /**
     * Validates parameters, creates LOADING checkpoints, assembles the Beam pipeline graph,
     * and returns it ready for {@code run()} in {@link Main} — or an empty assembly
     * ({@link DataSourceAssembly#isEmpty()}) when no source is eligible and pending, which
     * {@code Main} must not submit.
     *
     * <p>Does NOT call {@code pipeline.run()} — that is the caller's responsibility.
     */
    public DataSourceAssembly assemble(FrameworkOptions options) {
        LOG.info("DATA_SOURCE_DOWNLOAD | datasource={} | period={} | subprocess={}",
                 options.getDatasourceName(), options.getPeriodId(), options.getSubprocessName());

        validateRequiredParameters(options);

        BigQuerySourceConfigRepository bqRepo = new BigQuerySourceConfigRepository(options);
        List<SourceConfig> sourceConfigs;
        try {
            sourceConfigs = bqRepo.fetchSourceConfigs(
                options.getParentId(), options.getDatasourceName(),
                options.getSubprocessName(), options.getPeriodId());
        } catch (Exception e) {
            throw DataSourceDownloadException.wrap(DataSourceDownloadException.Reason.INVALID_INPUT,
                options.getDatasourceName(), options.getSubprocessName(), options.getPeriodId(), e);
        }
        LOG.info("Found {} source config(s) for this run", sourceConfigs.size());

        return assembleForConfigs(options, sourceConfigs);
    }

    /**
     * Same assembly as {@link #assemble} — checkpoint filtering, {@code --manualOverrun}
     * previous-{@code da_id} capture, LOADING checkpoint creation, per-source Beam branch
     * assembly — starting from an explicitly supplied list of {@link SourceConfig} instead of
     * fetching by a single {@code --datasourceName}. Used by {@code PipelineSequenceFactory} to
     * batch every {@code DATA_SOURCE} step of a {@code PIPELINE} run's still-pending sources
     * (each fetched by its own name, possibly several different datasources) into one Dataflow
     * job — sources already {@code COMPLETED} for the period are still skipped here exactly as
     * they are for a standalone {@code DATA_SOURCE_DOWNLOAD} run, via the same
     * {@link #filterByCheckpoint}.
     *
     * <p>Does NOT call {@code pipeline.run()} — that is the caller's responsibility. Assigns a
     * {@code jobRunId} exactly like {@link #assemble} does, if the caller hasn't already.
     *
     * <p>Returns a {@link DataSourceAssembly} bundling the pipeline with each source branch's
     * {@link PostDownloadFinalizeTransform} output signal, so a caller batching several
     * datasources into one job (e.g. {@code PipelineSequenceFactory}) can wire a downstream step
     * with {@code Wait.on(assembly.finalizeSignals)} onto the SAME pipeline before submitting it.
     */
    public DataSourceAssembly assembleForConfigs(FrameworkOptions options, List<SourceConfig> sourceConfigs) {
        try {
            return doAssembleForConfigs(options, sourceConfigs);
        } catch (DataSourceDownloadException e) {
            throw e;
        } catch (Exception e) {
            String names = sourceConfigs.stream().map(c -> c.datasourceName).distinct()
                .collect(Collectors.joining(","));
            int periodId = sourceConfigs.isEmpty() ? options.getPeriodId() : sourceConfigs.get(0).periodId;
            DataSourceDownloadException.Reason reason =
                (e instanceof IllegalArgumentException || e instanceof IllegalStateException
                    || e instanceof UnsupportedOperationException)
                ? DataSourceDownloadException.Reason.INVALID_INPUT
                : DataSourceDownloadException.Reason.CONNECTIVITY_FAILURE;
            throw DataSourceDownloadException.wrap(reason, names, null, periodId, e);
        }
    }

    private DataSourceAssembly doAssembleForConfigs(FrameworkOptions options, List<SourceConfig> sourceConfigs) {
        String jobRunId = options.getJobRunId();
        if (jobRunId == null || jobRunId.isBlank()) {
            jobRunId = UUID.randomUUID().toString();
            options.setJobRunId(jobRunId);
        }
        LOG.info("Assembling {} source config(s) | jobRunId={}", sourceConfigs.size(), jobRunId);

        // Each source gets its own run dates, resolved before the COMPLETED skip-check and
        // checkpoint creation below, so DaRefer is keyed by the resolved periodId — the same value
        // a report reading this source resolves to — not the raw --periodId.
        // Finance Automation scheduling (RunDateCalculator): each source is evaluated on its own
        // — WHEN may it run on the Business Date, and WHICH period does it process. Only an
        // ELIGIBLE source goes on to the COMPLETED check and gets a DaRefer row and a branch.
        // A skip is not persisted anywhere: BAU re-evaluates every item on every execution, so a
        // NOT_YET_ELIGIBLE source is simply picked up by a later run inside its window.
        // The source's periodId is replaced with its calculated one before the COMPLETED check
        // and checkpoint creation, so DaRefer is keyed by the period actually processed.
        Map<String, RunDates> runDates = new HashMap<>();
        List<SourceConfig> datedConfigs = new ArrayList<>();
        for (SourceConfig config : sourceConfigs) {
            RunDateCalculator.ScheduleDecision decision =
                RunDateCalculator.evaluateDataSource(config.runScheduleConfig, options);
            if (!decision.shouldRun()) {
                reportSkip(options, config, decision);
                continue;
            }
            LOG.info("Source '{}' eligible: {}", config.datasourceName, decision);
            runDates.put(config.datasourceName, decision.dates);
            datedConfigs.add(config.toBuilder().periodId(decision.dates.periodId).build());
        }

        BigQueryDataSourceCheckpointAdapter checkpointAdapter =
            new BigQueryDataSourceCheckpointAdapter(options);

        List<SourceConfig> toProcess = filterByCheckpoint(datedConfigs, checkpointAdapter, options);
        if (toProcess.isEmpty()) {
            LOG.info("Nothing to process: of {} source(s), {} not eligible today, the rest "
                     + "already completed (set --manualOverrun=true to force a re-run).",
                     sourceConfigs.size(), sourceConfigs.size() - datedConfigs.size());
            return new DataSourceAssembly(Pipeline.create(options), new ArrayList<>());
        }
        LOG.info("Will process {} of {} source(s)", toProcess.size(), sourceConfigs.size());

        // Under --manualOverrun, capture each source's previous COMPLETED da_id (if any) BEFORE
        // creating the new checkpoint below. Once the new run reaches COMPLETED,
        // PostDownloadFinalizeTransform deletes this previous da_id's DaRec rows — DaRefer itself
        // is never touched, only a new row is ever inserted (see createCheckpoint() below), so the
        // full run history stays intact; only the superseded bulk row data is reclaimed.
        boolean manualOverrun = options.getManualOverrun();
        Map<String, Long> previousDaIds = new HashMap<>();
        if (manualOverrun) {
            for (SourceConfig config : toProcess) {
                try {
                    long prevDaId = checkpointAdapter.fetchLatestCompletedDaId(
                        config.datasourceName, config.periodId);
                    previousDaIds.put(config.datasourceName, prevDaId);
                    LOG.info("manualOverrun: '{}' will supersede previous COMPLETED da_id={}",
                             config.datasourceName, prevDaId);
                } catch (IllegalArgumentException e) {
                    LOG.debug("manualOverrun: no previous COMPLETED da_id for '{}' — nothing to supersede",
                              config.datasourceName);
                }
            }
        }

        // Create LOADING checkpoints — one per source, before any worker touches the data.
        // Always a fresh INSERT (new da_id, incremented vsn_no) — re-runs never overwrite or
        // reuse a prior DaRefer row, even under --manualOverrun.
        Map<String, Long> dataSourceIds = new HashMap<>();
        for (SourceConfig config : toProcess) {
            long dsId = checkpointAdapter.createCheckpoint(
                config.datasourceName, config.periodId, extractDsNm(config));
            dataSourceIds.put(config.datasourceName, dsId);
            LOG.info("DaRefer LOADING row created for '{}': da_id={}", config.datasourceName, dsId);
        }

        return assemblePipeline(options, toProcess, dataSourceIds, previousDaIds, runDates);
    }

    /**
     * Logs a source skipped by its schedule. The ordinary BAU outcomes — not yet eligible,
     * expired window, a non-business day — are expected and only logged (even under
     * {@code --manualOverrun}, which never overrides eligibility). NOT_EVALUABLE
     * (bad frequency, missing/unknown calendarKey, no calendar provider, unparseable WD/lag) is a
     * configuration problem: it is also sent through {@link FailureNotifier} as a
     * {@code DataSourceDownloadException(INVALID_INPUT)} so it isn't lost in the logs — without
     * failing the other sources in the same run (BAU: the failing item is skipped and re-evaluated
     * next execution; others carry on).
     */
    private static void reportSkip(FrameworkOptions options, SourceConfig config,
                                   RunDateCalculator.ScheduleDecision decision) {
        if (decision.status != RunDateCalculator.ScheduleDecision.Status.NOT_EVALUABLE) {
            LOG.info("Skipping source '{}': {}", config.datasourceName, decision);
            return;
        }
        LOG.error("Skipping source '{}' — run schedule could not be evaluated: {}",
                  config.datasourceName, decision);
        FailureNotifier.notify(options, new DataSourceDownloadException(
            DataSourceDownloadException.Reason.INVALID_INPUT, config.datasourceName,
            config.subprocessName, config.periodId,
            "Run schedule for '" + config.datasourceName + "' could not be evaluated on business date "
            + decision.businessDate + ": " + decision.detail + " — source skipped, "
            + "will be re-evaluated on the next run", null));
    }

    // ── Graph assembly ────────────────────────────────────────────────────────

    private static DataSourceAssembly assemblePipeline(FrameworkOptions options,
                                             List<SourceConfig> configs,
                                             Map<String, Long> dataSourceIds,
                                             Map<String, Long> previousDaIds,
                                             Map<String, RunDates> runDates) {
        Pipeline  pipeline = Pipeline.create(options);
        List<PCollection<?>> finalizeSignals = new ArrayList<>();

        // Pre-compute the checkpoint table refs once — passed to FinalizeDoFn fields (Strings are
        // serializable; FrameworkOptions is not, so we extract what we need here in the driver JVM).
        String project        = options.getCheckpointBqProject() != null
                                && !options.getCheckpointBqProject().isBlank()
                                ? options.getCheckpointBqProject() : options.getProject();
        String daReferTableRef = "`" + project + "." + options.getCheckpointBqDataset()
                               + "." + options.getDaReferTable() + "`";
        String daRecTableRef   = "`" + project + "." + options.getCheckpointBqDataset()
                               + "." + options.getDaRecTable() + "`";

        for (SourceConfig config : configs) {
            long dsId = dataSourceIds.get(config.datasourceName);
            LOG.info("Assembling source branch: {} ({}) → DaRec (da_id={})",
                     config.datasourceName, config.sourceType, dsId);

            RunDates dates = runDates.get(config.datasourceName);
            SourceConfig resolved = resolveQueryTokens(config, options, dates);
            Schema bqSchema = fetchBqSchema(config);
            PCollection<Row> sourceData = SourceRouter.routeFromConfig(
                pipeline, resolved, options, dates.runDate, bqSchema);

            PCollection<Row> transformed = SourceTransformChainAssembler.assemble(
                sourceData, config, options, pipeline);

            // Write rows to DaRec (streaming inserts) and emit count after all inserts commit
            PCollection<Long> writtenCount = transformed.apply(
                "RecordSink-" + config.datasourceName,
                new DataSourceRecordSinkTransform(options,
                    ValueProvider.StaticValueProvider.of(dsId)));

            // Finalize: row/BnC validation, optional data_transform_query, checkpoint update,
            // manualOverrun cleanup, and failure email — all in the worker. Its output is a
            // signal element (this source's da_id) always emitted on completion, success or
            // failure, so a downstream Wait.on() can gate on it without polling.
            long previousDaId = previousDaIds.getOrDefault(config.datasourceName, -1L);
            PCollection<Long> finalizeSignal = writtenCount.apply(
                "Finalize-" + config.datasourceName,
                new PostDownloadFinalizeTransform(
                    dsId, config, daReferTableRef, daRecTableRef, previousDaId));
            finalizeSignals.add(finalizeSignal);
        }

        return new DataSourceAssembly(pipeline, finalizeSignals);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * For BQ sources, resolves {periodStart}/{periodEnd}/{periodId}/{runDate} (from this source's
     * own {@code dates}) and custom
     * tokens in {@code bqFetchConfig.query} before the query reaches BigQueryIO.
     * Must run here in beam-runner (not in beam-io SourceRouter) because
     * QueryParameterResolver is in beam-utils and beam-io cannot depend on beam-utils.
     * Non-BQ sources are returned unchanged.
     */
    private static SourceConfig resolveQueryTokens(SourceConfig config, FrameworkOptions options,
                                                   RunDates dates) {
        if (config.sourceType != SourceType.BQ
                || config.bqFetchConfig == null
                || !config.bqFetchConfig.hasQuery()) {
            return config;
        }
        BqFetchConfig bq = config.bqFetchConfig;
        String resolvedQuery = QueryParameterResolver.resolve(bq.query, bq.queryParams, options, dates);
        BqFetchConfig resolvedBq = new BqFetchConfig(
            bq.projectId, bq.dataset, bq.table, resolvedQuery, bq.queryParams, bq.schema);
        // toBuilder() copies every existing field first, so swapping in the token-resolved
        // bqFetchConfig here can never silently drop a field the way manually re-listing every
        // field in a fresh SourceConfig.builder() call can (it once did, for runScheduleConfig).
        return config.toBuilder().bqFetchConfig(resolvedBq).build();
    }

    /**
     * Resolves the Beam Schema for a BQ table source at driver-JVM time.
     * Returns null for non-BQ sources, query-only sources with no declared schema (no static
     * table to inspect), or when metadata fetch fails (logs a warning and continues with the
     * generic fallback in {@code BigQuerySourceTransform}).
     *
     * <p>Resolution order:
     * <ol>
     *   <li>{@link BqFetchConfig#schema} — an explicit column list the operator declared via
     *       {@code bq_schema_json} in {@code parameter_store}. When present, this is used
     *       directly and no BigQuery metadata call is made at all; a bad declared type name
     *       throws {@link IllegalArgumentException} uncaught, failing the run before any data
     *       moves rather than silently falling back.</li>
     *   <li>{@code BigQuerySchemaUtils.fetchBeamSchema()} — table-metadata lookup. Must be
     *       called here in beam-runner because {@link BigQuerySchemaUtils} is in beam-utils
     *       and beam-io cannot depend on beam-utils.</li>
     *   <li>{@code null} — {@code BigQuerySourceTransform} resolves column names itself via a
     *       preview query when this returns null.</li>
     * </ol>
     */
    private static Schema fetchBqSchema(SourceConfig config) {
        if (config.sourceType != SourceType.BQ || config.bqFetchConfig == null) return null;
        BqFetchConfig bq = config.bqFetchConfig;

        if (bq.hasSchema()) {
            Schema schema = BigQuerySchemaUtils.toBeamSchema(bq.schema);
            LOG.info("Using explicitly declared schema ({} fields, bq_schema_json) for BQ source "
                     + "'{}' — no table-metadata fetch needed",
                     schema.getFieldCount(), config.datasourceName);
            return schema;
        }

        if (bq.table == null || bq.table.isBlank()) {
            LOG.debug("BQ source '{}' is query-only with no declared schema — using generic fallback",
                      config.datasourceName);
            return null;
        }
        try {
            Schema schema = BigQuerySchemaUtils.fetchBeamSchema(bq.tableRef());
            LOG.info("Fetched typed schema ({} fields) for BQ source '{}'",
                     schema.getFieldCount(), config.datasourceName);
            return schema;
        } catch (Exception e) {
            LOG.warn("Could not fetch BQ schema for source '{}' ({}): {} — using generic fallback",
                     config.datasourceName, bq.tableRef(), e.getMessage());
            return null;
        }
    }

    private static String extractDsNm(SourceConfig config) {
        if (config.bqFetchConfig != null) {
            return config.bqFetchConfig.projectId + "."
                + config.bqFetchConfig.dataset + "."
                + config.bqFetchConfig.table;
        }
        if (config.fileConfig != null && config.fileConfig.location != null) {
            return config.fileConfig.location;
        }
        if (config.apiConfig != null && config.apiConfig.endpoint != null) {
            return config.apiConfig.endpoint;
        }
        return config.datasourceName;
    }

    private static void validateRequiredParameters(FrameworkOptions options) {
        if (options.getDatasourceName() == null || options.getDatasourceName().isBlank()) {
            throw new DataSourceDownloadException(DataSourceDownloadException.Reason.INVALID_INPUT,
                options.getDatasourceName(), options.getSubprocessName(), options.getPeriodId(),
                "--datasourceName is required for DATA_SOURCE_DOWNLOAD", null);
        }
        // --periodId is NOT required here: a source with a run schedule gets its period id
        // calculated from the Business Date (RunDateCalculator). A source without one is reported
        // as not evaluable per item, with a message asking for --periodId.
    }

    private static List<SourceConfig> filterByCheckpoint(List<SourceConfig> configs,
                                                          BigQueryDataSourceCheckpointAdapter adapter,
                                                          FrameworkOptions options) {
        if (options.getManualOverrun()) {
            LOG.info("--manualOverrun=true: skipping COMPLETED checkpoint guard, re-downloading all sources");
            return configs;
        }
        return configs.stream()
            .filter(config -> {
                boolean done = adapter.isCompleted(config.datasourceName, config.periodId);
                if (done) {
                    LOG.info("Skipping '{}' (period={}, parent={}) — COMPLETED row found in DaRefer. "
                             + "Pass --manualOverrun=true to force a re-run.",
                             config.datasourceName, config.periodId, config.parentId);
                }
                return !done;
            })
            .collect(Collectors.toList());
    }
}
