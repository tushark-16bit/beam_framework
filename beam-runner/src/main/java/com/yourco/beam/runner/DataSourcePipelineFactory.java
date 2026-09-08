package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.io.config.BigQuerySourceConfigRepository;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.sink.DataSourceRecordSinkTransform;
import com.yourco.beam.io.source.SourceRouter;
import com.yourco.beam.model.BqFetchConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import com.yourco.beam.utils.BigQuerySchemaUtils;
import com.yourco.beam.utils.DateUtils;
import com.yourco.beam.utils.QueryParameterResolver;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
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
     * and returns the pipeline ready for {@code run()} in {@link Main}.
     *
     * <p>Does NOT call {@code pipeline.run()} — that is the caller's responsibility.
     */
    public Pipeline assemble(FrameworkOptions options) {
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
     */
    public Pipeline assembleForConfigs(FrameworkOptions options, List<SourceConfig> sourceConfigs) {
        try {
            return doAssembleForConfigs(options, sourceConfigs);
        } catch (DataSourceDownloadException e) {
            throw e;
        } catch (Exception e) {
            String names = sourceConfigs.stream().map(c -> c.datasourceName).distinct()
                .collect(Collectors.joining(","));
            int periodId = sourceConfigs.isEmpty() ? options.getPeriodId() : sourceConfigs.get(0).periodId;
            DataSourceDownloadException.Reason reason =
                (e instanceof IllegalArgumentException || e instanceof IllegalStateException)
                ? DataSourceDownloadException.Reason.INVALID_INPUT
                : DataSourceDownloadException.Reason.CONNECTIVITY_FAILURE;
            throw DataSourceDownloadException.wrap(reason, names, null, periodId, e);
        }
    }

    private Pipeline doAssembleForConfigs(FrameworkOptions options, List<SourceConfig> sourceConfigs) {
        String jobRunId = options.getJobRunId();
        if (jobRunId == null || jobRunId.isBlank()) {
            jobRunId = UUID.randomUUID().toString();
            options.setJobRunId(jobRunId);
        }
        LOG.info("Assembling {} source config(s) | jobRunId={}", sourceConfigs.size(), jobRunId);

        BigQueryDataSourceCheckpointAdapter checkpointAdapter =
            new BigQueryDataSourceCheckpointAdapter(options);

        List<SourceConfig> toProcess = filterByCheckpoint(sourceConfigs, checkpointAdapter, options);
        if (toProcess.isEmpty()) {
            LOG.info("All {} source(s) already completed. "
                     + "Set --overrideDownload=true to force re-download.", sourceConfigs.size());
            return Pipeline.create(options);
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

        return assemblePipeline(options, toProcess, dataSourceIds, previousDaIds);
    }

    // ── Graph assembly ────────────────────────────────────────────────────────

    private static Pipeline assemblePipeline(FrameworkOptions options,
                                             List<SourceConfig> configs,
                                             Map<String, Long> dataSourceIds,
                                             Map<String, Long> previousDaIds) {
        Pipeline  pipeline = Pipeline.create(options);
        LocalDate runDate  = DateUtils.resolveRunDate(options);
        LOG.info("Effective run date: {}", runDate);

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

            SourceConfig resolved = resolveQueryTokens(config, options);
            Schema bqSchema = fetchBqSchema(config);
            PCollection<Row> sourceData = SourceRouter.routeFromConfig(
                pipeline, resolved, options, runDate, bqSchema);

            PCollection<Row> transformed = SourceTransformChainAssembler.assemble(
                sourceData, config, options, pipeline);

            // Write rows to DaRec (streaming inserts) and emit count after all inserts commit
            PCollection<Long> writtenCount = transformed.apply(
                "RecordSink-" + config.datasourceName,
                new DataSourceRecordSinkTransform(options,
                    ValueProvider.StaticValueProvider.of(dsId)));

            // Finalize: row/BnC validation, optional data_transform_query, checkpoint update,
            // manualOverrun cleanup, and failure email — all in the worker
            long previousDaId = previousDaIds.getOrDefault(config.datasourceName, -1L);
            writtenCount.apply(
                "Finalize-" + config.datasourceName,
                new PostDownloadFinalizeTransform(
                    dsId, config, daReferTableRef, daRecTableRef, previousDaId));
        }

        return pipeline;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * For BQ sources, resolves {periodStart}/{periodEnd}/{periodId}/{runDate} and custom
     * tokens in {@code bqFetchConfig.query} before the query reaches BigQueryIO.
     * Must run here in beam-runner (not in beam-io SourceRouter) because
     * QueryParameterResolver is in beam-utils and beam-io cannot depend on beam-utils.
     * Non-BQ sources are returned unchanged.
     */
    private static SourceConfig resolveQueryTokens(SourceConfig config, FrameworkOptions options) {
        if (config.sourceType != SourceType.BQ
                || config.bqFetchConfig == null
                || !config.bqFetchConfig.hasQuery()) {
            return config;
        }
        BqFetchConfig bq = config.bqFetchConfig;
        String resolvedQuery = QueryParameterResolver.resolve(bq.query, bq.queryParams, options);
        BqFetchConfig resolvedBq = new BqFetchConfig(
            bq.projectId, bq.dataset, bq.table, resolvedQuery, bq.queryParams, bq.schema);
        return SourceConfig.builder()
            .parentId(config.parentId)
            .datasourceName(config.datasourceName)
            .periodId(config.periodId)
            .subprocessName(config.subprocessName)
            .sourceType(config.sourceType)
            .bqFetchConfig(resolvedBq)
            .queryConfig(config.queryConfig)
            .sourceTransforms(new java.util.ArrayList<>(config.sourceTransforms))
            .validationConfig(config.validationConfig)
            .failureEmailConfig(config.failureEmailConfig)
            .dataTransformConfig(config.dataTransformConfig)
            .build();
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
        if (options.getPeriodId() <= 0) {
            throw new DataSourceDownloadException(DataSourceDownloadException.Reason.INVALID_INPUT,
                options.getDatasourceName(), options.getSubprocessName(), options.getPeriodId(),
                "--periodId is required for DATA_SOURCE_DOWNLOAD", null);
        }
    }

    private static List<SourceConfig> filterByCheckpoint(List<SourceConfig> configs,
                                                          BigQueryDataSourceCheckpointAdapter adapter,
                                                          FrameworkOptions options) {
        boolean forceRerun = options.getManualOverrun() || options.getOverrideDownload();
        if (forceRerun) {
            String flag = options.getManualOverrun() ? "--manualOverrun" : "--overrideDownload";
            LOG.info("{} = true: skipping COMPLETED checkpoint guard, re-downloading all sources", flag);
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
