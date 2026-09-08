package com.yourco.beam.model;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Represents one row in the {@code RptStageDa} staging table.
 *
 * <p>Staged rows are copied from {@code DaRec} into this table at the start of a report run
 * (keyed by {@code map_id} from {@code RptDaMap}) — one {@code RptStageDa} row per {@code DaRec}
 * <em>page</em> (≤250 source records as a JSON array, or a FILE-with-header source's
 * {@code {"Data":[...],"DataHeaders":[...]}}), copied across unchanged, exactly mirroring
 * {@code DaRec}'s own pagination. {@link #stageDsJsonTx} therefore holds a <em>page</em>, not a
 * single record.
 *
 * <p>Report transform SQL never sees that pagination: {@code stagedDataSubquery(mapId)} (see
 * {@code ReportCheckpointAdapter}) un-nests every page back into individual source-row JSON
 * objects on read, so {@code {alias}} substitution in a {@code query_template} still resolves to
 * one row per record with column {@code stage_ds_json_tx} — identical to before this table was
 * batched. All rows for a report run are deleted after the transformation chain completes —
 * staged data is purely transient.
 *
 * <h2>BQ table schema (RptStageDa)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.RptStageDa (
 *   stage_id          INT64   NOT NULL,   -- surrogate PK (computed via ROW_NUMBER), one per page
 *   map_id            INT64   NOT NULL,   -- FK → RptDaMap.map_id
 *   stage_ds_json_tx  STRING  NOT NULL,   -- one DaRec page's JSON, copied verbatim (≤250 records)
 *   query_config_tx   STRING,             -- JSON: {"da_id": N, "map_id": N}
 *   load_dt           DATE    NOT NULL,
 *   lst_updt_ts       DATETIME NOT NULL
 * );
 * }</pre>
 */
public final class RptStageDa implements Serializable {

    private static final long serialVersionUID = 1L;

    public final long          stageId;
    public final long          mapId;
    public final String        stageDsJsonTx;
    public final String        queryConfigTx;
    public final LocalDate     loadDt;
    public final LocalDateTime lstUpdtTs;

    public RptStageDa(long stageId, long mapId, String stageDsJsonTx,
                      String queryConfigTx, LocalDate loadDt, LocalDateTime lstUpdtTs) {
        this.stageId       = stageId;
        this.mapId         = mapId;
        this.stageDsJsonTx = stageDsJsonTx;
        this.queryConfigTx = queryConfigTx;
        this.loadDt        = loadDt;
        this.lstUpdtTs     = lstUpdtTs;
    }
}
