package com.yourco.beam.model;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Represents one row in the {@code RptStageDa} staging table.
 *
 * <p>Staged rows are copied from {@code DaRec} into this table at the start of a report run
 * (keyed by {@code map_id} from {@code RptDaMap}). Transform SQL reads staged data via
 * {@code SELECT stage_ds_json_tx FROM RptStageDa WHERE map_id = X} subqueries.
 * All rows for a report run are deleted after the transformation chain completes —
 * staged data is purely transient.
 *
 * <h2>BQ table schema (RptStageDa)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.RptStageDa (
 *   stage_id          INT64   NOT NULL,   -- surrogate PK (computed via ROW_NUMBER)
 *   map_id            INT64   NOT NULL,   -- FK → RptDaMap.map_id
 *   stage_ds_json_tx  STRING  NOT NULL,   -- row JSON (copied from DaRec.row_da_json_tx)
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
