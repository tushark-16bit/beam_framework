package com.yourco.beam.model;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Represents one row in the {@code RptDaMap} mapping table.
 *
 * <p>Links a report run ({@code rpt_id} from {@code RptRefer}) to a data-source run
 * ({@code da_id} from {@code DaRefer}). Created by {@code ReportCheckpointAdapter.addDaMapping()}
 * during the alias-registry phase, before staged data is loaded into {@code RptStageDa}.
 *
 * <h2>BQ table schema (RptDaMap)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.RptDaMap (
 *   map_id      INT64    NOT NULL,   -- surrogate PK: MAX(map_id)+1 per row
 *   rpt_id      INT64    NOT NULL,   -- FK → RptRefer.rpt_id
 *   da_id       INT64    NOT NULL,   -- FK → DaRefer.da_id (the datasource run that was used)
 *   lst_updt_ts DATETIME NOT NULL
 * );
 * }</pre>
 */
public final class RptDaMap implements Serializable {

    private static final long serialVersionUID = 1L;

    public final long          mapId;
    public final long          rptId;
    public final long          daId;
    public final LocalDateTime lstUpdtTs;

    public RptDaMap(long mapId, long rptId, long daId, LocalDateTime lstUpdtTs) {
        this.mapId     = mapId;
        this.rptId     = rptId;
        this.daId      = daId;
        this.lstUpdtTs = lstUpdtTs;
    }
}
