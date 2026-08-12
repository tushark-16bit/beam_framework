package com.yourco.beam.io.util;

/**
 * Constants for the FILE-source column-letter storage convention.
 *
 * <p>FILE (CSV/Excel) source rows are stored in {@code DaRec} keyed by Excel-style column
 * letters ({@code A}, {@code B}, ..., {@code Z}, {@code AA}, ...) rather than by real header
 * names — see {@code FileSourceAdapter}. Every {@code DaRec} page for such a source additionally
 * carries one <em>header-legend</em> object mapping each letter to its real column name, tagged
 * with {@link #MARKER_KEY} so it can be told apart from real data rows.
 *
 * <p>Every reader that unnests {@code row_da_json_tx} into individual rows — row counts, BnC
 * sums, {@code data_transform_query}'s {@code {data}} token, REPORT_PROCESSING staging — must
 * exclude legend objects using {@link #EXCLUDE_LEGEND_SQL_FRAGMENT}. For BQ/API sources, which
 * never produce a legend row, this filter is always true and has no effect.
 */
public final class FileHeaderLegend {

    private FileHeaderLegend() {}

    /** Marker key present (and {@code true}) only on a header-legend object, never on a data row. */
    public static final String MARKER_KEY = "__header_map__";

    /**
     * SQL fragment excluding header-legend objects from an already-unnested {@code row_json}
     * column. Append with {@code AND} after the usual {@code da_id} filter.
     */
    public static final String EXCLUDE_LEGEND_SQL_FRAGMENT =
        "JSON_VALUE(row_json, '$." + MARKER_KEY + "') IS NULL";
}
