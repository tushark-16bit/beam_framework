package com.yourco.beam.io.util;

import java.util.List;

/**
 * Helpers for the FILE-source column-letter storage convention.
 *
 * <p>FILE (CSV/Excel) source rows are stored in {@code DaRec} keyed by Excel-style column
 * letters ({@code A}, {@code B}, ..., {@code Z}, {@code AA}, ...) rather than by real header
 * names — see {@code FileSourceAdapter}. When the file has a header row, every DaRec page for
 * that source is written as a single JSON object instead of a flat array:
 * {@code {"Data":[{"A":"T1001",...},...],"DataHeaders":[{"A":"trade_id",...}]}} — {@code Data}
 * holds the page's rows exactly as before, {@code DataHeaders} holds one object mapping each
 * letter to its real header name. Headerless FILE pages, and all BQ/API pages (which never have
 * letter-keyed columns or a legend), keep the original flat {@code [{...},{...},...]} shape.
 *
 * <p>{@link #dataArrayExpr} extracts the row array from a page's JSON column regardless of
 * which of the two shapes it uses, so every page reader (DaRec row counts, BnC sums, the
 * {@code data} CTE always prepended to {@code data_transform_query}, and — since
 * {@code RptStageDa} copies {@code DaRec}'s pages verbatim rather than un-nesting them at write
 * time — {@code ReportCheckpointAdapter.stagedDataSubquery()} reading {@code RptStageDa} back out
 * for report SQL) can use one SQL pattern uniformly — no per-source-type branching, and no
 * explicit legend exclusion, since {@code DataHeaders} is structurally separate from {@code Data}
 * and never gets unnested with it.
 */
public final class FileHeaderLegend {

    private FileHeaderLegend() {}

    /**
     * Wraps a header-legend content object so it can be told apart from a real data row while
     * both travel together through the same {@code PCollection<Row>} (single {@code raw_json}
     * STRING field — see {@code FileSourceTransform}), before {@code DataSourceRecordSinkTransform}
     * pulls it out and moves its content into a page's {@code DataHeaders} array.
     */
    private static final String WRAPPER_PREFIX = "{\"__header_map__\":";
    private static final String WRAPPER_SUFFIX = "}";

    /** Wraps a legend content object, e.g. {@code {"A":"trade_id",...}}, for Row-level transit. */
    public static String wrapLegend(String legendContentJson) {
        return WRAPPER_PREFIX + legendContentJson + WRAPPER_SUFFIX;
    }

    /** True if {@code json} is a marker-wrapped header-legend payload, not a real data row. */
    public static boolean isMarkerWrapped(String json) {
        return json.startsWith(WRAPPER_PREFIX);
    }

    /** Unwraps a marker-wrapped legend payload back to its plain content object. */
    public static String unwrapLegend(String markerWrappedJson) {
        return markerWrappedJson.substring(
            WRAPPER_PREFIX.length(), markerWrappedJson.length() - WRAPPER_SUFFIX.length());
    }

    /**
     * Builds a FILE-sourced DaRec page with a header legend:
     * {@code {"Data":[rows...],"DataHeaders":[legendContentJson]}}.
     */
    public static String buildPage(List<String> pageRowsJson, String legendContentJson) {
        return "{\"Data\":[" + String.join(",", pageRowsJson) + "],"
             + "\"DataHeaders\":[" + legendContentJson + "]}";
    }

    /**
     * SQL expression extracting the array of individual source rows from a DaRec JSON column,
     * regardless of shape: {@code {"Data":[...],"DataHeaders":[...]}} (FILE with header) or a
     * flat {@code [...]} array (everything else). {@code JSON_EXTRACT(..., '$.Data')} returns
     * NULL when the root is already an array, so {@code IFNULL} falls back to extracting it
     * directly — one expression covers both shapes without needing to know the source type.
     */
    public static String dataArrayExpr(String jsonColumnExpr) {
        return "IFNULL(JSON_EXTRACT_ARRAY(JSON_EXTRACT(" + jsonColumnExpr + ", '$.Data')), "
             + "JSON_EXTRACT_ARRAY(" + jsonColumnExpr + "))";
    }
}
