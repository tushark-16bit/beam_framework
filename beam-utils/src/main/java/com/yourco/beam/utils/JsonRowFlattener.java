package com.yourco.beam.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Builds the SQL that turns a report's staged data — one JSON object per row, in a single
 * {@code stage_ds_json_tx} column — into an ordinary table: every top-level JSON key becomes a
 * column (the header, once exported) and every value goes in that column.
 *
 * <p>Pure SQL generation, no BigQuery access, so it is unit-testable. {@code ReportPipelineFactory}
 * runs {@link #keysQuery} to learn the keys and then {@link #selectSql} to materialise the table
 * that is exported to CSV/JSON or written to {@code output_bq_table}.
 *
 * <h2>Rules</h2>
 * <ul>
 *   <li><b>Keys</b> are the union of the top-level keys of every staged row, in first-seen order
 *       (a row missing a key gets an empty cell for it).</li>
 *   <li><b>Values</b> are STRINGs: a JSON scalar becomes its text; a nested object or array
 *       becomes its JSON text; JSON {@code null} and a missing key become SQL NULL (empty CSV cell).</li>
 *   <li><b>Column names</b> are the keys with every character outside {@code [A-Za-z0-9_]}
 *       replaced by {@code _}, prefixed with {@code _} if they would start with a digit, and made
 *       unique (case-insensitively, as BigQuery column names are) by a {@code _2}, {@code _3}… suffix.</li>
 * </ul>
 * FILE sources store their rows keyed by Excel-style column letters ({@code A}, {@code B}…), so
 * for those the header is the letter.
 */
public final class JsonRowFlattener {

    /** The column every staged-data subquery exposes (see {@code stagedDataSubquery()}). */
    public static final String JSON_COLUMN = "stage_ds_json_tx";

    private JsonRowFlattener() {}

    /**
     * SQL returning one row per distinct top-level key (single STRING column {@code k}), first-seen
     * order. Uses {@code JSON_KEYS} with depth 1; an unparseable row is skipped rather than
     * failing the whole query.
     *
     * @param stagedSubquery a parenthesised subquery exposing {@link #JSON_COLUMN}
     */
    public static String keysQuery(String stagedSubquery) {
        return "SELECT k FROM ("
            + " SELECT k, MIN(pos) AS first_pos"
            + " FROM (SELECT " + JSON_COLUMN + " AS j FROM " + stagedSubquery + "),"
            + " UNNEST(JSON_KEYS(SAFE.PARSE_JSON(j, wide_number_mode => 'round'), 1)) AS k WITH OFFSET AS pos"
            + " GROUP BY k"
            + ") ORDER BY first_pos, k";
    }

    /**
     * SQL selecting one STRING column per key from {@code stagedSubquery}.
     *
     * @param keys the top-level keys, in the order the columns should appear; must not be empty
     */
    public static String selectSql(String stagedSubquery, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("no JSON keys to build columns from");
        }
        List<String> names = columnNames(keys);
        StringBuilder sql = new StringBuilder("SELECT\n");
        for (int i = 0; i < keys.size(); i++) {
            String path = sqlLiteral("$['" + escapeJsonPathKey(keys.get(i)) + "']");
            sql.append("  COALESCE(JSON_VALUE(j, ").append(path).append("), ")
               .append("NULLIF(JSON_QUERY(j, ").append(path).append("), 'null')) AS `")
               .append(names.get(i)).append('`')
               .append(i < keys.size() - 1 ? ",\n" : "\n");
        }
        return sql.append("FROM (SELECT ").append(JSON_COLUMN).append(" AS j FROM ")
                  .append(stagedSubquery).append(")").toString();
    }

    /** Sanitised, unique column names for {@code keys}, same order. */
    static List<String> columnNames(List<String> keys) {
        List<String> names = new ArrayList<>();
        Set<String> used = new HashSet<>();
        for (String key : keys) {
            String base = key == null ? "" : key.replaceAll("[^A-Za-z0-9_]", "_");
            if (base.isEmpty()) {
                base = "col";
            }
            if (Character.isDigit(base.charAt(0))) {
                base = "_" + base;
            }
            String name = base;
            for (int n = 2; !used.add(name.toLowerCase(Locale.ROOT)); n++) {
                name = base + "_" + n;
            }
            names.add(name);
        }
        return names;
    }

    private static String escapeJsonPathKey(String key) {
        return key.replace("\\", "\\\\").replace("'", "\\'");
    }

    /** A BigQuery single-quoted string literal containing {@code text}. */
    private static String sqlLiteral(String text) {
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
