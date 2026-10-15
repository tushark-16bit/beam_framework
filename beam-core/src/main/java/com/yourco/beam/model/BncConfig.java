package com.yourco.beam.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Locale;

/**
 * Balance &amp; Control (BnC) check for one data source — the {@code "bnc"} object of its
 * {@code parameters_val_json}. The rules it implements, and every decision made when mapping the
 * legacy framework onto this one, are in {@code BNC_RULES.md} (read it before changing behaviour).
 *
 * <pre>{@code
 * "bnc": {
 *   "type":             "SOURCE_QUERY",
 *   "source_query":     "SELECT COUNT(*) AS c, SUM(amt) AS a FROM `p.d.t` WHERE per = '{periodLabel}'",
 *   "dest_column":      "N_SETTLEMENT_MONTHLY_USD_AMT",
 *   "variance_allowed": 1.0,
 *   "error_on_failure": true
 * }
 * }</pre>
 *
 * <p>Absent object = {@link #none()} = no BnC check. A present but incomplete object is not
 * skipped: the finalize step records it as a failed check (BNC_RULES.md, decision B2).
 *
 * <p>Parsing is strict where the legacy framework was strict: an unknown {@code type} or a
 * non-numeric / negative {@code variance_allowed} throws at construction, so the config load
 * fails loudly before any data moves (legacy Q2 rule).
 */
public final class BncConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Which source-control strategy is used (legacy {@code bncType} + query path). */
    public enum Type {
        /** Query sources: run {@link #sourceQuery}; column 1 = count, column 2 = amount. */
        SOURCE_QUERY,
        /** Excel files: the sheet after the data sheet, cell A2 = count, B2 = amount. */
        EXCEL_SHEET,
        /** Rows read by the pipeline vs rows stored; the amount is ignored. */
        COUNT_ONLY,
        /** A SQL status check: {@link #sourceQuery} returns {@code bncstatus}/{@code bnctext}. */
        SQL_STATUS,
        /** Explicitly no check (skipped for every source kind). */
        NONE;

        public static Type parse(String text) {
            if (text == null || text.isBlank()) {
                throw new IllegalArgumentException(
                    "bnc.type is required — expected SOURCE_QUERY, EXCEL_SHEET, COUNT_ONLY, SQL_STATUS or NONE");
            }
            String key = text.trim().toUpperCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
            try {
                return valueOf(key);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown bnc.type '" + text
                    + "' — expected SOURCE_QUERY, EXCEL_SHEET, COUNT_ONLY, SQL_STATUS or NONE");
            }
        }
    }

    public final Type       type;
    /** Null when not configured. */
    public final String     sourceQuery;
    /** Null when not configured. */
    public final String     destColumn;
    /** Absolute tolerance on the amount; never null, never negative. Default 0. */
    public final BigDecimal varianceAllowed;
    /** Legacy {@code errorOnBnCFailure}: blank/missing = true. Only controls notification. */
    public final boolean    errorOnFailure;

    public BncConfig(String type, String sourceQuery, String destColumn,
                     String varianceAllowed, String errorOnFailure) {
        this(Type.parse(type), sourceQuery, destColumn, parseVariance(varianceAllowed),
             parseFlag(errorOnFailure));
    }

    private BncConfig(Type type, String sourceQuery, String destColumn,
                      BigDecimal varianceAllowed, boolean errorOnFailure) {
        this.type            = type;
        this.sourceQuery     = (sourceQuery != null && !sourceQuery.isBlank()) ? sourceQuery : null;
        this.destColumn      = (destColumn != null && !destColumn.isBlank()) ? destColumn.trim() : null;
        this.varianceAllowed = varianceAllowed;
        this.errorOnFailure  = errorOnFailure;
    }

    /** No BnC check. */
    public static BncConfig none() {
        return new BncConfig(Type.NONE, null, null, BigDecimal.ZERO, true);
    }

    /** True when a check must run (anything but {@link Type#NONE}). */
    public boolean isActive() {
        return type != Type.NONE;
    }

    /** A copy with {@link #sourceQuery} replaced (e.g. by its token-resolved text). */
    public BncConfig withSourceQuery(String resolvedQuery) {
        return new BncConfig(type, resolvedQuery, destColumn, varianceAllowed, errorOnFailure);
    }

    /** Legacy K1: blank/missing = true; anything other than "true" (case-insensitive) = false. */
    static boolean parseFlag(String text) {
        return text == null || text.isBlank() || text.trim().equalsIgnoreCase("true");
    }

    static BigDecimal parseVariance(String text) {
        if (text == null || text.isBlank()) return BigDecimal.ZERO;
        BigDecimal v;
        try {
            v = new BigDecimal(text.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "bnc.variance_allowed '" + text + "' is not a number");
        }
        if (v.signum() < 0) {
            throw new IllegalArgumentException("bnc.variance_allowed must be >= 0 (got " + text + ")");
        }
        return v;
    }

    @Override
    public String toString() {
        return "BncConfig{type=" + type + ", destColumn=" + destColumn
            + ", variance=" + varianceAllowed + ", errorOnFailure=" + errorOnFailure + "}";
    }
}
