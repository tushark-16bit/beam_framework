package com.yourco.beam.model;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * One operator-configured query parameter whose value is a date or period of the run, formatted
 * as the query needs it. Stored in the {@code date_params} array of a data source's or a report's
 * {@code parameters_val_json}; resolved into the query text by {@code QueryParameterResolver}
 * (beam-utils) after the standard tokens, from the run's own {@link RunDates}.
 *
 * <pre>{@code
 * "date_params": [
 *   {"token": "{periodLabel}", "source": "PERIOD_END",   "format": "MMM-yy"},
 *   {"token": "%PRD%",         "source": "PERIOD_ID"},
 *   {"token": "{asAt}",        "source": "RUN_DATE",      "format": "dd/MM/yyyy",
 *                              "apply_to": ["aggregate_by_currency"]}
 * ]
 * }</pre>
 *
 * <ul>
 *   <li>{@link #token} — the exact text as it appears in the query; every occurrence is replaced.</li>
 *   <li>{@link #source} — which value of the run to use: {@code PERIOD_ID}, {@code PERIOD_START},
 *       {@code PERIOD_END} or {@code RUN_DATE} (case-insensitive; {@code periodId} /
 *       {@code period_id} also accepted).</li>
 *   <li>{@link #format} — optional {@link DateTimeFormatter} pattern (Java letters are
 *       case-sensitive: month is {@code MM}/{@code MMM}, minutes are {@code mm}; names are
 *       English, so {@code MMM-yy} gives {@code Jan-24}). Absent: dates as {@code yyyy-MM-dd},
 *       {@code PERIOD_ID} as the integer period id. {@code PERIOD_ID} <b>with</b> a format renders
 *       the period's start date in that format (a monthly period 202401 with {@code MMM-yy} →
 *       {@code Jan-24}). A bad pattern fails the config load.</li>
 *   <li>{@link #applyTo} — optional list of the queries to resolve it in: for a report the
 *       {@code step_name} of a preprocessing or transform step; for a data source the input query
 *       is named {@value #SOURCE_QUERY}. Empty/absent = every query of that source or report.</li>
 * </ul>
 */
public final class DateParamConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Scope name of a data source's input query, for {@link #applyTo}. */
    public static final String SOURCE_QUERY = "bq_query";

    public enum Source {
        PERIOD_ID, PERIOD_START, PERIOD_END, RUN_DATE;

        static Source parse(String text) {
            String normalised = text == null ? "" : text.trim().replace("_", "").replace("-", "")
                                                        .toLowerCase(Locale.ROOT);
            return switch (normalised) {
                case "periodid"    -> PERIOD_ID;
                case "periodstart" -> PERIOD_START;
                case "periodend"   -> PERIOD_END;
                case "rundate"     -> RUN_DATE;
                default -> throw new IllegalArgumentException(
                    "unknown date_params source '" + text + "' — expected PERIOD_ID, PERIOD_START, "
                    + "PERIOD_END or RUN_DATE");
            };
        }
    }

    public final String       token;
    public final Source       source;
    /** Null when not configured. */
    public final String       format;
    /** Never null; empty means "every query". */
    public final List<String> applyTo;

    /**
     * @throws IllegalArgumentException on a blank token, an unknown source, or an invalid format
     *         pattern — a broken parameter fails the config load rather than leaving an
     *         unresolved placeholder in a query
     */
    public DateParamConfig(String token, String source, String format, List<String> applyTo) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("date_params entry has no \"token\"");
        }
        this.token  = token;
        this.source = Source.parse(source);
        this.format = (format == null || format.isBlank()) ? null : format;
        if (this.format != null) {
            try {
                DateTimeFormatter.ofPattern(this.format, Locale.ENGLISH);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("date_params token '" + token
                    + "': invalid format '" + format + "' — " + e.getMessage(), e);
            }
        }
        this.applyTo = applyTo == null ? Collections.emptyList()
                                       : Collections.unmodifiableList(new ArrayList<>(applyTo));
    }

    /** True when this parameter is resolved in the query named {@code scope}. */
    public boolean appliesTo(String scope) {
        if (applyTo.isEmpty()) return true;
        for (String name : applyTo) {
            if (name != null && name.equalsIgnoreCase(scope)) return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "DateParamConfig{token=" + token + ", source=" + source + ", format=" + format
            + ", applyTo=" + applyTo + "}";
    }
}
