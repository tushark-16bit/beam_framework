package com.yourco.beam.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.model.DateParamConfig;
import com.yourco.beam.model.RunDates;
import com.yourco.beam.options.FrameworkOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves placeholder tokens in query templates to their runtime values.
 *
 * <h2>Standard tokens</h2>
 * <ul>
 *   <li>{@code {periodStart}} — from {@code --periodStart} option</li>
 *   <li>{@code {periodEnd}}   — from {@code --periodEnd} option</li>
 *   <li>{@code {periodId}}    — from {@code --periodId} option</li>
 *   <li>{@code {runDate}}     — from {@code --runDate} (or today UTC if unset)</li>
 * </ul>
 *
 * <h2>Percent-delimited equivalents</h2>
 * <p>The same four values are also resolved using {@code %name%} delimiters —
 * {@code %periodStart%}, {@code %periodEnd%}, {@code %periodId%}, {@code %runDate%} — for
 * queries where the curly-brace style collides with something else in the target SQL dialect
 * (or an external system already uses {@code %...%} for its own substitution). Both styles
 * resolve the exact same underlying {@link FrameworkOptions} values; use whichever fits the
 * query. Unlike curly-brace custom params (below), {@code %name%} tokens are a fixed, built-in
 * set — there's no operator-declared mapping for them; the name in {@code %name%} directly names
 * the {@link FrameworkOptions} value to substitute.
 *
 * <h2>Configured date parameters</h2>
 * <p>{@code date_params} from the source's or report's config ({@link DateParamConfig}) are
 * resolved right after the standard tokens, from the same {@link RunDates}: an operator names the
 * placeholder as it appears in the query, which value of the run to use ({@code PERIOD_ID},
 * {@code PERIOD_START}, {@code PERIOD_END}, {@code RUN_DATE}) and a date format such as
 * {@code MMM-yy}. Each can be limited to named queries ({@code apply_to}). Longer tokens are
 * replaced first, so a token that is a prefix of another does not clobber it.
 *
 * <h2>Custom params</h2>
 * Additional {@code paramMappings} (a step's own {@code query_params_json} from
 * {@code parameter_store}) are resolved after the standard tokens. On top of those,
 * {@code --customParamsJson} — a JSON object CLI flag, the run-time equivalent of
 * {@code query_params_json} for a value that should come from the invocation itself rather than
 * be hard-coded into stored config — is merged in, overriding any same-named key from
 * {@code paramMappings}. Param values (from either source) can themselves reference standard
 * tokens — resolution is two-pass (standard first, then custom), so
 * {@code {"startDate": "{periodStart}"}} works correctly.
 *
 * <h2>Example</h2>
 * <pre>{@code
 * String query = "SELECT * FROM trades WHERE trade_date >= '{startDate}' AND exchange = '{exchange}'";
 * Map<String,String> params = Map.of("startDate", "{periodStart}", "exchange", "NYSE");
 * String resolved = QueryParameterResolver.resolve(query, params, options);
 * // → "SELECT * FROM trades WHERE trade_date >= '2024-01-01' AND exchange = 'NYSE'"
 * // --customParamsJson='{"exchange":"NASDAQ"}' on the CLI would override "NYSE" above.
 *
 * // Percent-delimited style, same underlying values, no query_params_json entry needed:
 * String query2 = "SELECT * FROM trades WHERE period_id = %periodId%";
 * String resolved2 = QueryParameterResolver.resolve(query2, options);
 * // → "SELECT * FROM trades WHERE period_id = 202401"
 * }</pre>
 */
public final class QueryParameterResolver {

    private static final Logger LOG = LoggerFactory.getLogger(QueryParameterResolver.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{[A-Za-z_][A-Za-z0-9_]*\\}");

    private QueryParameterResolver() {}

    /**
     * Resolves all placeholder tokens in {@code template} using standard tokens from
     * {@code options}, a step's own {@code paramMappings}, and any {@code --customParamsJson}
     * on {@code options} (which wins on a key collision with {@code paramMappings}).
     *
     * @param template     SQL or other string with {@code {token}} placeholders
     * @param paramMappings  custom param names → values (values may reference standard tokens)
     * @param options      pipeline options providing periodStart, periodEnd, periodId, runDate,
     *                     and customParamsJson
     * @return the fully resolved string; unknown tokens are left unchanged
     */
    public static String resolve(String template, Map<String, String> paramMappings,
                                 FrameworkOptions options) {
        if (template == null || template.isBlank()) return template;
        return resolve(template, paramMappings, options, RunDateCalculator.fromOptions(options));
    }

    /**
     * Same as {@link #resolve(String, Map, FrameworkOptions)}, but standard tokens come from
     * {@code dates} — a source's or report's own {@link RunDates} from
     * {@link RunDateCalculator#evaluateDataSource}/{@link RunDateCalculator#evaluateReport} — rather than straight from the CLI options.
     * {@code options} still supplies {@code --customParamsJson}.
     */
    public static String resolve(String template, Map<String, String> paramMappings,
                                 FrameworkOptions options, RunDates dates) {
        return resolve(template, paramMappings, options, dates, List.of(), null);
    }

    /**
     * As {@link #resolve(String, Map, FrameworkOptions, RunDates)}, additionally resolving the
     * configured {@code dateParams} (those that apply to {@code scope}) after the standard tokens.
     *
     * @param dateParams the source's/report's {@code date_params}; may be empty
     * @param scope      the name of the query being resolved — a report step's {@code step_name},
     *                   or {@link DateParamConfig#SOURCE_QUERY} for a data source's input query
     */
    public static String resolve(String template, Map<String, String> paramMappings,
                                 FrameworkOptions options, RunDates dates,
                                 List<DateParamConfig> dateParams, String scope) {
        if (template == null || template.isBlank()) return template;

        // Pass 1: resolve standard tokens, then the configured date parameters
        String result = resolveDateParams(resolveStandardTokens(template, dates),
                                          dates, dateParams, scope);

        // Pass 2: resolve custom params — step-level query_params_json first, then
        // --customParamsJson on top, so an operator can override any step's configured value
        // at run time without editing parameter_store.
        Map<String, String> merged = new LinkedHashMap<>();
        if (paramMappings != null) merged.putAll(paramMappings);
        merged.putAll(parseCustomParamsJson(options.getCustomParamsJson()));

        for (Map.Entry<String, String> entry : merged.entrySet()) {
            String value = resolveDateParams(resolveStandardTokens(
                (entry.getValue() != null ? entry.getValue() : ""), dates), dates, dateParams, scope);
            result = result.replace("{" + entry.getKey() + "}", value);
        }

        warnOnUnresolved(result, scope);
        return result;
    }

    /**
     * {@code {name}}-style placeholders still in a fully resolved query — usually a token that has
     * no matching {@code date_params} / {@code query_params_json} entry, a misspelt or
     * differently-braced token, or an {@code apply_to} that excludes this query. Unknown tokens are
     * left as written (by design), so BigQuery would otherwise be the first to complain.
     */
    static List<String> unresolvedPlaceholders(String resolved) {
        List<String> found = new java.util.ArrayList<>();
        Matcher m = PLACEHOLDER.matcher(resolved);
        while (m.find()) {
            if (!found.contains(m.group())) found.add(m.group());
        }
        return found;
    }

    private static void warnOnUnresolved(String resolved, String scope) {
        List<String> left = unresolvedPlaceholders(resolved);
        if (!left.isEmpty()) {
            LOG.warn("Query{} still contains unresolved placeholder(s) {} — check the date_params / "
                     + "query_params_json token text (braces included), apply_to, and that string "
                     + "values are quoted in the SQL", scope != null ? " '" + scope + "'" : "", left);
        }
    }

    /** Overload for templates with no custom param mappings. */
    public static String resolve(String template, FrameworkOptions options) {
        return resolve(template, null, options);
    }

    // ── Private ───────────────────────────────────────────────────────────────

    /** Replaces every configured date parameter (longest token first) that applies to {@code scope}. */
    static String resolveDateParams(String s, RunDates dates, List<DateParamConfig> dateParams,
                                    String scope) {
        if (dateParams == null || dateParams.isEmpty()) return s;
        String result = s;
        List<DateParamConfig> ordered = dateParams.stream()
            .filter(p -> p.appliesTo(scope))
            .sorted(Comparator.comparingInt((DateParamConfig p) -> p.token.length()).reversed())
            .toList();
        for (DateParamConfig param : ordered) {
            result = result.replace(param.token, render(param, dates));
        }
        return result;
    }

    /** The value of one configured date parameter for this run (see {@link DateParamConfig}). */
    static String render(DateParamConfig param, RunDates dates) {
        java.time.LocalDate date = switch (param.source) {
            case PERIOD_ID    -> param.format == null ? null : dates.periodStart;  // id → its start date
            case PERIOD_START -> dates.periodStart;
            case PERIOD_END   -> dates.periodEnd;
            case RUN_DATE     -> dates.runDate;
        };
        if (param.source == DateParamConfig.Source.PERIOD_ID && param.format == null) {
            return String.valueOf(dates.periodId);
        }
        if (date == null) {
            return "";   // same convention as RunDates.periodStartIso()/periodEndIso() when unset
        }
        DateTimeFormatter formatter = param.format == null
            ? DateTimeFormatter.ISO_LOCAL_DATE
            : DateTimeFormatter.ofPattern(param.format, Locale.ENGLISH);
        return date.format(formatter);
    }

    private static String resolveStandardTokens(String s, RunDates dates) {
        String periodStart = dates.periodStartIso();
        String periodEnd   = dates.periodEndIso();
        String periodId    = String.valueOf(dates.periodId);
        String runDate     = dates.runDateIso();
        return s
            .replace("{periodStart}", periodStart)
            .replace("{periodEnd}",   periodEnd)
            .replace("{periodId}",    periodId)
            .replace("{runDate}",     runDate)
            .replace("%periodStart%", periodStart)
            .replace("%periodEnd%",   periodEnd)
            .replace("%periodId%",    periodId)
            .replace("%runDate%",     runDate);
    }

    /**
     * Parses {@code --customParamsJson} into a String→String map. Blank/unset returns an empty
     * map. Malformed JSON or a non-object root fails loudly rather than being silently ignored —
     * an operator-supplied CLI value with a typo should fail the run, not resolve to nothing.
     */
    private static Map<String, String> parseCustomParamsJson(String json) {
        if (json == null || json.isBlank()) return Map.of();
        JsonNode root;
        try {
            root = JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "--customParamsJson is not valid JSON: " + json, e);
        }
        if (!root.isObject()) {
            throw new IllegalArgumentException(
                "--customParamsJson must be a JSON object, got: " + json);
        }
        Map<String, String> result = new LinkedHashMap<>();
        root.fields().forEachRemaining(e -> result.put(e.getKey(), e.getValue().asText()));
        return result;
    }
}
