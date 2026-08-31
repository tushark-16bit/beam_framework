package com.yourco.beam.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.options.FrameworkOptions;

import java.util.LinkedHashMap;
import java.util.Map;

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
 * }</pre>
 */
public final class QueryParameterResolver {

    private static final ObjectMapper JSON = new ObjectMapper();

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

        // Pass 1: resolve standard tokens
        String result = resolveStandardTokens(template, options);

        // Pass 2: resolve custom params — step-level query_params_json first, then
        // --customParamsJson on top, so an operator can override any step's configured value
        // at run time without editing parameter_store.
        Map<String, String> merged = new LinkedHashMap<>();
        if (paramMappings != null) merged.putAll(paramMappings);
        merged.putAll(parseCustomParamsJson(options.getCustomParamsJson()));

        for (Map.Entry<String, String> entry : merged.entrySet()) {
            String value = resolveStandardTokens(
                (entry.getValue() != null ? entry.getValue() : ""), options);
            result = result.replace("{" + entry.getKey() + "}", value);
        }

        return result;
    }

    /** Overload for templates with no custom param mappings. */
    public static String resolve(String template, FrameworkOptions options) {
        return resolve(template, null, options);
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private static String resolveStandardTokens(String s, FrameworkOptions options) {
        String runDate = DateUtils.resolveRunDate(options).toString();
        return s
            .replace("{periodStart}", nvl(options.getPeriodStart()))
            .replace("{periodEnd}",   nvl(options.getPeriodEnd()))
            .replace("{periodId}",    String.valueOf(options.getPeriodId()))
            .replace("{runDate}",     runDate);
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

    private static String nvl(String s) {
        return s != null ? s : "";
    }
}
