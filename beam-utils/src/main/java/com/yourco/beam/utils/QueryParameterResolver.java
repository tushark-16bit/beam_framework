package com.yourco.beam.utils;

import com.yourco.beam.options.FrameworkOptions;

import java.time.LocalDate;
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
 * Additional {@code paramMappings} are resolved after the standard tokens.
 * Param values can themselves reference standard tokens — resolution is two-pass
 * (standard first, then custom), so {@code {"startDate": "{periodStart}"}} works correctly.
 *
 * <h2>Example</h2>
 * <pre>{@code
 * String query = "SELECT * FROM trades WHERE trade_date >= '{startDate}' AND exchange = '{exchange}'";
 * Map<String,String> params = Map.of("startDate", "{periodStart}", "exchange", "NYSE");
 * String resolved = QueryParameterResolver.resolve(query, params, options);
 * // → "SELECT * FROM trades WHERE trade_date >= '2024-01-01' AND exchange = 'NYSE'"
 * }</pre>
 */
public final class QueryParameterResolver {

    private QueryParameterResolver() {}

    /**
     * Resolves all placeholder tokens in {@code template} using standard tokens from
     * {@code options} and any additional entries in {@code paramMappings}.
     *
     * @param template     SQL or other string with {@code {token}} placeholders
     * @param paramMappings  custom param names → values (values may reference standard tokens)
     * @param options      pipeline options providing periodStart, periodEnd, periodId, runDate
     * @return the fully resolved string; unknown tokens are left unchanged
     */
    public static String resolve(String template, Map<String, String> paramMappings,
                                 FrameworkOptions options) {
        if (template == null || template.isBlank()) return template;

        // Pass 1: resolve standard tokens
        String result = resolveStandardTokens(template, options);

        // Pass 2: resolve custom param mappings (values may themselves contain standard tokens)
        if (paramMappings != null) {
            for (Map.Entry<String, String> entry : paramMappings.entrySet()) {
                String value = resolveStandardTokens(
                    (entry.getValue() != null ? entry.getValue() : ""), options);
                result = result.replace("{" + entry.getKey() + "}", value);
            }
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
            .replace("{periodId}",    nvl(options.getPeriodId()))
            .replace("{runDate}",     runDate);
    }

    private static String nvl(String s) {
        return s != null ? s : "";
    }
}
