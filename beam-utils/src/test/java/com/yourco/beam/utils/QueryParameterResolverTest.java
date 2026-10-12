package com.yourco.beam.utils;

import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryParameterResolverTest {

    private static FrameworkOptions options(String periodStart, String periodEnd, int periodId,
                                            String customParamsJson) {
        FrameworkOptions options = PipelineOptionsFactory.as(FrameworkOptions.class);
        options.setPeriodStart(periodStart);
        options.setPeriodEnd(periodEnd);
        options.setPeriodId(periodId);
        options.setCustomParamsJson(customParamsJson);
        return options;
    }

    @Test
    void resolvesStandardTokens() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401, null);
        String resolved = QueryParameterResolver.resolve(
            "WHERE trade_date BETWEEN '{periodStart}' AND '{periodEnd}' AND period={periodId}",
            options);

        assertEquals("WHERE trade_date BETWEEN '2024-01-01' AND '2024-01-31' AND period=202401",
            resolved);
    }

    @Test
    void resolvesStepLevelParamMappingsUnaffectedByCustomParamsJson() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401, null);
        String resolved = QueryParameterResolver.resolve(
            "exchange = '{exchange}'", Map.of("exchange", "NYSE"), options);

        assertEquals("exchange = 'NYSE'", resolved);
    }

    @Test
    void customParamsJsonAloneIsResolved() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401,
            "{\"exchange\":\"NASDAQ\",\"threshold\":\"10000\"}");
        String resolved = QueryParameterResolver.resolve(
            "exchange = '{exchange}' AND amount > {threshold}", options);

        assertEquals("exchange = 'NASDAQ' AND amount > 10000", resolved);
    }

    @Test
    void customParamsJsonOverridesSameKeyFromStepLevelParamMappings() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401,
            "{\"exchange\":\"NASDAQ\"}");
        String resolved = QueryParameterResolver.resolve(
            "exchange = '{exchange}'", Map.of("exchange", "NYSE"), options);

        assertEquals("exchange = 'NASDAQ'", resolved);
    }

    @Test
    void customParamsJsonValueMayReferenceAStandardToken() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401,
            "{\"startDate\":\"{periodStart}\"}");
        String resolved = QueryParameterResolver.resolve(
            "trade_date >= '{startDate}'", options);

        assertEquals("trade_date >= '2024-01-01'", resolved);
    }

    @Test
    void blankOrUnsetCustomParamsJsonIsANoOp() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401, "");
        String resolved = QueryParameterResolver.resolve("literal text, no tokens", options);

        assertEquals("literal text, no tokens", resolved);
    }

    @Test
    void malformedCustomParamsJsonThrows() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401, "{not valid json");
        assertThrows(IllegalArgumentException.class,
            () -> QueryParameterResolver.resolve("SELECT 1", options));
    }

    @Test
    void nonObjectCustomParamsJsonThrows() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401, "[1,2,3]");
        assertThrows(IllegalArgumentException.class,
            () -> QueryParameterResolver.resolve("SELECT 1", options));
    }

    @Test
    void unknownTokensAreLeftUnchanged() {
        FrameworkOptions options = options("2024-01-01", "2024-01-31", 202401, null);
        String resolved = QueryParameterResolver.resolve("{notAKnownToken}", options);

        assertEquals("{notAKnownToken}", resolved);
    }

    // ── configured date parameters (date_params) ──────────────────────────────

    private static final com.yourco.beam.model.RunDates DATES = new com.yourco.beam.model.RunDates(
        java.time.LocalDate.of(2024, 2, 6), java.time.LocalDate.of(2024, 1, 1),
        java.time.LocalDate.of(2024, 1, 31), 202401);

    private static com.yourco.beam.model.DateParamConfig param(String token, String source,
                                                               String format, String... applyTo) {
        return new com.yourco.beam.model.DateParamConfig(token, source, format, java.util.List.of(applyTo));
    }

    private static String resolveWith(String sql, com.yourco.beam.model.DateParamConfig... params) {
        return QueryParameterResolver.resolve(sql, null, options("2024-01-01", "2024-01-31", 202401, null),
            DATES, java.util.List.of(params), "step1");
    }

    @Test
    void datePeriodsAreFormattedWithTheConfiguredPattern() {
        assertEquals("Jan-24 / 31/01/2024 / 2024-02-06",
            resolveWith("{a} / {b} / {c}",
                param("{a}", "PERIOD_START", "MMM-yy"),
                param("{b}", "period_end", "dd/MM/yyyy"),
                param("{c}", "runDate", null)));
    }

    @Test
    void periodIdIsTheIntegerUnlessAFormatTurnsItIntoItsStartDate() {
        assertEquals("202401 | Jan-24",
            resolveWith("%PRD% | %LABEL%",
                param("%PRD%", "PERIOD_ID", null),
                param("%LABEL%", "PERIOD_ID", "MMM-yy")));
    }

    @Test
    void aParameterCanBeLimitedToNamedQueries() {
        String sql = "x = {a}";
        assertEquals("x = Jan-24", resolveWith(sql, param("{a}", "PERIOD_START", "MMM-yy", "step1")));
        assertEquals("x = {a}",    resolveWith(sql, param("{a}", "PERIOD_START", "MMM-yy", "other_step")));
        assertEquals("x = Jan-24", resolveWith(sql, param("{a}", "PERIOD_START", "MMM-yy", "STEP1")));
    }

    @Test
    void longerTokensAreReplacedFirstSoAPrefixDoesNotClobberThem() {
        assertEquals("2024-01-31 vs 2024-01-01",
            resolveWith("{p_end} vs {p}",
                param("{p}", "PERIOD_START", null),
                param("{p_end}", "PERIOD_END", null)));
    }

    @Test
    void customParamValuesMayReferenceConfiguredDateParameters() {
        String resolved = QueryParameterResolver.resolve("label = '{label}'", Map.of("label", "{mon}"),
            options("2024-01-01", "2024-01-31", 202401, null), DATES,
            java.util.List.of(param("{mon}", "PERIOD_END", "MMM-yy")), "step1");
        assertEquals("label = 'Jan-24'", resolved);
    }

    @Test
    void badConfigurationFailsLoudly() {
        assertThrows(IllegalArgumentException.class, () -> param("", "PERIOD_END", null));
        assertThrows(IllegalArgumentException.class, () -> param("{a}", "TODAY", null));
        assertThrows(IllegalArgumentException.class, () -> param("{a}", "PERIOD_END", "MMM-yy-bb"));
    }

    @Test
    void unresolvedPlaceholdersAreListedOnceEach() {
        assertEquals(java.util.List.of("{periodLabel}", "{other}"),
            QueryParameterResolver.unresolvedPlaceholders(
                "WHERE a = {periodLabel} AND b = {periodLabel} AND c = '{other}' AND d = 202401"));
        assertEquals(java.util.List.of(),
            QueryParameterResolver.unresolvedPlaceholders("WHERE a = 'Jan-24' AND j = JSON '{\"k\": 1}'"));
    }
}
