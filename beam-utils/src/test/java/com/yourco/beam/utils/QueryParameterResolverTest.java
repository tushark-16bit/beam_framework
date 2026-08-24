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
}
