package com.yourco.beam.utils;

import com.yourco.beam.model.BncConfig;
import com.yourco.beam.model.BncConfig.Type;
import com.yourco.beam.utils.BncEvaluator.Outcome;
import com.yourco.beam.utils.BncEvaluator.Result;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The comparison rules of BNC_RULES.md — sections C, F, K and the regression table L. */
class BncEvaluatorTest {

    private static BigDecimal d(String s) { return new BigDecimal(s); }

    private static Result totals(Type type, String sc, String sa, String dc, String da, String v) {
        return BncEvaluator.compareTotals(type, d(sc), d(sa), d(dc), d(da), d(v));
    }

    // ── Regression table L ────────────────────────────────────────────────────

    @Test
    void row1_queryAmountDifferenceOfVPlusOffsetPasses() {
        // Q1, V=1: 1000.00 vs 998.999 → diff 1.001 <= 1 + 0.001 (inclusive)
        Result r = totals(Type.SOURCE_QUERY, "100", "1000.00", "100", "998.999", "1");
        assertEquals(Outcome.PASS, r.outcome());
        assertTrue(r.text().startsWith("Status: Matched, "), r.text());
    }

    @Test
    void row2_queryAmountJustOverVPlusOffsetFails() {
        Result r = totals(Type.SOURCE_QUERY, "100", "1000", "100", "998.998", "1");   // diff 1.002
        assertEquals(Outcome.FAIL, r.outcome());
        assertTrue(r.text().startsWith("Status: Failed, "), r.text());
    }

    @Test
    void row3_fileHasNoOffset() {
        // F1/F2: abs(diff) <= V exactly. 1.0005 passes for a query source but fails for a file.
        assertEquals(Outcome.PASS, totals(Type.SOURCE_QUERY, "100", "1000", "100", "998.9995", "1").outcome());
        assertEquals(Outcome.FAIL, totals(Type.EXCEL_SHEET,  "100", "1000", "100", "998.9995", "1").outcome());
    }

    @Test
    void row4_countsRoundHalfEvenToZeroDecimals() {
        // Q2, V=0: 100.4 vs 100 → both "100"
        assertEquals(Outcome.PASS, totals(Type.SOURCE_QUERY, "100.4", "0", "100", "0", "0").outcome());
    }

    @Test
    void row5_countMismatchFails() {
        assertEquals(Outcome.FAIL, totals(Type.SOURCE_QUERY, "100", "50", "99", "50", "1").outcome());
        assertEquals(Outcome.FAIL, totals(Type.EXCEL_SHEET,  "100", "50", "99", "50", "1").outcome());
    }

    // ── Section F ─────────────────────────────────────────────────────────────

    @Test
    void amountExactlyVPassesInclusive() {
        assertEquals(Outcome.PASS, totals(Type.EXCEL_SHEET, "1", "101", "1", "100", "1").outcome());
    }

    @Test
    void onlyCountOrOnlyAmountDifferingIsAFail() {
        assertEquals(Outcome.FAIL, totals(Type.SOURCE_QUERY, "10", "100", "11", "100", "0").outcome());
        assertEquals(Outcome.FAIL, totals(Type.SOURCE_QUERY, "10", "100", "10", "105", "1").outcome());
    }

    @Test
    void sourceQueryReturningNothingPassesOnlyAgainstAnEmptyDestination() {
        // src = 0/0 (NULL -> 0): PASS only if the destination is 0/0 too
        assertEquals(Outcome.PASS, totals(Type.SOURCE_QUERY, "0", "0", "0", "0", "0").outcome());
        assertEquals(Outcome.FAIL, totals(Type.SOURCE_QUERY, "0", "0", "5", "10", "0").outcome());
    }

    @Test
    void excelSheetWithAZeroCountIsAFail() {
        Result r = totals(Type.EXCEL_SHEET, "0", "0", "0", "0", "0");
        assertEquals(Outcome.FAIL, r.outcome());
        assertTrue(r.text().contains("valid B&C data"), r.text());
    }

    @Test
    void countOnlyIgnoresTheAmount() {
        Result r = totals(Type.COUNT_ONLY, "100", "0", "100", "999999", "0");
        assertEquals(Outcome.PASS, r.outcome());
        assertTrue(!r.text().contains("Amount"), r.text());
    }

    // ── Count rounding (K2) ───────────────────────────────────────────────────

    @Test
    void halfEvenRounding() {
        assertEquals("2",   BncEvaluator.fmt0(d("2.5")));
        assertEquals("4",   BncEvaluator.fmt0(d("3.5")));
        assertEquals("100", BncEvaluator.fmt0(d("100.5")));
        assertEquals("102", BncEvaluator.fmt0(d("101.5")));
    }

    // ── SQL status ────────────────────────────────────────────────────────────

    @Test
    void sqlStatusPassesWhenTheUpperCasedStatusContainsSuccess() {
        assertEquals(Outcome.PASS, BncEvaluator.evaluateStatus("success", "all good").outcome());
        assertEquals(Outcome.PASS, BncEvaluator.evaluateStatus("Completed with SUCCESS flag", null).outcome());
        assertEquals(Outcome.FAIL, BncEvaluator.evaluateStatus("FAILED", "totals differ").outcome());
        assertEquals(Outcome.FAIL, BncEvaluator.evaluateStatus(null, "x").outcome());
        assertTrue(BncEvaluator.evaluateStatus("SUCCESS", null).text().contains("bnctext column missing"));
    }

    // ── Errors, skips, numbers ────────────────────────────────────────────────

    @Test
    void anErrorIsAFailNeverASkip() {
        Result r = BncEvaluator.error("query failed");
        assertEquals(Outcome.FAIL, r.outcome());
        assertTrue(r.text().contains("Source Count: NA") && r.text().contains("ERROR: query failed"), r.text());
    }

    @Test
    void skippedIsDistinctFromFail() {
        assertTrue(BncEvaluator.skipped("bnc.type NONE").skipped());
    }

    @Test
    void parseDecimalIsStrict() {
        assertEquals(d("12345.67"), BncEvaluator.parseDecimal(" 12345.67 "));
        assertThrows(NumberFormatException.class, () -> BncEvaluator.parseDecimal("1,234"));
        assertThrows(NumberFormatException.class, () -> BncEvaluator.parseDecimal(null));
        assertEquals(BigDecimal.ZERO, BncEvaluator.orZero(null));
    }

    @Test
    void textShowsTheVarianceOnlyWhenOneIsConfigured() {
        assertTrue(totals(Type.SOURCE_QUERY, "1", "1", "1", "1", "1").text()
            .contains("(Amt variance allowed +/- 1)"));
        assertTrue(!totals(Type.SOURCE_QUERY, "1", "1", "1", "1", "0").text().contains("variance"));
    }

    // ── BncConfig parsing (K1, Q2 rule) ───────────────────────────────────────

    @Test
    void errorOnFailureBlankOrTrueIsTrueAnythingElseFalse() {
        assertTrue(new BncConfig("COUNT_ONLY", null, null, null, null).errorOnFailure);
        assertTrue(new BncConfig("COUNT_ONLY", null, null, null, "  ").errorOnFailure);
        assertTrue(new BncConfig("COUNT_ONLY", null, null, null, "TRUE").errorOnFailure);
        assertEquals(false, new BncConfig("COUNT_ONLY", null, null, null, "false").errorOnFailure);
        assertEquals(false, new BncConfig("COUNT_ONLY", null, null, null, "no").errorOnFailure);
    }

    @Test
    void aNonNumericOrNegativeVarianceFailsTheConfigLoad() {
        assertThrows(IllegalArgumentException.class,
            () -> new BncConfig("SOURCE_QUERY", "q", "c", "abc", null));
        assertThrows(IllegalArgumentException.class,
            () -> new BncConfig("SOURCE_QUERY", "q", "c", "-1", null));
        assertEquals(BigDecimal.ZERO, new BncConfig("SOURCE_QUERY", "q", "c", "", null).varianceAllowed);
    }

    @Test
    void anUnknownTypeFailsTheConfigLoad() {
        assertThrows(IllegalArgumentException.class, () -> new BncConfig("MAGIC", null, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new BncConfig(null, null, null, null, null));
        assertEquals(Type.EXCEL_SHEET, new BncConfig("excel sheet", null, null, null, null).type);
    }
}
