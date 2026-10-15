package com.yourco.beam.utils;

import com.yourco.beam.model.BncConfig;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * The comparison rules of Balance &amp; Control — pure functions, no BigQuery, no Beam. The
 * rules are the legacy ones in {@code BNC_RULES.md} (Part 1, sections C, F and K); this class is
 * what {@code BncEvaluatorTest} pins them down against, including the regression rows.
 *
 * <ul>
 *   <li><b>Count</b> (K2): both counts are rounded to 0 decimals, {@link RoundingMode#HALF_EVEN},
 *       and compared as text.</li>
 *   <li><b>Amount</b>: PASS iff {@code abs(source − destination) ≤ V + offset}, inclusive, where
 *       {@code V} is {@code variance_allowed} and the offset is {@code 0.001} for query sources
 *       ({@link BncConfig.Type#SOURCE_QUERY}, legacy Q1/Q2) and {@code 0} for file sources
 *       ({@link BncConfig.Type#EXCEL_SHEET}, legacy F1/F2). Computed in {@link BigDecimal} so the
 *       inclusive boundary is exact (regression row 1: {@code 1000.00 − 998.999 = 1.001 ≤ 1.001}).</li>
 *   <li><b>Excel sheet</b>: a source count of 0 is a FAIL ("do not contain valid B&amp;C data").</li>
 *   <li><b>SQL status</b>: PASS iff {@code upper(bncstatus)} contains {@code "SUCCESS"}; no variance.</li>
 *   <li><b>Count only</b>: the amount is ignored.</li>
 * </ul>
 *
 * <p>Every method returns a {@link Result}; none throws for a failed check — a failed check is a
 * normal outcome, recorded as text (legacy format: {@code Status: Matched, Source Count: x, …}).
 */
public final class BncEvaluator {

    private BncEvaluator() {}

    /** Legacy {@code B_C_VARIANCE_ZERO_OFFSET}: added to the variance for query sources only. */
    public static final BigDecimal VARIANCE_ZERO_OFFSET = new BigDecimal("0.001");

    /** Legacy SQL-mode PASS marker. */
    public static final String SQL_PASS_MARKER = "SUCCESS";

    public enum Outcome { PASS, FAIL, SKIP }

    /**
     * @param outcome PASS / FAIL / SKIP (there is no separate ERROR: an error is a FAIL whose
     *                text says so — BNC_RULES.md, Part 1 section F)
     * @param text    the result sentence, persisted as {@code text} in the DaRefer summary
     * @param figures the four compared numbers (as text), empty when nothing was compared
     */
    public record Result(Outcome outcome, String text, Map<String, Object> figures) {
        public boolean passed()  { return outcome == Outcome.PASS; }
        public boolean failed()  { return outcome == Outcome.FAIL; }
        public boolean skipped() { return outcome == Outcome.SKIP; }
    }

    // ── Totals: count (+ amount) ──────────────────────────────────────────────

    /**
     * Compares the source control values with the stored values.
     *
     * @param type     {@code SOURCE_QUERY} or {@code EXCEL_SHEET} (amount compared) or
     *                 {@code COUNT_ONLY} (amount ignored)
     * @param srcCount source count (may be fractional — it is rounded)
     * @param srcAmount source amount; ignored for {@code COUNT_ONLY}
     * @param dstCount rows stored
     * @param dstAmount sum of the destination column; ignored for {@code COUNT_ONLY}
     * @param variance absolute tolerance on the amount ({@code variance_allowed})
     */
    public static Result compareTotals(BncConfig.Type type,
                                       BigDecimal srcCount, BigDecimal srcAmount,
                                       BigDecimal dstCount, BigDecimal dstAmount,
                                       BigDecimal variance) {
        if (type == BncConfig.Type.EXCEL_SHEET && srcCount.signum() == 0) {
            return failure("source control sheet does not contain valid B&C data (count A2 is 0)",
                           srcCount, srcAmount, dstCount, dstAmount, variance);
        }
        boolean countOk = fmt0(srcCount).equals(fmt0(dstCount));
        boolean amountOk = true;
        if (type != BncConfig.Type.COUNT_ONLY) {
            BigDecimal offset = type == BncConfig.Type.SOURCE_QUERY ? VARIANCE_ZERO_OFFSET
                                                                    : BigDecimal.ZERO;
            amountOk = srcAmount.subtract(dstAmount).abs().compareTo(variance.add(offset)) <= 0;
        }
        boolean pass = countOk && amountOk;
        String text = (pass ? "Status: Matched, " : "Status: Failed, ")
            + figuresText(srcCount, srcAmount, dstCount, dstAmount, variance, type != BncConfig.Type.COUNT_ONLY);
        return new Result(pass ? Outcome.PASS : Outcome.FAIL, text,
                          figures(srcCount, srcAmount, dstCount, dstAmount));
    }

    // ── SQL status ────────────────────────────────────────────────────────────

    /**
     * {@code SQL Query Totals}: the author's SQL decides. PASS iff {@code upper(bncstatus)}
     * contains {@code "SUCCESS"}; a missing status is a FAIL. The framework applies no variance.
     */
    public static Result evaluateStatus(String bncStatus, String bncText) {
        String text = bncText != null ? bncText : "bnctext column missing in SQL output";
        boolean pass = bncStatus != null
            && bncStatus.toUpperCase(Locale.ROOT).contains(SQL_PASS_MARKER);
        return new Result(pass ? Outcome.PASS : Outcome.FAIL,
                          (pass ? "Status: Matched, " : "Status: Failed, ") + text,
                          Map.of());
    }

    // ── Non-comparisons ───────────────────────────────────────────────────────

    /** A check that could not be evaluated (query failed, sheet unreadable…) — a FAIL, never a SKIP. */
    public static Result error(String message) {
        return new Result(Outcome.FAIL,
            "Status: Failed, Source Count: NA, Source Amount: NA, Destination Count: NA, "
            + "Destination Amount: NA (ERROR: " + message + ")",
            Map.of("error", message));
    }

    /** Nothing to check (legacy SKIP; status stays COMPLETED). */
    public static Result skipped(String reason) {
        return new Result(Outcome.SKIP, reason, Map.of());
    }

    // ── Number helpers ────────────────────────────────────────────────────────

    /** Legacy count rule: 0 fraction digits, HALF_EVEN, compared as text. */
    public static String fmt0(BigDecimal value) {
        return value.setScale(0, RoundingMode.HALF_EVEN).toPlainString();
    }

    /** {@code null} (SQL NULL / empty cell) counts as 0, as in the legacy framework. */
    public static BigDecimal orZero(BigDecimal value) {
        return value != null ? value : BigDecimal.ZERO;
    }

    /**
     * Parses a number from a cell or result text. Strict: {@code "1,234"} is a parse error (legacy
     * regression: "B2 = \"1,234\" (parse error)").
     *
     * @throws NumberFormatException when the text is not a plain decimal number
     */
    public static BigDecimal parseDecimal(String text) {
        if (text == null) throw new NumberFormatException("empty value");
        return new BigDecimal(text.trim());
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private static Result failure(String reason, BigDecimal srcCount, BigDecimal srcAmount,
                                  BigDecimal dstCount, BigDecimal dstAmount, BigDecimal variance) {
        return new Result(Outcome.FAIL,
            "Status: Failed, " + figuresText(srcCount, srcAmount, dstCount, dstAmount, variance, true)
            + " (" + reason + ")",
            figures(srcCount, srcAmount, dstCount, dstAmount));
    }

    private static String figuresText(BigDecimal srcCount, BigDecimal srcAmount,
                                      BigDecimal dstCount, BigDecimal dstAmount,
                                      BigDecimal variance, boolean withAmounts) {
        StringBuilder sb = new StringBuilder("Source Count: ").append(fmt0(srcCount));
        if (withAmounts) {
            sb.append(", Source Amount: ").append(srcAmount.toPlainString());
        }
        sb.append(", Destination Count: ").append(fmt0(dstCount));
        if (withAmounts) {
            sb.append(", Destination Amount: ").append(dstAmount.toPlainString());
            if (variance.signum() > 0) {
                sb.append(" (Amt variance allowed +/- ").append(variance.toPlainString()).append(")");
            }
        }
        return sb.toString();
    }

    private static Map<String, Object> figures(BigDecimal srcCount, BigDecimal srcAmount,
                                               BigDecimal dstCount, BigDecimal dstAmount) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sourceCount",       fmt0(srcCount));
        m.put("sourceAmount",      srcAmount.toPlainString());
        m.put("destinationCount",  fmt0(dstCount));
        m.put("destinationAmount", dstAmount.toPlainString());
        return m;
    }
}
