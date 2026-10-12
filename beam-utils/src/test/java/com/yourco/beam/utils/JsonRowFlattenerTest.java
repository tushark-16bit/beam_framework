package com.yourco.beam.utils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonRowFlattenerTest {

    private static final String SUB = "(SELECT row_json AS stage_ds_json_tx FROM t WHERE map_id = 7)";

    @Test
    void oneColumnPerKeyInTheGivenOrderReadingFromTheStagedSubquery() {
        String sql = JsonRowFlattener.selectSql(SUB, List.of("trade_id", "currency", "amount"));
        assertTrue(sql.startsWith("SELECT\n"));
        assertTrue(sql.contains("JSON_VALUE(j, '$[\\'trade_id\\']')"), sql);
        assertTrue(sql.contains("AS `trade_id`,\n"), sql);
        assertTrue(sql.indexOf("`trade_id`") < sql.indexOf("`currency`"));
        assertTrue(sql.indexOf("`currency`") < sql.indexOf("`amount`"));
        assertTrue(sql.endsWith("FROM (SELECT stage_ds_json_tx AS j FROM " + SUB + ")"), sql);
    }

    @Test
    void nestedValuesFallBackToJsonTextAndJsonNullBecomesSqlNull() {
        String sql = JsonRowFlattener.selectSql(SUB, List.of("a"));
        assertTrue(sql.contains("NULLIF(JSON_QUERY(j, '$[\\'a\\']'), 'null')"), sql);
    }

    @Test
    void columnNamesAreSanitisedAndUnique() {
        assertEquals(List.of("trade_id", "trade_id_2", "_1st", "col", "a_b_c"),
            JsonRowFlattener.columnNames(List.of("trade_id", "trade-id", "1st", "", "a b!c")));
        // case-insensitive uniqueness, as BigQuery column names are
        assertEquals(List.of("Amount", "amount_2"),
            JsonRowFlattener.columnNames(List.of("Amount", "amount")));
    }

    @Test
    void quotesAndBackslashesInKeysAreEscapedForBothJsonPathAndSql() {
        String sql = JsonRowFlattener.selectSql(SUB, List.of("it's"));
        assertTrue(sql.contains("'$[\\'it\\\\\\'s\\']'"), sql);
        assertTrue(sql.contains("AS `it_s`"), sql);
    }

    @Test
    void keysQueryListsDistinctTopLevelKeysInFirstSeenOrder() {
        String sql = JsonRowFlattener.keysQuery(SUB);
        assertTrue(sql.contains("JSON_KEYS(SAFE.PARSE_JSON(j, wide_number_mode => 'round'), 1)"), sql);
        assertTrue(sql.contains("WITH OFFSET AS pos"), sql);
        assertTrue(sql.endsWith("ORDER BY first_pos, k"), sql);
    }

    @Test
    void noKeysIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> JsonRowFlattener.selectSql(SUB, List.of()));
    }
}
