package com.yourco.beam.io.records;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** How a BnC {@code dest_column} is matched to the keys of the stored rows (BNC_RULES.md B7). */
class DaRecColumnResolutionTest {

    @Test
    void flatPageMatchesRowKeysCaseInsensitively() {
        String page = "[{\"Trade_Id\":\"1\",\"AMOUNT\":\"5\"},{\"Trade_Id\":\"2\",\"AMOUNT\":\"6\"}]";
        assertEquals("AMOUNT",   BigQueryDataSourceRecordAdapter.resolveColumnKey(page, "amount"));
        assertEquals("Trade_Id", BigQueryDataSourceRecordAdapter.resolveColumnKey(page, "TRADE_ID"));
    }

    @Test
    void filePageMatchesTheHeaderNameThenTheLetter() {
        String page = "{\"Data\":[{\"A\":\"x\",\"B\":\"10\",\"C\":\"12.5\"}],"
                    + "\"DataHeaders\":[{\"A\":\"Name\",\"B\":\"Count\",\"C\":\"TotalRevenueAmt\"}]}";
        assertEquals("C", BigQueryDataSourceRecordAdapter.resolveColumnKey(page, "totalrevenueamt"));
        assertEquals("B", BigQueryDataSourceRecordAdapter.resolveColumnKey(page, "b"));     // by letter
    }

    @Test
    void aHeaderNameWinsOverALetterThatHappensToMatch() {
        // header of column A is literally "B"; asking for "B" means that header, i.e. column A
        String page = "{\"Data\":[{\"A\":\"1\",\"B\":\"2\"}],\"DataHeaders\":[{\"A\":\"B\",\"B\":\"A\"}]}";
        assertEquals("A", BigQueryDataSourceRecordAdapter.resolveColumnKey(page, "B"));
    }

    @Test
    void aMissingColumnIsAnError() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryDataSourceRecordAdapter
            .resolveColumnKey("[{\"a\":\"1\"}]", "nope"));
        assertThrows(IllegalArgumentException.class, () -> BigQueryDataSourceRecordAdapter
            .resolveColumnKey("{\"Data\":[{\"A\":\"1\"}],\"DataHeaders\":[{\"A\":\"x\"}]}", "nope"));
    }

    @Test
    void anEmptyRunKeepsTheConfiguredName() {
        assertEquals("amt", BigQueryDataSourceRecordAdapter.resolveColumnKey(null, "amt"));
        assertEquals("amt", BigQueryDataSourceRecordAdapter.resolveColumnKey("[]", "amt"));
    }

    @Test
    void jsonPathQuotesNamesThatAreNotPlainIdentifiers() {
        assertEquals("$.amount",     BigQueryDataSourceRecordAdapter.jsonPath("amount"));
        assertEquals("$.\"my col\"", BigQueryDataSourceRecordAdapter.jsonPath("my col"));
        assertEquals("$.\"a\\\"b\"", BigQueryDataSourceRecordAdapter.jsonPath("a\"b"));
    }
}
