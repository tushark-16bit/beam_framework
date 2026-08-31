package com.yourco.beam.io.util;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileHeaderLegendTest {

    @Test
    void wrapAndUnwrapAreInverses() {
        String content = "{\"A\":\"trade_id\",\"B\":\"amount\"}";
        String wrapped = FileHeaderLegend.wrapLegend(content);

        assertEquals("{\"__header_map__\":{\"A\":\"trade_id\",\"B\":\"amount\"}}", wrapped);
        assertTrue(FileHeaderLegend.isMarkerWrapped(wrapped));
        assertEquals(content, FileHeaderLegend.unwrapLegend(wrapped));
    }

    @Test
    void plainDataRowIsNotMarkerWrapped() {
        assertFalse(FileHeaderLegend.isMarkerWrapped("{\"A\":\"T1001\",\"B\":\"500.00\"}"));
    }

    @Test
    void buildPageWrapsRowsAndLegendUnderDataAndDataHeaders() {
        String page = FileHeaderLegend.buildPage(
            List.of("{\"A\":\"T1001\",\"B\":\"500.00\"}", "{\"A\":\"T1002\",\"B\":\"300.00\"}"),
            "{\"A\":\"trade_id\",\"B\":\"amount\"}");

        assertEquals(
            "{\"Data\":[{\"A\":\"T1001\",\"B\":\"500.00\"},{\"A\":\"T1002\",\"B\":\"300.00\"}],"
            + "\"DataHeaders\":[{\"A\":\"trade_id\",\"B\":\"amount\"}]}",
            page);
    }

    @Test
    void dataArrayExprMentionsBothShapesAndTheGivenColumn() {
        String expr = FileHeaderLegend.dataArrayExpr("row_da_json_tx");

        assertEquals(
            "IFNULL(JSON_EXTRACT_ARRAY(JSON_EXTRACT(row_da_json_tx, '$.Data')), "
            + "JSON_EXTRACT_ARRAY(row_da_json_tx))",
            expr);
    }
}
