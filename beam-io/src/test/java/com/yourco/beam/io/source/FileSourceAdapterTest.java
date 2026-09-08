package com.yourco.beam.io.source;

import com.yourco.beam.model.FileSourceConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSourceAdapterTest {

    private static FileSourceConfig csvConfig(boolean hasHeader, int firstRow, String lastColumn) {
        return new FileSourceConfig("CSV", "gs://bucket/", "f", ".csv", ",",
            hasHeader, 0, firstRow, lastColumn);
    }

    // ── columnLetter / columnIndexFromLetter round-trip ─────────────────────

    @Test
    void columnLetterRoundTripsAtKnownBoundaries() {
        int[] indices = {0, 1, 25, 26, 27, 51, 701, 702};
        for (int i : indices) {
            String letter = FileSourceAdapter.columnLetter(i);
            assertEquals(i, FileSourceAdapter.columnIndexFromLetter(letter),
                "round-trip failed for index " + i + " (letter=" + letter + ")");
        }
    }

    @Test
    void columnLetterMatchesExcelNotationAtBoundaries() {
        assertEquals("A",  FileSourceAdapter.columnLetter(0));
        assertEquals("Z",  FileSourceAdapter.columnLetter(25));
        assertEquals("AA", FileSourceAdapter.columnLetter(26));
        assertEquals("AZ", FileSourceAdapter.columnLetter(51));
        assertEquals("ZZ", FileSourceAdapter.columnLetter(701));
        assertEquals("AAA", FileSourceAdapter.columnLetter(702));
    }

    @Test
    void columnIndexFromLetterRejectsEmptyOrNonLetterInput() {
        assertThrows(IllegalArgumentException.class, () -> FileSourceAdapter.columnIndexFromLetter(""));
        assertThrows(IllegalArgumentException.class, () -> FileSourceAdapter.columnIndexFromLetter("A1"));
    }

    // ── parseCsv: column width ───────────────────────────────────────────────

    @Test
    void dataWiderThanHeaderKeepsAllDataColumns() {
        // 2 header cells, but data rows carry 5 values — none should be dropped.
        String csv = "h1,h2\nv1,v2,v3,v4,v5\n";
        FileSourceAdapter.FileParseResult result =
            FileSourceAdapter.parseCsv(csv.getBytes(StandardCharsets.UTF_8), csvConfig(true, 1, null));

        assertEquals(List.of("{\"A\":\"v1\",\"B\":\"v2\",\"C\":\"v3\",\"D\":\"v4\",\"E\":\"v5\"}"), result.rows());
        assertTrue(result.headerLegendJson().contains("\"A\":\"h1\""));
        assertTrue(result.headerLegendJson().contains("\"E\":null"),
            "letters beyond header width should map to null in the legend, not be omitted");
    }

    @Test
    void firstRowSkipsLeadingJunkAndTreatsNextRowAsHeader() {
        String csv = "junk1\njunk2\nh1,h2,h3\nv1,v2,v3\n";
        FileSourceAdapter.FileParseResult result =
            FileSourceAdapter.parseCsv(csv.getBytes(StandardCharsets.UTF_8), csvConfig(true, 3, null));

        assertEquals(List.of("{\"A\":\"v1\",\"B\":\"v2\",\"C\":\"v3\"}"), result.rows());
        assertTrue(result.headerLegendJson().contains("\"A\":\"h1\""));
    }

    @Test
    void firstRowWithNoHeaderTreatsThatRowAsData() {
        String csv = "skip1\nd1,d2,d3\nd4,d5,d6\n";
        FileSourceAdapter.FileParseResult result =
            FileSourceAdapter.parseCsv(csv.getBytes(StandardCharsets.UTF_8), csvConfig(false, 2, null));

        assertNull(result.headerLegendJson());
        assertEquals(List.of(
            "{\"A\":\"d1\",\"B\":\"d2\",\"C\":\"d3\"}",
            "{\"A\":\"d4\",\"B\":\"d5\",\"C\":\"d6\"}"
        ), result.rows());
    }

    @Test
    void explicitLastColumnTruncatesEvenWhenDataIsWider() {
        String csv = "h1,h2\nv1,v2,v3,v4\n";
        FileSourceAdapter.FileParseResult result =
            FileSourceAdapter.parseCsv(csv.getBytes(StandardCharsets.UTF_8), csvConfig(true, 1, "B"));

        assertEquals(List.of("{\"A\":\"v1\",\"B\":\"v2\"}"), result.rows());
        assertEquals("{\"__header_map__\":{\"A\":\"h1\",\"B\":\"h2\"}}", result.headerLegendJson());
    }

    @Test
    void emptyFileProducesNoRowsAndNoLegend() {
        FileSourceAdapter.FileParseResult result =
            FileSourceAdapter.parseCsv("".getBytes(StandardCharsets.UTF_8), csvConfig(true, 1, null));

        assertTrue(result.rows().isEmpty());
        assertNull(result.headerLegendJson());
    }
}
