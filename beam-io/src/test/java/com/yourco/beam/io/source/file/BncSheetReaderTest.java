package com.yourco.beam.io.source.file;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Excel BnC sheet: the sheet after the data sheet, A2 = count, B2 = amount (BNC_RULES.md B6).
 *
 * <p>The workbooks are written as raw OOXML with {@link ZipOutputStream}: POI's own xlsx
 * <em>writer</em> needs a newer commons-compress than this module's classpath carries (the reader —
 * all production code uses — is unaffected).
 */
class BncSheetReaderTest {

    private static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    private static final String NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

    private static void entry(ZipOutputStream zip, String name, String body) throws Exception {
        zip.putNextEntry(new ZipEntry(name));
        zip.write((HEADER + body).getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
    }

    private static String inline(String ref, String text) {
        return "<c r=\"" + ref + "\" t=\"inlineStr\"><is><t>" + text + "</t></is></c>";
    }

    /**
     * A workbook whose sheet 1 is a data sheet and whose sheet 2's row 2 holds {@code row2Cells}
     * (raw {@code <c>} elements); {@code sheets == 1} omits the BnC sheet, {@code withRow2 == false}
     * leaves it with a header row only.
     */
    private static byte[] workbook(String row2Cells, int sheets, boolean withRow2) throws Exception {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             ZipOutputStream zip = new ZipOutputStream(out)) {
            entry(zip, "[Content_Types].xml",
                "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
                + "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
                + "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
                + "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
                + "<Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
                + (sheets > 1 ? "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>" : "")
                + "</Types>");
            entry(zip, "_rels/.rels",
                "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
                + "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>"
                + "</Relationships>");
            entry(zip, "xl/workbook.xml",
                "<workbook xmlns=\"" + NS + "\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><sheets>"
                + "<sheet name=\"Data\" sheetId=\"1\" r:id=\"rId1\"/>"
                + (sheets > 1 ? "<sheet name=\"BnC\" sheetId=\"2\" r:id=\"rId2\"/>" : "")
                + "</sheets></workbook>");
            entry(zip, "xl/_rels/workbook.xml.rels",
                "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
                + "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/>"
                + (sheets > 1 ? "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet2.xml\"/>" : "")
                + "</Relationships>");
            entry(zip, "xl/worksheets/sheet1.xml",
                "<worksheet xmlns=\"" + NS + "\"><sheetData>"
                + "<row r=\"1\">" + inline("A1", "TotalRevenueAmt") + "</row>"
                + "<row r=\"2\"><c r=\"A2\"><v>100.5</v></c></row>"
                + "</sheetData></worksheet>");
            if (sheets > 1) {
                entry(zip, "xl/worksheets/sheet2.xml",
                    "<worksheet xmlns=\"" + NS + "\"><sheetData>"
                    + "<row r=\"1\">" + inline("A1", "Count") + inline("B1", "Amount") + "</row>"
                    + (withRow2 ? "<row r=\"2\">" + row2Cells + "</row>" : "")
                    + "</sheetData></worksheet>");
            }
            zip.finish();
            return out.toByteArray();
        }
    }

    private static byte[] withRow2(String cells) throws Exception { return workbook(cells, 2, true); }

    @Test
    void readsCountAndAFormulaAmount() throws Exception {
        // B2 is =SUM(...) — evaluated, not read as text
        byte[] file = withRow2("<c r=\"A2\"><v>350</v></c>"
            + "<c r=\"B2\"><f>SUM(1000.25,11345.42)</f><v>0</v></c>");
        FileSourceAdapter.BncSheet sheet = FileSourceAdapter.readBncSheet(file, 1);
        assertEquals(0, new BigDecimal("350").compareTo(sheet.count()));
        assertEquals(0, new BigDecimal("12345.67").compareTo(sheet.amount()));
    }

    @Test
    void readsNumbersStoredAsPlainText() throws Exception {
        byte[] file = withRow2(inline("A2", "350") + inline("B2", "12345.67"));
        assertEquals(0, new BigDecimal("12345.67").compareTo(FileSourceAdapter.readBncSheet(file, 1).amount()));
    }

    @Test
    void aTextAmountWithAThousandsSeparatorIsAParseError() throws Exception {
        byte[] file = withRow2("<c r=\"A2\"><v>350</v></c>" + inline("B2", "1,234"));
        assertThrows(FileSourceAdapter.FileSourceException.class, () -> FileSourceAdapter.readBncSheet(file, 1));
    }

    @Test
    void aBlankCellIsAnError() throws Exception {
        byte[] file = withRow2("<c r=\"A2\"><v>350</v></c>");           // B2 missing
        FileSourceAdapter.FileSourceException e = assertThrows(FileSourceAdapter.FileSourceException.class,
            () -> FileSourceAdapter.readBncSheet(file, 1));
        assertTrue(e.getMessage().contains("B2"), e.getMessage());
    }

    @Test
    void aMissingSheetIsAnError() throws Exception {
        byte[] oneSheet = workbook("", 1, false);
        FileSourceAdapter.FileSourceException e = assertThrows(FileSourceAdapter.FileSourceException.class,
            () -> FileSourceAdapter.readBncSheet(oneSheet, 1));
        assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
    }

    @Test
    void aMissingRowTwoIsAnError() throws Exception {
        byte[] noRow2 = workbook("", 2, false);
        FileSourceAdapter.FileSourceException e = assertThrows(FileSourceAdapter.FileSourceException.class,
            () -> FileSourceAdapter.readBncSheet(noRow2, 1));
        assertTrue(e.getMessage().contains("row 2"), e.getMessage());
    }
}
