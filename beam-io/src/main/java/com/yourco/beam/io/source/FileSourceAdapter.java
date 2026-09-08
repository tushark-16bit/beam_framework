package com.yourco.beam.io.source;

import com.yourco.beam.io.util.FileHeaderLegend;
import com.yourco.beam.model.FileSourceConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapter for reading CSV and Excel files.
 *
 * <p>This class contains all file-parsing logic separated from Beam lifecycle concerns.
 * It can be unit-tested with raw byte arrays without a running pipeline.
 * {@link FileSourceTransform} uses it inside a DoFn with {@code @Setup}/{@code @Teardown}.
 *
 * <h2>Output format — column letters, not header names</h2>
 * Each data row is returned as a JSON string keyed by Excel-style column letters ({@code A},
 * {@code B}, ..., {@code Z}, {@code AA}, {@code AB}, ...) in file column order — see
 * {@link #columnLetter}. This is deliberately independent of the file's actual header content
 * (or absence of one), so downstream storage never depends on header text being stable,
 * unique, or even present.
 *
 * <p>When the file has a header row ({@link FileSourceConfig#hasHeader}), the real header
 * names are instead captured separately as {@link FileParseResult#headerLegendJson()} — a
 * single JSON object mapping each column letter to its real header name, wrapped via
 * {@link FileHeaderLegend#wrapLegend} for transit through the same {@code PCollection<Row>} as
 * the data rows. {@code DataSourceRecordSinkTransform} unwraps it and places it in each DaRec
 * page's {@code DataHeaders} array (see {@link FileHeaderLegend} for the page shape). When there
 * is no header row, no legend is produced ({@code headerLegendJson()} is {@code null}) — there
 * is no real name to record.
 *
 * <p>Downstream transforms parse each data row's JSON from the standard
 * {@link com.yourco.beam.model.Schemas#RAW_JSON} wire type, same as before.
 *
 * <h2>Where reading starts, and column width</h2>
 * {@link FileSourceConfig#firstRow} (1-based, default 1) skips any leading rows before real
 * content starts; the row landing there becomes the header row or the first data row depending
 * on {@link FileSourceConfig#hasHeader}. Column width — how many letters get generated, and how
 * wide every row is padded/truncated to — is {@link FileSourceConfig#lastColumn} when set
 * (via {@link #columnIndexFromLetter}), otherwise the widest row seen across the header and all
 * data rows, so a data row wider than the header never loses columns just for lacking a header
 * name.
 *
 * <h2>Path resolution</h2>
 * {@link #resolvePath} substitutes placeholders in prefix/suffix:
 * <ul>
 *   <li>{@code {date}}        → {@code yyyy-MM-dd}</li>
 *   <li>{@code {dateCompact}} → {@code yyyyMMdd}</li>
 *   <li>{@code {periodId}}    → value of the periodId option</li>
 * </ul>
 */
public final class FileSourceAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(FileSourceAdapter.class);
    private static final DateTimeFormatter ISO   = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter COMPACT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private FileSourceAdapter() {}

    /**
     * Result of parsing one file: the data rows (letter-keyed JSON), plus the optional
     * letter-to-real-header-name legend (null when the file has no header row).
     */
    public record FileParseResult(List<String> rows, String headerLegendJson) {}

    /**
     * Parses a CSV file from raw bytes.
     *
     * <p>Rows before {@link FileSourceConfig#firstRow} are skipped entirely. The column width
     * used for both the header legend and every data row is {@link FileSourceConfig#lastColumn}
     * when set, otherwise the widest row seen (header or data row) — so a data row with more
     * columns than the header is never truncated just because the header is narrower.
     *
     * @param fileBytes raw file content
     * @param config    file configuration (delimiter, hasHeader, firstRow, lastColumn, etc.)
     * @return data rows keyed by column letter, plus the header legend if the file has headers
     */
    public static FileParseResult parseCsv(byte[] fileBytes, FileSourceConfig config) {
        String content = new String(fileBytes);
        CSVFormat format = CSVFormat.DEFAULT
            .builder()
            .setDelimiter(config.delimiter.charAt(0))
            .setSkipHeaderRecord(false)
            .build();

        try (CSVParser parser = format.parse(new StringReader(content))) {
            List<CSVRecord> allRecords = parser.getRecords();
            int skip = Math.min(config.firstRow - 1, allRecords.size());
            List<CSVRecord> records = allRecords.subList(skip, allRecords.size());
            if (records.isEmpty()) return new FileParseResult(List.of(), null);

            int dataStartIndex = config.hasHeader ? 1 : 0;
            int headerWidth    = config.hasHeader ? records.get(0).size() : 0;

            int maxDataWidth = 0;
            for (int i = dataStartIndex; i < records.size(); i++) {
                maxDataWidth = Math.max(maxDataWidth, records.get(i).size());
            }

            int columnCount = resolveColumnCount(config, headerWidth, maxDataWidth);
            List<String> letters = columnLetters(columnCount);

            String legendJson = config.hasHeader
                ? headerLegendJson(letters, records.get(0).toList()) : null;

            List<String> jsonRows = new ArrayList<>(records.size() - dataStartIndex);
            for (int i = dataStartIndex; i < records.size(); i++) {
                jsonRows.add(rowToJson(letters, records.get(i).toList()));
            }
            LOG.info("Parsed {} CSV rows ({} header, first_row={}, columns={})",
                     jsonRows.size(), config.hasHeader ? "with" : "without",
                     config.firstRow, columnCount);
            return new FileParseResult(jsonRows, legendJson);
        } catch (IOException e) {
            throw new FileSourceException("Failed to parse CSV", e);
        }
    }

    /**
     * Parses an Excel (.xlsx) file from raw bytes.
     *
     * <p>Rows before {@link FileSourceConfig#firstRow} are skipped entirely. The column width
     * used for both the header legend and every data row is {@link FileSourceConfig#lastColumn}
     * when set, otherwise the widest row seen (header or data row) — so a data row with more
     * columns than the header is never truncated just because the header is narrower.
     *
     * @param fileBytes raw XLSX file content
     * @param config    file configuration (sheetIndex, hasHeader, firstRow, lastColumn)
     * @return data rows keyed by column letter, plus the header legend if the file has headers
     */
    public static FileParseResult parseExcel(byte[] fileBytes, FileSourceConfig config) {
        try (Workbook workbook = new XSSFWorkbook(new ByteArrayInputStream(fileBytes))) {
            Sheet sheet = workbook.getSheetAt(config.sheetIndex);
            if (sheet.getPhysicalNumberOfRows() == 0) return new FileParseResult(List.of(), null);

            int firstRowIndex = config.firstRow - 1; // 0-based
            int lastRowIndex  = sheet.getLastRowNum(); // 0-based, inclusive
            if (firstRowIndex > lastRowIndex) return new FileParseResult(List.of(), null);

            int headerRowIdx = config.hasHeader ? firstRowIndex : -1;
            int dataStartIdx = config.hasHeader ? firstRowIndex + 1 : firstRowIndex;

            int headerWidth = 0;
            if (headerRowIdx >= 0) {
                Row headerRow = sheet.getRow(headerRowIdx);
                headerWidth = headerRow != null ? headerRow.getLastCellNum() : 0;
            }

            int maxDataWidth = 0;
            for (int r = dataStartIdx; r <= lastRowIndex; r++) {
                Row row = sheet.getRow(r);
                if (row != null) maxDataWidth = Math.max(maxDataWidth, row.getLastCellNum());
            }

            int columnCount = resolveColumnCount(config, headerWidth, maxDataWidth);
            List<String> letters = columnLetters(columnCount);

            String legendJson = null;
            if (headerRowIdx >= 0) {
                Row headerRow = sheet.getRow(headerRowIdx);
                legendJson = headerRow != null
                    ? headerLegendJson(letters, excelRowToStrings(headerRow)) : null;
            }

            List<String> jsonRows = new ArrayList<>();
            for (int r = dataStartIdx; r <= lastRowIndex; r++) {
                Row row = sheet.getRow(r);
                if (row == null) continue;
                jsonRows.add(rowToJson(letters, excelRowToStrings(row)));
            }
            LOG.info("Parsed {} Excel rows from sheet {} ({}, first_row={}, columns={})",
                     jsonRows.size(), config.sheetIndex, config.hasHeader ? "with header" : "no header",
                     config.firstRow, columnCount);
            return new FileParseResult(jsonRows, legendJson);
        } catch (IOException e) {
            throw new FileSourceException("Failed to parse Excel file", e);
        }
    }

    /**
     * Resolves the full GCS path by substituting placeholders in prefix and suffix.
     *
     * @param config   file source configuration
     * @param periodId value of the {@code --periodId} pipeline option
     * @param runDate  the effective run date
     * @return fully resolved GCS path, e.g. {@code gs://bucket/raw/trades_2024-01-15.csv}
     */
    public static String resolvePath(FileSourceConfig config, String periodId, LocalDate runDate) {
        String date        = runDate.format(ISO);
        String dateCompact = runDate.format(COMPACT);

        String resolvedPrefix = substitute(config.prefix, date, dateCompact, periodId);
        String resolvedSuffix = substitute(config.suffix, date, dateCompact, periodId);

        String location = config.location.endsWith("/") ? config.location : config.location + "/";
        String path = location + resolvedPrefix + resolvedSuffix;
        LOG.debug("Resolved file path: {}", path);
        return path;
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private static String substitute(String template, String date, String dateCompact, String periodId) {
        if (template == null) return "";
        return template
            .replace("{date}",        date)
            .replace("{dateCompact}", dateCompact)
            .replace("{periodId}",    periodId != null ? periodId : "");
    }

    /**
     * Returns the Excel-style column letters (A, B, ..., Z, AA, AB, ...) for {@code count}
     * columns, in file column order.
     */
    private static List<String> columnLetters(int count) {
        List<String> letters = new ArrayList<>(count);
        for (int i = 0; i < count; i++) letters.add(columnLetter(i));
        return letters;
    }

    /**
     * Converts a 0-based column index to its Excel column letter using the same bijective
     * base-26 numbering Excel itself uses: 0→A, 1→B, ..., 25→Z, 26→AA, 27→AB, ...
     */
    static String columnLetter(int index) {
        StringBuilder sb = new StringBuilder();
        int n = index + 1; // work in 1-based terms
        while (n > 0) {
            int remainder = (n - 1) % 26;
            sb.insert(0, (char) ('A' + remainder));
            n = (n - 1) / 26;
        }
        return sb.toString();
    }

    /**
     * Inverse of {@link #columnLetter}: converts an Excel column letter (e.g. {@code "A"},
     * {@code "T"}, {@code "AB"}) to its 0-based column index. Used to resolve
     * {@link FileSourceConfig#lastColumn} into a fixed column count.
     *
     * @throws IllegalArgumentException if {@code letter} is empty or contains anything other
     *                                   than A-Z (after {@link FileSourceConfig} upper-cases it)
     */
    static int columnIndexFromLetter(String letter) {
        if (letter == null || letter.isEmpty()) {
            throw new IllegalArgumentException("last_column must not be empty");
        }
        int result = 0;
        for (char c : letter.toCharArray()) {
            if (c < 'A' || c > 'Z') {
                throw new IllegalArgumentException(
                    "last_column must contain only letters A-Z (Excel column notation), got: " + letter);
            }
            result = result * 26 + (c - 'A' + 1);
        }
        return result - 1;
    }

    /**
     * Resolves the column width shared by both {@link #parseCsv} and {@link #parseExcel}:
     * {@link FileSourceConfig#lastColumn} when set (an explicit, intentional cap — columns
     * beyond it are dropped even if the file extends further), otherwise the widest row seen
     * across the header and every data row, so a data row is never truncated just because the
     * header happens to be narrower.
     */
    private static int resolveColumnCount(FileSourceConfig config, int headerWidth, int maxDataWidth) {
        return config.lastColumn != null
            ? columnIndexFromLetter(config.lastColumn) + 1
            : Math.max(headerWidth, maxDataWidth);
    }

    /**
     * Builds the header-legend content object mapping each column letter to its real header
     * name, e.g. {@code {"A":"trade_id","B":"amount"}}, wrapped via
     * {@link FileHeaderLegend#wrapLegend} so it can be told apart from a data row while both
     * travel through the same {@code PCollection<Row>}. {@code DataSourceRecordSinkTransform}
     * unwraps it back to this plain content object before placing it in a page's
     * {@code DataHeaders} array.
     */
    private static String headerLegendJson(List<String> letters, List<String> headerValues) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < letters.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(letters.get(i)).append("\":");
            String name = i < headerValues.size() ? headerValues.get(i) : null;
            if (name == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(jsonEscape(name)).append("\"");
            }
        }
        sb.append("}");
        return FileHeaderLegend.wrapLegend(sb.toString());
    }

    private static String rowToJson(List<String> keys, List<String> values) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(jsonEscape(keys.get(i))).append("\":");
            String value = i < values.size() ? values.get(i) : null;
            if (value == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(jsonEscape(value)).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static List<String> excelRowToStrings(Row row) {
        List<String> values = new ArrayList<>();
        for (Cell cell : row) {
            values.add(cellToString(cell));
        }
        return values;
    }

    private static String cellToString(Cell cell) {
        if (cell == null) return "";
        return switch (cell.getCellType()) {
            case STRING  -> cell.getStringCellValue();
            case NUMERIC -> {
                double d = cell.getNumericCellValue();
                yield d == Math.floor(d) ? String.valueOf((long) d) : String.valueOf(d);
            }
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            case FORMULA -> cell.getCellFormula();
            default      -> "";
        };
    }

    // ── Exception type ───────────────────────────────────────────────────────

    public static final class FileSourceException extends RuntimeException {
        public FileSourceException(String msg, Throwable cause) { super(msg, cause); }
    }
}
