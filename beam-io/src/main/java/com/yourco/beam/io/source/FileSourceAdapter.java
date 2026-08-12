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
import java.util.Iterator;
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
 * single JSON object mapping each column letter to its real header name, tagged with
 * {@link FileHeaderLegend#MARKER_KEY} so readers can tell it apart from a data row (see that
 * class for where this must be filtered out). When there is no header row, no legend is
 * produced ({@code headerLegendJson()} is {@code null}) — there is no real name to record.
 *
 * <p>Downstream transforms parse each data row's JSON from the standard
 * {@link com.yourco.beam.model.Schemas#RAW_JSON} wire type, same as before.
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
     * @param fileBytes raw file content
     * @param config    file configuration (delimiter, hasHeader, etc.)
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
            List<CSVRecord> records = parser.getRecords();
            if (records.isEmpty()) return new FileParseResult(List.of(), null);

            int cols = records.get(0).size();
            List<String> letters = columnLetters(cols);
            String legendJson = null;
            int dataStartIndex;
            if (config.hasHeader) {
                legendJson = headerLegendJson(letters, records.get(0).toList());
                dataStartIndex = 1;
            } else {
                dataStartIndex = 0;
            }

            List<String> jsonRows = new ArrayList<>(records.size() - dataStartIndex);
            for (int i = dataStartIndex; i < records.size(); i++) {
                jsonRows.add(rowToJson(letters, records.get(i).toList()));
            }
            LOG.info("Parsed {} CSV rows ({} header)", jsonRows.size(), config.hasHeader ? "with" : "without");
            return new FileParseResult(jsonRows, legendJson);
        } catch (IOException e) {
            throw new FileSourceException("Failed to parse CSV", e);
        }
    }

    /**
     * Parses an Excel (.xlsx) file from raw bytes.
     *
     * @param fileBytes raw XLSX file content
     * @param config    file configuration (sheetIndex, hasHeader)
     * @return data rows keyed by column letter, plus the header legend if the file has headers
     */
    public static FileParseResult parseExcel(byte[] fileBytes, FileSourceConfig config) {
        try (Workbook workbook = new XSSFWorkbook(new ByteArrayInputStream(fileBytes))) {
            Sheet sheet = workbook.getSheetAt(config.sheetIndex);
            Iterator<Row> rowIterator = sheet.iterator();

            if (!rowIterator.hasNext()) return new FileParseResult(List.of(), null);

            int cols = sheet.getRow(0).getLastCellNum();
            List<String> letters = columnLetters(cols);
            String legendJson = null;
            if (config.hasHeader) {
                legendJson = headerLegendJson(letters, excelRowToStrings(rowIterator.next()));
            }

            List<String> jsonRows = new ArrayList<>();
            while (rowIterator.hasNext()) {
                List<String> values = excelRowToStrings(rowIterator.next());
                jsonRows.add(rowToJson(letters, values));
            }
            LOG.info("Parsed {} Excel rows from sheet {} ({})",
                     jsonRows.size(), config.sheetIndex, config.hasHeader ? "with header" : "no header");
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
     * Builds the header-legend JSON object mapping each column letter to its real header name,
     * tagged with {@link FileHeaderLegend#MARKER_KEY} so readers can tell it apart from a data
     * row: {@code {"__header_map__":true,"A":"trade_id","B":"amount"}}.
     */
    private static String headerLegendJson(List<String> letters, List<String> headerValues) {
        StringBuilder sb = new StringBuilder("{\"").append(FileHeaderLegend.MARKER_KEY).append("\":true");
        for (int i = 0; i < letters.size(); i++) {
            sb.append(",\"").append(letters.get(i)).append("\":");
            String name = i < headerValues.size() ? headerValues.get(i) : null;
            if (name == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(jsonEscape(name)).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
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
