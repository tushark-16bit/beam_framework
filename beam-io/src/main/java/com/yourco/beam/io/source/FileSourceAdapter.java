package com.yourco.beam.io.source;

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
 * <h2>Output format</h2>
 * Every row is returned as a JSON string matching the column headers. Downstream transforms
 * parse the JSON from the standard {@link com.yourco.beam.model.Schemas#RAW_JSON} wire type.
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
     * Parses a CSV file from raw bytes. Returns each data row as a JSON string.
     *
     * @param fileBytes raw file content
     * @param config    file configuration (delimiter, hasHeader, etc.)
     * @return list of JSON strings, one per data row
     */
    public static List<String> parseCsv(byte[] fileBytes, FileSourceConfig config) {
        String content = new String(fileBytes);
        CSVFormat format = CSVFormat.DEFAULT
            .builder()
            .setDelimiter(config.delimiter.charAt(0))
            .setSkipHeaderRecord(false)
            .build();

        try (CSVParser parser = format.parse(new StringReader(content))) {
            List<CSVRecord> records = parser.getRecords();
            if (records.isEmpty()) return List.of();

            // Determine headers
            List<String> headers;
            int dataStartIndex;
            if (config.hasHeader) {
                headers = records.get(0).toList();
                dataStartIndex = 1;
            } else {
                int cols = records.get(0).size();
                headers = new ArrayList<>(cols);
                for (int i = 0; i < cols; i++) headers.add("field_" + i);
                dataStartIndex = 0;
            }

            List<String> jsonRows = new ArrayList<>(records.size() - dataStartIndex);
            for (int i = dataStartIndex; i < records.size(); i++) {
                jsonRows.add(rowToJson(headers, records.get(i).toList()));
            }
            LOG.info("Parsed {} CSV rows ({} header)", jsonRows.size(), config.hasHeader ? "with" : "without");
            return jsonRows;
        } catch (IOException e) {
            throw new FileSourceException("Failed to parse CSV", e);
        }
    }

    /**
     * Parses an Excel (.xlsx) file from raw bytes. Returns each data row as a JSON string.
     *
     * @param fileBytes raw XLSX file content
     * @param config    file configuration (sheetIndex, hasHeader)
     * @return list of JSON strings, one per data row
     */
    public static List<String> parseExcel(byte[] fileBytes, FileSourceConfig config) {
        try (Workbook workbook = new XSSFWorkbook(new ByteArrayInputStream(fileBytes))) {
            Sheet sheet = workbook.getSheetAt(config.sheetIndex);
            Iterator<Row> rowIterator = sheet.iterator();

            if (!rowIterator.hasNext()) return List.of();

            List<String> headers;
            if (config.hasHeader) {
                headers = excelRowToStrings(rowIterator.next());
            } else {
                int cols = sheet.getRow(0).getLastCellNum();
                headers = new ArrayList<>(cols);
                for (int i = 0; i < cols; i++) headers.add("field_" + i);
            }

            List<String> jsonRows = new ArrayList<>();
            while (rowIterator.hasNext()) {
                List<String> values = excelRowToStrings(rowIterator.next());
                jsonRows.add(rowToJson(headers, values));
            }
            LOG.info("Parsed {} Excel rows from sheet {} ({})",
                     jsonRows.size(), config.sheetIndex, config.hasHeader ? "with header" : "no header");
            return jsonRows;
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

    private static String rowToJson(List<String> headers, List<String> values) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < headers.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(jsonEscape(headers.get(i))).append("\":");
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
