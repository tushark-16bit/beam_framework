package com.yourco.beam.model;

import java.io.Serializable;

/**
 * Configuration for a file-based data source (CSV or Excel on GCS), fetched from the parameter DB.
 *
 * <p>The final GCS path is assembled as: {@code location + resolvedPrefix + resolvedSuffix}.
 * Both prefix and suffix may contain the following placeholders which are substituted at runtime:
 * <ul>
 *   <li>{@code {date}}        — run date in {@code yyyy-MM-dd} format</li>
 *   <li>{@code {dateCompact}} — run date in {@code yyyyMMdd} format</li>
 *   <li>{@code {periodId}}    — the value of {@code --periodId}</li>
 * </ul>
 *
 * <p>Example: {@code prefix="trades_"}, {@code suffix="_{date}.csv"} with runDate 2024-01-15
 * resolves to {@code trades_2024-01-15.csv}.
 *
 * <p>{@link #firstRow} (1-based) skips any leading rows before real content starts. The row at
 * that position becomes the header row if {@link #hasHeader} is true, or the first data row
 * otherwise. {@link #lastColumn}, if set, fixes the column width (Excel-style letter, e.g.
 * {@code "T"}) used for both the header legend and every data row, overriding the default of
 * auto-detecting width from the widest row seen (header or data) — see {@code FileSourceAdapter}.
 *
 * <p>Corresponding columns in the {@code source_config} table:
 * <pre>
 *   file_type, file_location, file_prefix, file_suffix, file_delimiter,
 *   file_has_header, file_sheet_index, file_first_row, file_last_column
 * </pre>
 */
public final class FileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** File type. Supported values: {@code CSV}, {@code EXCEL}. */
    public final String fileType;
    /** GCS path prefix, e.g. {@code gs://my-bucket/raw/}. */
    public final String location;
    /** File name prefix. May contain date/periodId placeholders. */
    public final String prefix;
    /** File name suffix including extension. May contain placeholders. */
    public final String suffix;
    /** CSV column delimiter. Default: {@code ,}. Ignored for Excel. */
    public final String delimiter;
    /** Whether the row at {@link #firstRow} contains column headers. */
    public final boolean hasHeader;
    /** 0-based Excel sheet index. Ignored for CSV. Default: 0. */
    public final int sheetIndex;
    /**
     * 1-based row number where real content starts — everything before it is skipped. That row
     * is the header row if {@link #hasHeader} is true, otherwise the first data row. Default: 1
     * (start at the very first row, same as before this field existed).
     */
    public final int firstRow;
    /**
     * Excel-style letter (e.g. {@code "A"}, {@code "T"}, {@code "AB"}) marking the last column to
     * read, inclusive. When set, this fixes the column width for the header legend and every data
     * row — columns beyond it are dropped even if data extends further. When {@code null}
     * (default), width is auto-detected as the widest row seen (header or data), so no data
     * column is ever dropped just because the header row is narrower.
     */
    public final String lastColumn;

    public FileSourceConfig(String fileType, String location, String prefix, String suffix,
                            String delimiter, boolean hasHeader, int sheetIndex,
                            int firstRow, String lastColumn) {
        this.fileType   = fileType != null ? fileType.toUpperCase() : "CSV";
        this.location   = location;
        this.prefix     = prefix != null ? prefix : "";
        this.suffix     = suffix != null ? suffix : "";
        this.delimiter  = delimiter != null ? delimiter : ",";
        this.hasHeader  = hasHeader;
        this.sheetIndex = Math.max(0, sheetIndex);
        this.firstRow   = Math.max(1, firstRow);
        this.lastColumn = (lastColumn != null && !lastColumn.isBlank())
                          ? lastColumn.trim().toUpperCase() : null;
    }
}
