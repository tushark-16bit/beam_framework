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
 * <p>Corresponding columns in the {@code source_config} table:
 * <pre>
 *   file_type, file_location, file_prefix, file_suffix,
 *   file_delimiter, file_has_header, file_sheet_index
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
    /** Whether the first row contains column headers. */
    public final boolean hasHeader;
    /** 0-based Excel sheet index. Ignored for CSV. Default: 0. */
    public final int sheetIndex;

    public FileSourceConfig(String fileType, String location, String prefix, String suffix,
                            String delimiter, boolean hasHeader, int sheetIndex) {
        this.fileType   = fileType != null ? fileType.toUpperCase() : "CSV";
        this.location   = location;
        this.prefix     = prefix != null ? prefix : "";
        this.suffix     = suffix != null ? suffix : "";
        this.delimiter  = delimiter != null ? delimiter : ",";
        this.hasHeader  = hasHeader;
        this.sheetIndex = Math.max(0, sheetIndex);
    }
}
