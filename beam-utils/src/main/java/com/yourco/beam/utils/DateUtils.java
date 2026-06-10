package com.yourco.beam.utils;

import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Date utilities for report pipelines.
 *
 * <p>Report pipelines often need a consistent "business date" for:
 * <ul>
 *   <li>Determining which partition of data to read</li>
 *   <li>Labelling output files (e.g., {@code report_2024-01-15.csv})</li>
 *   <li>Setting report headers and email subjects</li>
 *   <li>Filtering BQ data to a specific day</li>
 * </ul>
 *
 * <p>All methods are pure functions — stateless, serialization-safe.
 * They can be called in the driver JVM or inside a DoFn.
 *
 * <h2>Integration with Airflow</h2>
 * Airflow's built-in templating makes date passing easy:
 * <pre>
 * "--runDate": "{{ ds }}"          # e.g. 2024-01-15  (execution date)
 * "--runDate": "{{ prev_ds }}"     # previous execution date
 * "--runDate": "{{ macros.ds_add(ds, -1) }}"  # custom offset
 * </pre>
 */
public final class DateUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DateUtils.class);

    /** Standard ISO-8601 date format used throughout this framework. */
    public static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;

    /** Compact format for filenames and BQ partition decorators: {@code yyyyMMdd}. */
    public static final DateTimeFormatter COMPACT_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** Human-readable format for report headers: {@code dd MMM yyyy} (e.g., 15 Jan 2024). */
    public static final DateTimeFormatter DISPLAY_DATE = DateTimeFormatter.ofPattern("dd MMM yyyy");

    private DateUtils() {}

    // =========================================================================
    // RUN DATE RESOLUTION
    // =========================================================================

    /**
     * Resolves the run date from pipeline options.
     *
     * <p>If {@code --runDate} is set, parses it as ISO-8601 ({@code YYYY-MM-DD}).
     * If not set, defaults to today's date in UTC — consistent across all timezones.
     *
     * @param options pipeline options containing the optional {@code runDate} flag
     * @return the resolved run date
     * @throws IllegalArgumentException if {@code runDate} is set but not valid ISO-8601
     */
    public static LocalDate resolveRunDate(FrameworkOptions options) {
        String runDateStr = options.getRunDate();
        if (runDateStr == null || runDateStr.isBlank()) {
            LocalDate today = LocalDate.now(ZoneOffset.UTC);
            LOG.info("--runDate not set; defaulting to today UTC: {}", today);
            return today;
        }
        try {
            LocalDate parsed = LocalDate.parse(runDateStr, ISO_DATE);
            LOG.info("Run date resolved: {}", parsed);
            return parsed;
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                "--runDate '" + runDateStr + "' is not a valid ISO-8601 date (YYYY-MM-DD). "
                + "Example: --runDate=2024-01-15", e);
        }
    }

    // =========================================================================
    // FORMATTING
    // =========================================================================

    /**
     * Formats a date as {@code YYYY-MM-DD} (ISO-8601).
     * Use this when writing to BigQuery partition decorators or GCS path segments.
     *
     * <p>Example: {@code 2024-01-15}
     */
    public static String toIsoString(LocalDate date) {
        return date.format(ISO_DATE);
    }

    /**
     * Formats a date as {@code yyyyMMdd} (no separators).
     * Use this for GCS file names, BQ table suffixes, or compact identifiers.
     *
     * <p>Example: {@code 20240115}
     */
    public static String toCompactString(LocalDate date) {
        return date.format(COMPACT_DATE);
    }

    /**
     * Formats a date as a human-readable string for report headers and email subjects.
     *
     * <p>Example: {@code 15 Jan 2024}
     */
    public static String toDisplayString(LocalDate date) {
        return date.format(DISPLAY_DATE);
    }

    // =========================================================================
    // PATH AND TABLE BUILDING
    // =========================================================================

    /**
     * Builds a date-partitioned GCS path by appending the run date.
     *
     * <p>Example:
     * <pre>{@code
     * // basePath = "gs://my-bucket/reports/"
     * // runDate  = 2024-01-15
     * // returns  = "gs://my-bucket/reports/2024-01-15/"
     * String outputPath = DateUtils.partitionedPath(options.getGcsSinkPath(), runDate);
     * }</pre>
     *
     * @param basePath the GCS path prefix (trailing slash optional)
     * @param date     the date to append as a subdirectory
     * @return path with the date appended as a partition segment
     */
    public static String partitionedPath(String basePath, LocalDate date) {
        String base = basePath.endsWith("/") ? basePath : basePath + "/";
        return base + toIsoString(date) + "/";
    }

    /**
     * Builds a BigQuery table name with a date suffix for sharded tables.
     *
     * <p>Example:
     * <pre>{@code
     * // baseTable = "my-project:reports.daily_summary"
     * // date      = 2024-01-15
     * // returns   = "my-project:reports.daily_summary$20240115"
     * }</pre>
     *
     * @param baseTable BigQuery table reference ({@code project:dataset.table})
     * @param date      the date to use as the shard decorator
     * @return sharded table reference with BQ's {@code $YYYYMMDD} decorator
     */
    public static String shardedTable(String baseTable, LocalDate date) {
        return baseTable + "$" + toCompactString(date);
    }

    // =========================================================================
    // DATE ARITHMETIC (CALENDAR-INDEPENDENT)
    // =========================================================================

    /**
     * Returns the Monday of the ISO week containing the given date.
     * Useful for weekly report pipelines.
     */
    public static LocalDate startOfWeek(LocalDate date) {
        return date.minusDays(date.getDayOfWeek().getValue() - 1L);
    }

    /**
     * Returns the first day of the month containing the given date.
     * Useful for monthly report pipelines.
     */
    public static LocalDate startOfMonth(LocalDate date) {
        return date.withDayOfMonth(1);
    }

    /**
     * Returns the first day of the year containing the given date.
     * Useful for year-to-date report pipelines.
     */
    public static LocalDate startOfYear(LocalDate date) {
        return date.withDayOfYear(1);
    }
}
