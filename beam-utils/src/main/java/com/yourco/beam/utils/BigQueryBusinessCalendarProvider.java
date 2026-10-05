package com.yourco.beam.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Fetches business calendars from the parameter store table in BigQuery and turns each into a
 * {@link BusinessCalendar} for {@link RunDateCalculator}.
 *
 * <h2>Where the data lives</h2>
 * The same parameter table the source and report configs use ({@code --paramBqProject} /
 * {@code --paramBqDataset} / {@code --paramStoreTable}): one row per calendar, where
 * <ul>
 *   <li>{@code parameter_group_name} = {@value #CALENDAR_GROUP} — marks the row as a calendar,</li>
 *   <li>{@code parameter_name} = the item's {@code calendarKey},</li>
 *   <li>{@code parameters_val_json} = the calendar definition (below).</li>
 * </ul>
 * The three column names are constants ({@link #COL_GROUP}, {@link #COL_NAME}, {@link #COL_VALUE})
 * so they can be changed in one place if the physical columns are named differently.
 *
 * <h2>Calendar JSON</h2>
 * <pre>{@code
 * [{"Calendar": {"holiday": "20260101,20270901", "weekend": "saturday,sunday"}}]
 * }</pre>
 * <ul>
 *   <li>{@code holiday} — comma-separated {@code yyyyMMdd} dates that are holidays. Optional /
 *       blank = no holidays. A malformed date fails the whole calendar (never silently dropped —
 *       a lost holiday would turn it into a business day).</li>
 *   <li>{@code weekend} — comma-separated weekday names ({@code saturday,sunday}; case-insensitive;
 *       the 3-letter forms {@code sat,sun} are accepted too). <b>Required</b>: a missing
 *       {@code weekend} key fails the calendar rather than guessing Saturday/Sunday. It may be
 *       blank for a calendar with no weekend.</li>
 * </ul>
 * A date is a business day when it is neither a weekend day nor a holiday.
 *
 * <h2>Behaviour</h2>
 * <ul>
 *   <li>One query per distinct {@code calendarKey} per run; the result is cached in memory (all
 *       dates of that calendar, so there is no date range to fall outside of).</li>
 *   <li>Driver JVM only, like the other repositories — never call from a DoFn.</li>
 *   <li>No row for the key, more than one row, bad JSON or a bad value → {@link #forKey} throws,
 *       and {@link RunDateCalculator} reports the item as not evaluable (not processed + failure
 *       notification), as the calendar must exist.</li>
 * </ul>
 */
public final class BigQueryBusinessCalendarProvider implements BusinessCalendarProvider {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryBusinessCalendarProvider.class);

    /** {@code parameter_group_name} value that marks a parameter row as a business calendar. */
    public static final String CALENDAR_GROUP = "FINACOE_Calendars";

    // Column names of the parameter table — the same ones BigQuerySourceConfigRepository reads.
    static final String COL_GROUP = "parameter_group_name";
    static final String COL_NAME  = "parameter_name";
    static final String COL_VALUE = "parameters_val_json";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter HOLIDAY_FORMAT = DateTimeFormatter.BASIC_ISO_DATE; // yyyyMMdd

    /** calendarKey → raw JSON text from the table; throws if the row doesn't exist. */
    private final Function<String, String> jsonFetcher;
    private final Map<String, BusinessCalendar> cache = new ConcurrentHashMap<>();

    /** Reads the parameter table named by {@code options}. The BigQuery client is created lazily. */
    public BigQueryBusinessCalendarProvider(FrameworkOptions options) {
        this(new BigQueryJsonFetcher(options));
    }

    /** For tests: supply the raw JSON for a calendar key instead of querying BigQuery. */
    BigQueryBusinessCalendarProvider(Function<String, String> jsonFetcher) {
        this.jsonFetcher = jsonFetcher;
    }

    @Override
    public BusinessCalendar forKey(String calendarKey) {
        return cache.computeIfAbsent(calendarKey, key -> {
            BusinessCalendar calendar = parse(key, jsonFetcher.apply(key));
            LOG.info("Loaded business calendar '{}' from the parameter table", key);
            return calendar;
        });
    }

    // ── Parsing (pure, unit-tested) ───────────────────────────────────────────

    /**
     * Converts the stored calendar JSON into a {@link BusinessCalendar}.
     *
     * @throws IllegalArgumentException if the JSON is not in the documented shape, or any weekday
     *         name or holiday date is not recognised
     */
    static BusinessCalendar parse(String calendarKey, String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("calendar '" + calendarKey + "' has an empty definition");
        }
        JsonNode root;
        try {
            root = JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "calendar '" + calendarKey + "' is not valid JSON: " + e.getMessage(), e);
        }
        JsonNode body = findCalendarBody(root);
        if (body == null) {
            throw new IllegalArgumentException(
                "calendar '" + calendarKey + "' has no \"Calendar\" object with holiday/weekend "
                + "(expected [{\"Calendar\": {\"holiday\": \"...\", \"weekend\": \"...\"}}])");
        }
        if (!body.has("weekend") || body.get("weekend").isNull()) {
            throw new IllegalArgumentException(
                "calendar '" + calendarKey + "' has no \"weekend\" entry — it is required "
                + "(e.g. \"saturday,sunday\"), not assumed");
        }
        Set<DayOfWeek> weekend = parseWeekend(calendarKey, body.get("weekend").asText());
        Set<LocalDate> holidays = parseHolidays(calendarKey,
            body.has("holiday") && !body.get("holiday").isNull() ? body.get("holiday").asText() : "");
        return new ListBusinessCalendar(weekend, holidays);
    }

    /** {@code [{"Calendar": {...}}]} — also tolerates a bare object or a bare {@code {...}} body. */
    private static JsonNode findCalendarBody(JsonNode node) {
        if (node == null) return null;
        if (node.isArray()) {
            for (JsonNode element : node) {
                JsonNode found = findCalendarBody(element);
                if (found != null) return found;
            }
            return null;
        }
        if (!node.isObject()) return null;
        if (node.has("holiday") || node.has("weekend")) return node;
        JsonNode inner = node.get("Calendar");
        if (inner == null) {
            // tolerate different capitalisation of the wrapper key
            for (var it = node.fields(); it.hasNext(); ) {
                var field = it.next();
                if (field.getKey().equalsIgnoreCase("calendar")) { inner = field.getValue(); break; }
            }
        }
        return inner != null && inner.isObject() ? inner : null;
    }

    private static Set<DayOfWeek> parseWeekend(String calendarKey, String text) {
        Set<DayOfWeek> days = EnumSet.noneOf(DayOfWeek.class);
        for (String raw : text.split(",")) {
            String name = raw.trim().toLowerCase(Locale.ROOT);
            if (name.isEmpty()) continue;
            days.add(switch (name) {
                case "monday",    "mon" -> DayOfWeek.MONDAY;
                case "tuesday",   "tue" -> DayOfWeek.TUESDAY;
                case "wednesday", "wed" -> DayOfWeek.WEDNESDAY;
                case "thursday",  "thu" -> DayOfWeek.THURSDAY;
                case "friday",    "fri" -> DayOfWeek.FRIDAY;
                case "saturday",  "sat" -> DayOfWeek.SATURDAY;
                case "sunday",    "sun" -> DayOfWeek.SUNDAY;
                default -> throw new IllegalArgumentException(
                    "calendar '" + calendarKey + "': unknown weekend day '" + raw.trim() + "'");
            });
        }
        return days;
    }

    private static Set<LocalDate> parseHolidays(String calendarKey, String text) {
        Set<LocalDate> holidays = new HashSet<>();
        for (String raw : text.split(",")) {
            String value = raw.trim();
            if (value.isEmpty()) continue;
            try {
                holidays.add(LocalDate.parse(value, HOLIDAY_FORMAT));
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                    "calendar '" + calendarKey + "': holiday '" + value + "' is not a yyyyMMdd date", e);
            }
        }
        return holidays;
    }

    /** weekend days + holidays; everything else is a business day. */
    static final class ListBusinessCalendar implements BusinessCalendar {
        private final Set<DayOfWeek> weekend;
        private final Set<LocalDate> holidays;

        ListBusinessCalendar(Set<DayOfWeek> weekend, Set<LocalDate> holidays) {
            this.weekend  = weekend;
            this.holidays = holidays;
        }

        @Override
        public boolean isBusinessDay(LocalDate date) {
            return !weekend.contains(date.getDayOfWeek()) && !holidays.contains(date);
        }
    }

    // ── BigQuery access ───────────────────────────────────────────────────────

    /** Looks up one calendar row in the parameter table. */
    private static final class BigQueryJsonFetcher implements Function<String, String> {
        private final String table;
        private BigQuery bigquery; // created on first use so constructing a provider never needs credentials

        BigQueryJsonFetcher(FrameworkOptions options) {
            String project = options.getParamBqProject() != null && !options.getParamBqProject().isBlank()
                             ? options.getParamBqProject() : options.getProject();
            this.table = "`" + project + "." + options.getParamBqDataset()
                       + "." + options.getParamStoreTable() + "`";
        }

        private synchronized BigQuery client() {
            if (bigquery == null) {
                bigquery = BigQueryOptions.getDefaultInstance().getService();
            }
            return bigquery;
        }

        @Override
        public String apply(String calendarKey) {
            String sql = "SELECT " + COL_VALUE + " FROM " + table
                + " WHERE " + COL_GROUP + " = @grp AND " + COL_NAME + " = @key LIMIT 2";
            QueryJobConfiguration query = QueryJobConfiguration.newBuilder(sql)
                .addNamedParameter("grp", QueryParameterValue.string(CALENDAR_GROUP))
                .addNamedParameter("key", QueryParameterValue.string(calendarKey))
                .setUseLegacySql(false)
                .build();
            List<String> rows = new ArrayList<>();
            try {
                for (FieldValueList row : client().query(query).iterateAll()) {
                    rows.add(row.get(0).isNull() ? null : row.get(0).getStringValue());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("BQ calendar query interrupted", e);
            }
            if (rows.isEmpty()) {
                throw new IllegalArgumentException(
                    "no calendar '" + calendarKey + "' in " + table + " (" + COL_GROUP + " = "
                    + CALENDAR_GROUP + ")");
            }
            if (rows.size() > 1) {
                throw new IllegalArgumentException(
                    "calendar '" + calendarKey + "' is defined more than once in " + table
                    + " — refusing to pick one");
            }
            return rows.get(0);
        }
    }
}
