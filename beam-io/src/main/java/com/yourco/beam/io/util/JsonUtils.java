package com.yourco.beam.io.util;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.List;

/**
 * Shared JSON serialization utilities for sink transforms.
 *
 * <p>Handles Beam {@link Row} field types correctly:
 * strings are quoted and escaped, numerics and booleans are unquoted,
 * null values are emitted as JSON {@code null}.
 */
public final class JsonUtils {

    private JsonUtils() {}

    /**
     * Serializes a {@link Row} to a JSON object string.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>STRING → {@code "value"} (with {@code "} and {@code \} escaped)</li>
     *   <li>BOOLEAN, BYTE, INT16, INT32, INT64, FLOAT, DOUBLE, DECIMAL → bare value</li>
     *   <li>null → {@code null}</li>
     *   <li>All other types → quoted {@code toString()}</li>
     * </ul>
     */
    public static String rowToJson(Row row) {
        List<Schema.Field> fields = row.getSchema().getFields();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            if (i > 0) sb.append(',');
            sb.append('"').append(field.getName()).append("\":");
            appendValue(sb, row.getValue(field.getName()), field.getType());
        }
        return sb.append('}').toString();
    }

    private static void appendValue(StringBuilder sb, Object value, Schema.FieldType type) {
        if (value == null) {
            sb.append("null");
            return;
        }
        switch (type.getTypeName()) {
            case BOOLEAN, BYTE, INT16, INT32, INT64, FLOAT, DOUBLE, DECIMAL ->
                    sb.append(value);
            case STRING ->
                    sb.append('"')
                      .append(value.toString().replace("\\", "\\\\").replace("\"", "\\\""))
                      .append('"');
            default ->
                    sb.append('"').append(value).append('"');
        }
    }
}
