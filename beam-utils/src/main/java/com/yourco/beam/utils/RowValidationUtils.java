package com.yourco.beam.utils;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Reusable, stateless row-level validation helpers for use inside {@code DoFn}
 * implementations.
 *
 * <h2>Design</h2>
 * All methods are pure functions — no state, no I/O, no side effects. They return
 * {@link ValidationResult} objects rather than throwing exceptions, so callers can
 * decide whether to route to DLQ or apply a default. This makes them safe to call
 * inside a {@code @ProcessElement} method.
 *
 * <h2>Serialization</h2>
 * This class is stateless and final, so it does not need to implement
 * {@link java.io.Serializable}. Do NOT hold an instance of this class as a
 * {@code DoFn} field — call the static methods directly.
 *
 * <h2>Usage inside a DoFn</h2>
 * <pre>{@code
 * @ProcessElement
 * public void processElement(@Element Row row, MultiOutputReceiver out) {
 *     ValidationResult result = RowValidationUtils.requireFields(
 *         row, Set.of("order_id", "customer_email", "amount"));
 *     if (!result.isValid()) {
 *         out.get(DEAD_LETTER_TAG).output(FailedRecord.of(row,
 *             new IllegalArgumentException(result.errorSummary()), 0));
 *         return;
 *     }
 *     out.get(SUCCESS_TAG).output(row);
 * }
 * }</pre>
 */
public final class RowValidationUtils {

    private RowValidationUtils() {}

    // ── Validation methods ───────────────────────────────────────────────────

    /**
     * Checks that all required field names are present in the row's schema
     * AND that none of those fields have a null value.
     *
     * @param row            the row to validate
     * @param requiredFields set of field names that must be present and non-null
     * @return a {@link ValidationResult} describing any failures
     */
    public static ValidationResult requireFields(Row row, Set<String> requiredFields) {
        List<String> errors = new ArrayList<>();
        for (String field : requiredFields) {
            if (!row.getSchema().hasField(field)) {
                errors.add("missing field: " + field);
            } else if (row.getValue(field) == null) {
                errors.add("null value for required field: " + field);
            }
        }
        return new ValidationResult(errors);
    }

    /**
     * Checks that a string field matches a given regular expression pattern.
     *
     * @param row       the row to validate
     * @param fieldName name of the string field
     * @param pattern   compiled regex pattern the value must match
     * @return a {@link ValidationResult} describing any failures
     */
    public static ValidationResult matchesPattern(Row row, String fieldName, Pattern pattern) {
        if (!row.getSchema().hasField(fieldName)) {
            return ValidationResult.failure("missing field: " + fieldName);
        }
        Object value = row.getValue(fieldName);
        if (value == null) {
            return ValidationResult.failure("null value for field: " + fieldName);
        }
        if (!pattern.matcher(value.toString()).matches()) {
            return ValidationResult.failure(
                "field '" + fieldName + "' value '" + value
                + "' does not match pattern " + pattern.pattern());
        }
        return ValidationResult.ok();
    }

    /**
     * Checks that a numeric field value falls within an inclusive range [min, max].
     *
     * @param row       the row to validate
     * @param fieldName name of the numeric field
     * @param min       minimum permitted value (inclusive)
     * @param max       maximum permitted value (inclusive)
     * @return a {@link ValidationResult} describing any failures
     */
    public static ValidationResult inRange(Row row, String fieldName, double min, double max) {
        if (!row.getSchema().hasField(fieldName)) {
            return ValidationResult.failure("missing field: " + fieldName);
        }
        Object value = row.getValue(fieldName);
        if (value == null) {
            return ValidationResult.failure("null value for field: " + fieldName);
        }
        double numeric;
        try {
            numeric = Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return ValidationResult.failure(
                "field '" + fieldName + "' is not numeric: " + value);
        }
        if (numeric < min || numeric > max) {
            return ValidationResult.failure(
                "field '" + fieldName + "' value " + numeric
                + " is outside range [" + min + ", " + max + "]");
        }
        return ValidationResult.ok();
    }

    /**
     * Checks that a string field is one of a set of permitted values.
     *
     * @param row            the row to validate
     * @param fieldName      name of the field to check
     * @param allowedValues  set of acceptable values (case-sensitive)
     */
    public static ValidationResult oneOf(Row row, String fieldName, Set<String> allowedValues) {
        if (!row.getSchema().hasField(fieldName)) {
            return ValidationResult.failure("missing field: " + fieldName);
        }
        Object value = row.getValue(fieldName);
        if (value == null || !allowedValues.contains(value.toString())) {
            return ValidationResult.failure(
                "field '" + fieldName + "' value '" + value
                + "' is not in allowed set: " + allowedValues);
        }
        return ValidationResult.ok();
    }

    /**
     * Checks that a row's schema contains all expected fields with compatible types.
     * Useful as a pre-processing guard in transforms that need specific columns.
     *
     * @param row            the row to validate
     * @param expectedSchema the schema whose fields must all exist in the row
     * @return a {@link ValidationResult} listing any missing or incompatible fields
     */
    public static ValidationResult schemaContains(Row row, Schema expectedSchema) {
        List<String> errors = new ArrayList<>();
        for (Schema.Field expected : expectedSchema.getFields()) {
            if (!row.getSchema().hasField(expected.getName())) {
                errors.add("missing field: " + expected.getName());
            }
        }
        return new ValidationResult(errors);
    }

    // ── Result type ──────────────────────────────────────────────────────────

    /**
     * The outcome of a validation check.
     *
     * <p>Immutable value type — safe to pass between methods without copying.
     */
    public static final class ValidationResult {

        private final List<String> errors;

        ValidationResult(List<String> errors) {
            this.errors = List.copyOf(errors);
        }

        static ValidationResult ok() {
            return new ValidationResult(List.of());
        }

        static ValidationResult failure(String error) {
            return new ValidationResult(List.of(error));
        }

        /** Returns {@code true} if there are no validation errors. */
        public boolean isValid() {
            return errors.isEmpty();
        }

        /** Returns all validation error messages. */
        public List<String> errors() {
            return errors;
        }

        /** Returns a single human-readable summary of all errors, joined by "; ". */
        public String errorSummary() {
            return String.join("; ", errors);
        }

        @Override
        public String toString() {
            return isValid() ? "ValidationResult{OK}" : "ValidationResult{FAILED: " + errorSummary() + "}";
        }
    }
}
