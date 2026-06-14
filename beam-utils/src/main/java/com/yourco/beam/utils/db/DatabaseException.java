package com.yourco.beam.utils.db;

/**
 * Unchecked wrapper for JDBC {@link java.sql.SQLException}.
 *
 * <p>All checked SQL exceptions are caught inside {@link JdbcDatabaseAdapter} and
 * re-thrown as this type, so callers of {@link DatabaseAdapter} do not need to
 * declare or catch checked exceptions.
 */
public final class DatabaseException extends RuntimeException {

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseException(String message) {
        super(message);
    }
}
