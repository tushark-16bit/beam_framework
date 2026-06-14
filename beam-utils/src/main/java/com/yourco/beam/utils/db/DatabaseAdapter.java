package com.yourco.beam.utils.db;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Adapter interface for relational database operations.
 *
 * <p>Every external database interaction in this framework goes through this interface.
 * The separation means:
 * <ul>
 *   <li>The concrete implementation ({@link JdbcDatabaseAdapter}) is swappable without
 *       touching any business code.</li>
 *   <li>Tests can inject a fake without a live DB.</li>
 *   <li>All JDBC checked exceptions are translated to the unchecked
 *       {@link DatabaseException} — callers decide whether to handle or propagate.</li>
 * </ul>
 *
 * <p>Instances are created by {@link DatabaseAdapterFactory} and hold an internal
 * HikariCP connection pool. Always close the adapter when done:
 * <pre>{@code
 * try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
 *     List<Map<String, Object>> rows = db.query("SELECT ...", param1, param2);
 *     // use rows
 * }
 * }</pre>
 *
 * <p><b>Thread safety:</b> implementations backed by a connection pool are thread-safe.
 * <b>Do NOT call from inside a DoFn</b> — use {@link com.yourco.beam.transforms.side.SideEffectDbWriteTransform}
 * for in-pipeline DB writes, which handles the Beam serialization lifecycle correctly.
 */
public interface DatabaseAdapter extends AutoCloseable {

    /**
     * Executes a parameterised SELECT query and returns all result rows.
     *
     * <p>Each row is a {@code LinkedHashMap} keyed by column name (as returned by
     * {@code ResultSetMetaData.getColumnName}), preserving column order.
     *
     * @param sql    parameterised SQL with {@code ?} placeholders
     * @param params positional parameter values (matched left-to-right to {@code ?})
     * @return list of rows; empty list if no rows match
     * @throws DatabaseException if the query fails
     */
    List<Map<String, Object>> query(String sql, Object... params);

    /**
     * Executes a parameterised SELECT query and returns at most one row.
     *
     * @return {@link Optional#empty()} if no rows match; the first row otherwise
     * @throws DatabaseException if the query fails or returns more than one row
     */
    Optional<Map<String, Object>> queryOne(String sql, Object... params);

    /**
     * Executes a parameterised INSERT / UPDATE / DELETE.
     *
     * @return number of affected rows
     * @throws DatabaseException if the statement fails
     */
    int update(String sql, Object... params);

    /**
     * Releases all connections back to the pool and shuts the pool down.
     * Called automatically when used in try-with-resources.
     */
    @Override
    void close();
}
