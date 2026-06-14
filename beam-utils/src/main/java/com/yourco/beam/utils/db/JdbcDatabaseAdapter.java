package com.yourco.beam.utils.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JDBC-backed implementation of {@link DatabaseAdapter} using HikariCP connection pooling.
 *
 * <h2>Why HikariCP?</h2>
 * The driver JVM may query the parameter DB multiple times during pipeline assembly
 * (validation check, source config fetch, auth token resolution). HikariCP maintains
 * a small pool of pre-opened connections which avoids the overhead of establishing a
 * new TCP + TLS + auth handshake on every call. Pool size is kept small (default: 3)
 * because this runs in the driver JVM, not on workers.
 *
 * <h2>Why not call from inside DoFns?</h2>
 * Dataflow workers are separate JVMs with no shared state. Each worker would need its
 * own pool, which means potentially hundreds of connections. Use
 * {@link com.yourco.beam.transforms.side.SideEffectDbWriteTransform} for in-pipeline
 * DB writes — it creates a per-worker pool in {@code @Setup}.
 *
 * <h2>Usage</h2>
 * Always use try-with-resources so the pool is shut down cleanly:
 * <pre>{@code
 * try (DatabaseAdapter db = new JdbcDatabaseAdapter(url, user, password)) {
 *     List<Map<String, Object>> rows = db.query("SELECT ...", params);
 * }
 * }</pre>
 */
public final class JdbcDatabaseAdapter implements DatabaseAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDatabaseAdapter.class);

    private final HikariDataSource dataSource;

    public JdbcDatabaseAdapter(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30_000);
        config.setIdleTimeout(60_000);
        config.setPoolName("beam-param-db");
        this.dataSource = new HikariDataSource(config);
        LOG.info("Parameter DB pool initialised: {}", jdbcUrl);
    }

    @Override
    public List<Map<String, Object>> query(String sql, Object... params) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            bindParams(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return toRowList(rs);
            }
        } catch (SQLException e) {
            throw new DatabaseException("Query failed: " + sql, e);
        }
    }

    @Override
    public Optional<Map<String, Object>> queryOne(String sql, Object... params) {
        List<Map<String, Object>> rows = query(sql, params);
        if (rows.isEmpty()) return Optional.empty();
        if (rows.size() > 1) {
            throw new DatabaseException(
                "Expected at most one row but got " + rows.size() + " for: " + sql);
        }
        return Optional.of(rows.get(0));
    }

    @Override
    public int update(String sql, Object... params) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            bindParams(ps, params);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException("Update failed: " + sql, e);
        }
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("Parameter DB pool closed.");
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private static void bindParams(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

    private static List<Map<String, Object>> toRowList(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            // LinkedHashMap preserves column order as returned by the DB
            Map<String, Object> row = new LinkedHashMap<>(cols);
            for (int i = 1; i <= cols; i++) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }
}
