package com.yourco.beam.transforms.side;

import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.SecretManagerUtils;
import com.yourco.beam.utils.db.DatabaseException;
import com.yourco.beam.utils.db.JdbcDatabaseAdapter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Side-effect transform that writes each incoming {@link Row} to a JDBC database table.
 *
 * <h2>What "side effect" means in Beam</h2>
 * Produces {@link PDone} — no data output. Runs as a concurrent branch alongside the
 * main pipeline. Use for audit logging, status updates, or writing processing metadata
 * back to the parameter DB without blocking the data path.
 *
 * <h2>Column mapping</h2>
 * Column names are taken directly from the Row's schema field names. Every field value
 * is written using {@link java.sql.PreparedStatement#setObject} with the field's Java
 * representation. Null values are handled correctly.
 *
 * <h2>Target table</h2>
 * The target table must exist and have columns matching (at least) the Row schema fields.
 * Extra columns in the table are ignored; missing columns cause a JDBC error.
 *
 * <h2>Why JDBC in a DoFn?</h2>
 * The DoFn uses {@code @Setup}/{@code @Teardown} to create and close a
 * {@link JdbcDatabaseAdapter} per worker thread. Workers do not share connections.
 * This is the correct pattern for in-pipeline DB writes in Beam.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Write processing audit log as a side effect
 * auditRows.apply("WriteAuditLog",
 *     new SideEffectDbWriteTransform(options, "audit_log"));
 * }</pre>
 */
public final class SideEffectDbWriteTransform extends PTransform<PCollection<Row>, PDone> {

    private final String jdbcUrl;
    private final String dbUser;
    private final String credentialSecretId;
    private final String targetTable;

    public SideEffectDbWriteTransform(FrameworkOptions options, String targetTable) {
        this.jdbcUrl            = options.getParamDbUrl();
        this.dbUser             = options.getParamDbUser();
        this.credentialSecretId = options.getParamDbCredentialSecretId();
        this.targetTable        = targetTable;
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        input.apply("DbWrite-" + targetTable,
                    ParDo.of(new DbWriteFn(jdbcUrl, dbUser, credentialSecretId, targetTable)));
        return PDone.in(input.getPipeline());
    }

    // ── DoFn ────────────────────────────────────────────────────────────────

    private static final class DbWriteFn extends DoFn<Row, Void> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(DbWriteFn.class);

        // Serialized — primitives and Strings
        private final String jdbcUrl;
        private final String dbUser;
        private final String credentialSecretId;
        private final String targetTable;

        // Transient — created per worker
        private transient JdbcDatabaseAdapter dbAdapter;

        DbWriteFn(String jdbcUrl, String dbUser, String credentialSecretId, String targetTable) {
            this.jdbcUrl            = jdbcUrl;
            this.dbUser             = dbUser;
            this.credentialSecretId = credentialSecretId;
            this.targetTable        = targetTable;
        }

        @Setup
        public void setup() {
            String password = SecretManagerUtils.fetchSecret(credentialSecretId);
            dbAdapter = new JdbcDatabaseAdapter(jdbcUrl, dbUser, password);
            LOG.info("DB writer initialised for table: {}", targetTable);
        }

        @ProcessElement
        public void processElement(@Element Row row) {
            Schema schema = row.getSchema();
            List<String> columns = new ArrayList<>();
            List<Object> values  = new ArrayList<>();

            for (Schema.Field field : schema.getFields()) {
                columns.add(field.getName());
                values.add(row.getValue(field.getName()));
            }

            StringJoiner colList  = new StringJoiner(", ");
            StringJoiner paramList = new StringJoiner(", ");
            columns.forEach(c -> { colList.add(c); paramList.add("?"); });

            String sql = "INSERT INTO " + targetTable
                + " (" + colList + ") VALUES (" + paramList + ")";

            try {
                dbAdapter.update(sql, values.toArray());
            } catch (DatabaseException e) {
                // Log but don't fail the pipeline — DB write is best-effort side effect
                LOG.error("Failed to write row to {}: {}", targetTable, e.getMessage(), e);
            }
        }

        @Teardown
        public void teardown() {
            if (dbAdapter != null) {
                dbAdapter.close();
                dbAdapter = null;
            }
        }
    }
}
