package com.yourco.beam.utils.db;

import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.utils.SecretManagerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link DatabaseAdapter} instances from pipeline options.
 *
 * <h2>Credential flow</h2>
 * <ol>
 *   <li>The JDBC URL and username come from {@code --paramDbUrl} and {@code --paramDbUser}.</li>
 *   <li>The password is fetched at runtime from GCP Secret Manager via
 *       {@code --paramDbCredentialSecretId} — it is never stored in options or logs.</li>
 *   <li>A {@link JdbcDatabaseAdapter} is created with a HikariCP pool.</li>
 * </ol>
 *
 * <p>The Dataflow and Cloud Composer service accounts must have
 * {@code roles/secretmanager.secretAccessor} on the secret.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
 *     ParameterRepository repo = new ParameterRepository(db, options);
 *     List<SourceConfig> configs = repo.fetchSourceConfigs(...);
 * }
 * }</pre>
 */
public final class DatabaseAdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAdapterFactory.class);

    private DatabaseAdapterFactory() {}

    /**
     * Creates a {@link DatabaseAdapter} using credentials from pipeline options and Secret Manager.
     *
     * @throws IllegalArgumentException if required options (paramDbUrl, paramDbUser,
     *                                  paramDbCredentialSecretId) are missing
     * @throws DatabaseException        if the initial pool connection cannot be established
     */
    public static DatabaseAdapter create(FrameworkOptions options) {
        requireOption(options.getParamDbUrl(), "--paramDbUrl");
        requireOption(options.getParamDbUser(), "--paramDbUser");
        requireOption(options.getParamDbCredentialSecretId(), "--paramDbCredentialSecretId");

        LOG.info("Fetching parameter DB password from Secret Manager: {}",
                 options.getParamDbCredentialSecretId());
        String password = SecretManagerUtils.fetchSecret(options.getParamDbCredentialSecretId());

        return new JdbcDatabaseAdapter(options.getParamDbUrl(), options.getParamDbUser(), password);
    }

    private static void requireOption(String value, String flagName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                flagName + " is required for parameter DB access but was not provided.");
        }
    }
}
