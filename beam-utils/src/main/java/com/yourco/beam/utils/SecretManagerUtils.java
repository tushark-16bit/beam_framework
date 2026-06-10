package com.yourco.beam.utils;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utilities for fetching secrets from GCP Secret Manager at pipeline-assembly time.
 *
 * <h2>The golden rule: never pass secrets as pipeline options</h2>
 * API keys, passwords, and tokens must NOT be passed via {@code --mySecret=value}
 * because they appear in plaintext in Dataflow job metadata, Airflow logs, and
 * process lists. Instead:
 * <ol>
 *   <li>Store the secret in GCP Secret Manager.</li>
 *   <li>Pass only the <em>secret ID</em> as a pipeline option
 *       (e.g., {@code --smtpPasswordSecretId=projects/p/secrets/smtp-pass/versions/latest}).</li>
 *   <li>Call {@link #fetchSecret(String)} in the driver JVM to retrieve the actual value.</li>
 *   <li>Pass the value to your code as a constructor argument, not via options.</li>
 * </ol>
 *
 * <h2>Example: fetching SMTP credentials</h2>
 * <pre>{@code
 * // In PipelineFactory or a transform's constructor (driver JVM only):
 * String smtpPassword = SecretManagerUtils.fetchSecret(
 *     options.getSmtpPasswordSecretId());
 * // smtpPassword is now in memory, never logged or stored
 * }</pre>
 *
 * <h2>Caching</h2>
 * Each call opens a new SecretManager gRPC channel. For multiple secrets in the
 * same process, use {@link #fetchSecret(SecretManagerServiceClient, String)} with
 * a shared client, or add a simple in-process cache if latency matters.
 *
 * <h2>IAM requirement</h2>
 * The Dataflow service account (and Cloud Composer service account) must have the
 * {@code roles/secretmanager.secretAccessor} role on each secret.
 */
public final class SecretManagerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SecretManagerUtils.class);

    private SecretManagerUtils() {}

    /**
     * Fetches the payload of a secret version from GCP Secret Manager.
     *
     * @param secretVersionName fully-qualified secret version name, e.g.
     *     {@code projects/my-project/secrets/my-secret/versions/latest}
     * @return the secret payload as a UTF-8 string
     * @throws RuntimeException if the secret cannot be accessed
     */
    public static String fetchSecret(String secretVersionName) {
        LOG.info("Fetching secret: {}", secretVersionName);
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            return fetchSecret(client, secretVersionName);
        } catch (IOException e) {
            throw new RuntimeException(
                "Failed to create SecretManager client: " + e.getMessage(), e);
        }
    }

    /**
     * Fetches a secret using a caller-provided {@link SecretManagerServiceClient}.
     * Use this overload when fetching multiple secrets to reuse the same gRPC channel.
     *
     * @param client           an open SecretManager client
     * @param secretVersionName fully-qualified secret version name
     * @return the secret payload as a UTF-8 string
     */
    public static String fetchSecret(SecretManagerServiceClient client,
                                     String secretVersionName) {
        try {
            SecretVersionName svn = SecretVersionName.parse(secretVersionName);
            AccessSecretVersionResponse response = client.accessSecretVersion(svn);
            return response.getPayload().getData().toStringUtf8();
        } catch (Exception e) {
            // Never log the secret value, only the name
            throw new RuntimeException(
                "Failed to fetch secret '" + secretVersionName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Builds a fully-qualified secret version name from its components.
     * Convenience method when you have the project, secret name, and version separately.
     *
     * @param project    GCP project ID
     * @param secretId   secret name (not the full path)
     * @param version    version number or {@code "latest"}
     * @return fully-qualified secret version name
     */
    public static String buildSecretName(String project, String secretId, String version) {
        return String.format("projects/%s/secrets/%s/versions/%s", project, secretId, version);
    }
}
