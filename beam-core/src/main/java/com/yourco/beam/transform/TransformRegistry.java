package com.yourco.beam.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Loads and indexes all {@link BeamTransform} implementations found on the
 * classpath via Java SPI ({@link ServiceLoader}).
 *
 * <p>Used only in the <em>driver JVM</em> that builds the Beam graph — never
 * serialized to workers.
 *
 * <h2>Discovery</h2>
 * Every JAR that provides transforms must contain:
 * <pre>META-INF/services/com.yourco.beam.transform.BeamTransform</pre>
 * with one fully-qualified class name per line. The {@code maven-shade-plugin}'s
 * {@code ServicesResourceTransformer} merges these files across JARs automatically.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * TransformRegistry registry = TransformRegistry.load();
 * List<BeamTransform> chain  = registry.resolve("filter-nulls,mask-pii");
 * }</pre>
 */
public final class TransformRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(TransformRegistry.class);

    private final Map<String, BeamTransform> registry;

    private TransformRegistry(Map<String, BeamTransform> registry) {
        this.registry = registry;
    }

    /** Loads transforms visible to the current thread's context class loader. */
    public static TransformRegistry load() {
        return load(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Loads transforms using an explicit {@link ClassLoader}.
     * Useful for loading transforms from a supplemental JAR at runtime.
     *
     * @throws IllegalStateException if two transforms share the same {@link BeamTransform#name()}
     */
    public static TransformRegistry load(ClassLoader classLoader) {
        Map<String, BeamTransform> map = new LinkedHashMap<>();

        ServiceLoader.load(BeamTransform.class, classLoader).forEach(transform -> {
            String transformName = transform.name();
            if (map.containsKey(transformName)) {
                throw new IllegalStateException(
                        "Duplicate transform name '" + transformName + "'. "
                        + "Each BeamTransform must have a unique name().");
            }
            map.put(transformName, transform);
            LOG.info("Registered transform: '{}'", transformName);
        });

        LOG.info("TransformRegistry loaded {} transform(s): {}", map.size(), map.keySet());
        return new TransformRegistry(Collections.unmodifiableMap(map));
    }

    /**
     * Resolves a comma-separated chain spec into an ordered list of
     * {@link BeamTransform} instances.
     *
     * @param chainSpec e.g. {@code "filter-nulls,mask-pii"} — order is preserved
     * @return ordered, unmodifiable list; empty if chainSpec is null or blank
     * @throws IllegalArgumentException if any name in the spec is not registered
     */
    public List<BeamTransform> resolve(String chainSpec) {
        if (chainSpec == null || chainSpec.isBlank()) {
            LOG.warn("No transformChain specified — data will pass through unchanged.");
            return List.of();
        }

        return Arrays.stream(chainSpec.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .map(name -> {
                    BeamTransform transform = registry.get(name);
                    if (transform == null) {
                        throw new IllegalArgumentException(
                                "Unknown transform '" + name + "'. "
                                + "Registered: " + registry.keySet());
                    }
                    return transform;
                })
                .toList();   // Java 16+ — returns an unmodifiable list
    }

    public Set<String> registeredNames() { return registry.keySet(); }

    public boolean contains(String name) { return registry.containsKey(name); }
}
