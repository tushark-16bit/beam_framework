package com.yourco.beam.model.bq;

import java.io.Serializable;

/**
 * The default BigQuery settings of one data source — the {@code "bq_settings"} object of its
 * {@code parameters_val_json}, split by what the setting is for:
 *
 * <pre>{@code
 * "bq_settings": {
 *   "fetch_options":   {"project_id": "my-project", "dataset": "raw_data", "location": "US"},
 *   "storage_options": {"page_size": 250}
 * }
 * }</pre>
 *
 * <ul>
 *   <li>{@link FetchOptions} — settings for <b>reading the source</b> from BigQuery (BQ sources
 *       only): the default project and dataset a bare {@code query_details.table} is resolved
 *       against, and the BigQuery job location of the fetch (the source read, the schema preview
 *       and the BnC source query).</li>
 *   <li>{@link StorageOptions} — settings for <b>storing the rows in our tables</b> (DaRec):
 *       {@code page_size}, the number of source rows per DaRec page.</li>
 * </ul>
 *
 * <p>The DaRec/DaRefer tables themselves (project, dataset, table names) stay global
 * ({@code --checkpointBq*}, {@code --daRecTable}…), because reports read the same tables — they
 * are deliberately not per-source settings.
 */
public final class BqSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_PAGE_SIZE = 250;
    public static final int MAX_PAGE_SIZE     = 1000;

    public static final class FetchOptions implements Serializable {
        private static final long serialVersionUID = 1L;
        /** Null when not configured. */
        public final String projectId;
        /** Null when not configured. */
        public final String dataset;
        /** BigQuery job location (e.g. {@code US}, {@code EU}); null = BigQuery's default. */
        public final String location;

        public FetchOptions(String projectId, String dataset, String location) {
            this.projectId = blankToNull(projectId);
            this.dataset   = blankToNull(dataset);
            this.location  = blankToNull(location);
        }
    }

    public static final class StorageOptions implements Serializable {
        private static final long serialVersionUID = 1L;
        /** Source rows per DaRec page; 1..{@value BqSettings#MAX_PAGE_SIZE}, default 250. */
        public final int pageSize;

        /** @throws IllegalArgumentException when {@code pageSize} is outside 1..1000 */
        public StorageOptions(int pageSize) {
            if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
                throw new IllegalArgumentException("storage_options.page_size must be between 1 and "
                    + MAX_PAGE_SIZE + " (got " + pageSize + ")");
            }
            this.pageSize = pageSize;
        }
    }

    public final FetchOptions   fetch;
    public final StorageOptions storage;

    public BqSettings(FetchOptions fetch, StorageOptions storage) {
        this.fetch   = fetch != null ? fetch : new FetchOptions(null, null, null);
        this.storage = storage != null ? storage : new StorageOptions(DEFAULT_PAGE_SIZE);
    }

    /** All defaults. */
    public static BqSettings none() {
        return new BqSettings(null, null);
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }
}
