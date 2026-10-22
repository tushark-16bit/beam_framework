package com.yourco.beam.io.util;

import java.util.List;

/**
 * Pure matching logic for finding a report output file in a GCS directory when its stored object
 * name is not exactly the name the framework computed (e.g. something added an extra prefix).
 *
 * <p>No GCS client here, so it is unit-testable; {@code EmailSendUtility.FetchFileFromGcsByPrefix}
 * lists the directory and feeds the candidates in.
 */
public final class GcsFileMatcher {

    private GcsFileMatcher() {}

    /** One object in the directory: its file name (no directory part) and last-update time. */
    public record Candidate(String fileName, long updatedMillis) {}

    /**
     * Picks the file to attach.
     *
     * <ol>
     *   <li>a file named exactly {@code expectedName}, else</li>
     *   <li>among files whose name contains {@code stem} and ends with {@code extension}
     *       (case-insensitive), the most recently updated one.</li>
     * </ol>
     *
     * @return the chosen file name, or {@code null} when nothing matches
     */
    public static String pick(List<Candidate> candidates, String expectedName,
                              String stem, String extension) {
        if (candidates == null) return null;
        for (Candidate c : candidates) {
            if (c.fileName().equals(expectedName)) return c.fileName();
        }
        String stemLc = stem == null ? "" : stem.toLowerCase();
        String extLc  = extension == null ? "" : extension.toLowerCase();
        Candidate best = null;
        for (Candidate c : candidates) {
            String n = c.fileName().toLowerCase();
            if (!stemLc.isEmpty() && !n.contains(stemLc)) continue;
            if (!extLc.isEmpty() && !n.endsWith(extLc)) continue;
            if (best == null || c.updatedMillis() > best.updatedMillis()) best = c;
        }
        return best == null ? null : best.fileName();
    }
}
