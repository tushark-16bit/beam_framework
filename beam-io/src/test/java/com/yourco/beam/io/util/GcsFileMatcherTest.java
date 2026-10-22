package com.yourco.beam.io.util;

import com.yourco.beam.io.util.GcsFileMatcher.Candidate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class GcsFileMatcherTest {

    private static final String STEM = "rpt_202401";

    @Test
    void exactNameWins() {
        var files = List.of(new Candidate("X_rpt_202401_2024-02-05.csv", 9),
                            new Candidate("rpt_202401_2024-02-05.csv", 1));
        assertEquals("rpt_202401_2024-02-05.csv",
            GcsFileMatcher.pick(files, "rpt_202401_2024-02-05.csv", STEM, ".csv"));
    }

    @Test
    void prefixedFileIsFoundAndNewestChosen() {
        var files = List.of(new Candidate("A1_rpt_202401_2024-02-05.csv", 1),
                            new Candidate("B2_rpt_202401_2024-02-05.csv", 5),
                            new Candidate("other.csv", 9));
        assertEquals("B2_rpt_202401_2024-02-05.csv",
            GcsFileMatcher.pick(files, "rpt_202401_2024-02-05.csv", STEM, ".csv"));
    }

    @Test
    void wrongExtensionOrStemIsIgnored() {
        var files = List.of(new Candidate("P_rpt_202401_2024-02-05.json", 1),
                            new Candidate("P_rpt_202402_2024-02-06.csv", 2));
        assertNull(GcsFileMatcher.pick(files, "rpt_202401_2024-02-05.csv", STEM, ".csv"));
    }
}
