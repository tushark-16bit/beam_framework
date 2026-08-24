package com.yourco.beam.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineConfigTest {

    @Test
    void acceptsDataSourceStepsFollowedByATerminalReportStep() {
        PipelineConfig config = new PipelineConfig(List.of(
            new DataSourceStep("trades", "eod"),
            new DataSourceStep("fx_rates", "eod"),
            new ReportStep("daily_trades_report", "eod")
        ));

        assertEquals(3, config.steps().size());
        assertTrue(config.steps().get(0) instanceof DataSourceStep);
        assertTrue(config.steps().get(2) instanceof ReportStep);
    }

    @Test
    void rejectsEmptySteps() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineConfig(List.of()));
    }

    @Test
    void rejectsNullSteps() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineConfig(null));
    }

    @Test
    void rejectsSequenceNotEndingInAReportStep() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineConfig(List.of(
            new ReportStep("daily_trades_report", "eod"),
            new DataSourceStep("trades", "eod")
        )));
    }

    @Test
    void rejectsASoleDataSourceStepWithNoTerminalReport() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineConfig(List.of(
            new DataSourceStep("trades", "eod")
        )));
    }

    @Test
    void stepsListIsDefensivelyCopiedAndImmutable() {
        var mutable = new java.util.ArrayList<PipelineStepConfig>();
        mutable.add(new ReportStep("r", "s"));
        PipelineConfig config = new PipelineConfig(mutable);

        mutable.clear(); // mutating the original list must not affect the record
        assertEquals(1, config.steps().size());
        assertThrows(UnsupportedOperationException.class,
            () -> config.steps().add(new ReportStep("other", "s")));
    }

    @Test
    void typeDiscriminatorsMatchStepKind() {
        assertEquals("DATA_SOURCE", new DataSourceStep("trades", "eod").type());
        assertEquals("REPORT", new ReportStep("r", "s").type());
    }
}
