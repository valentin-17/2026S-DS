package org.oxoo2a.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.oxoo2a.sim4da.OverwriteDistributionFunctionException;
import org.oxoo2a.sim4da.SimulationBehavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SimulationBehaviorLatencyTest {

    @AfterEach
    void resetBehavior() {
        SimulationBehavior.reset();
    }

    @Test
    void latencyDefaultsToZero() {
        assertEquals(0, SimulationBehavior.selectMessageLatencyMillis());
    }

    @Test
    void latencyDistributionMapsIntoConfiguredRange() {
        SimulationBehavior.setMessageLatencyDistributionFunction(() -> 0.5, 20, 80);

        assertEquals(50, SimulationBehavior.selectMessageLatencyMillis());
    }

    @Test
    void latencyDistributionCanOnlyBeConfiguredOncePerSimulation() {
        SimulationBehavior.setMessageLatencyDistributionFunction(() -> 0.0, 5, 10);

        assertThrows(OverwriteDistributionFunctionException.class,
                () -> SimulationBehavior.setMessageLatencyDistributionFunction(() -> 1.0, 5, 10));
    }

    @Test
    void latencyRangeMustBeValid() {
        assertThrows(IllegalArgumentException.class,
                () -> SimulationBehavior.setMessageLatencyDistributionFunction(() -> 0.0, -1, 10));
        assertThrows(IllegalArgumentException.class,
                () -> SimulationBehavior.setMessageLatencyDistributionFunction(() -> 0.0, 10, 5));
    }
}
