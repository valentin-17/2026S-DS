package org.oxoo2a.sim4da;

import java.util.function.Supplier;

public class SimulationBehavior {

    // Distribution function for the selection of the next message in a message queue
    private static RandomValues messageQueueSelector = null;
    private static RandomValues messageLatencySelector = null;
    private static long minMessageLatencyMillis = 0;
    private static long maxMessageLatencyMillis = 0;

    public static void setMessageQueueSelectionDistributionFunction ( Supplier<Double> distributionFunction ) {
        if (messageQueueSelector != null) {
            throw new OverwriteDistributionFunctionException(
                    "Distribution function for message queue selection has already been set");
        }
        messageQueueSelector = new RandomValues(distributionFunction);
    }
    public static int selectMessageInQueue ( int queueSize ) {
        assert(queueSize > 0);
        if (messageQueueSelector == null) {
            return 0;
        }
        else {
            return (int) messageQueueSelector.getLong(0, queueSize-1);
        }
    }

    /**
     * Configures simulated message transfer latency. The supplied distribution
     * must return values in [0, 1]; each sent message maps one sample into the
     * inclusive latency range given here.
     */
    public static void setMessageLatencyDistributionFunction (
            Supplier<Double> distributionFunction,
            long minLatencyMillis,
            long maxLatencyMillis
    ) {
        if (messageLatencySelector != null) {
            throw new OverwriteDistributionFunctionException(
                    "Distribution function for message latency has already been set");
        }
        if (minLatencyMillis < 0 || maxLatencyMillis < minLatencyMillis) {
            throw new IllegalArgumentException(
                    "Message latency range must satisfy 0 <= min <= max");
        }
        messageLatencySelector = new RandomValues(distributionFunction);
        minMessageLatencyMillis = minLatencyMillis;
        maxMessageLatencyMillis = maxLatencyMillis;
    }

    public static long selectMessageLatencyMillis() {
        if (messageLatencySelector == null) {
            return 0;
        }
        return messageLatencySelector.getLong(minMessageLatencyMillis, maxMessageLatencyMillis);
    }

    /** Resets all configurable behavior. Called from {@link Simulator#shutdown()}. */
    public static void reset() {
        messageQueueSelector = null;
        messageLatencySelector = null;
        minMessageLatencyMillis = 0;
        maxMessageLatencyMillis = 0;
    }
}
