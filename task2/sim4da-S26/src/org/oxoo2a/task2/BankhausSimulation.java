package org.oxoo2a.task2;

import org.oxoo2a.sim4da.RandomValues;
import org.oxoo2a.sim4da.SimulationBehavior;
import org.oxoo2a.sim4da.Simulator;

import java.util.ArrayList;
import java.util.List;

public final class BankhausSimulation {
    private static final int NODE_COUNT = 5;
    private static final int INITIAL_BALANCE = 1_000;
    private static final int SIMULATION_SECONDS = 10;
    private static final long MIN_MESSAGE_LATENCY_MILLIS = 25;
    private static final long MAX_MESSAGE_LATENCY_MILLIS = 150;
    private static final long SNAPSHOT_CHANNEL_DRAIN_MILLIS =
            MAX_MESSAGE_LATENCY_MILLIS * 3 + 100;
    private static final int NAIVE_SNAPSHOT_COUNT = 3;
    private static final long NAIVE_SNAPSHOT_INTERVAL_MILLIS = 500;

    private BankhausSimulation() {
    }

    public static void main(String[] args) {
        SimulationBehavior.setMessageQueueSelectionDistributionFunction(
                RandomValues.getUniformDistribution());
        SimulationBehavior.setMessageLatencyDistributionFunction(
                RandomValues.getUniformDistribution(),
                MIN_MESSAGE_LATENCY_MILLIS,
                MAX_MESSAGE_LATENCY_MILLIS);

        Simulator simulator = Simulator.getInstance();
        List<String> nodeNames = nodeNames(NODE_COUNT);

        for (String nodeName : nodeNames) {
            new BankNode(nodeName, INITIAL_BALANCE, nodeNames);
        }
        new SnapshotCoordinator(
                SnapshotCoordinator.NODE_NAME,
                nodeNames,
                2_000,
                NAIVE_SNAPSHOT_COUNT,
                NAIVE_SNAPSHOT_INTERVAL_MILLIS,
                SNAPSHOT_CHANNEL_DRAIN_MILLIS,
                NODE_COUNT * INITIAL_BALANCE);

        simulator.simulate(SIMULATION_SECONDS);
        simulator.shutdown();
    }

    private static List<String> nodeNames(int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add("P" + i);
        }
        return names;
    }
}
