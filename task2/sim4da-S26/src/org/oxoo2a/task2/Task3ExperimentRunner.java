package org.oxoo2a.task2;

import org.oxoo2a.sim4da.RandomValues;
import org.oxoo2a.sim4da.SimulationBehavior;
import org.oxoo2a.sim4da.Simulator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class Task3ExperimentRunner {
    private static final int INITIAL_BALANCE = 1_000;
    private static final int SIMULATION_SECONDS = 4;
    private static final long MIN_MESSAGE_LATENCY_MILLIS = 50;
    private static final long MAX_MESSAGE_LATENCY_MILLIS = 200;
    private static final long SNAPSHOT_START_DELAY_MILLIS = 500;
    private static final int NAIVE_SNAPSHOT_COUNT = 3;
    private static final long NAIVE_SNAPSHOT_INTERVAL_MILLIS = 350;
    private static final long SNAPSHOT_CHANNEL_DRAIN_MILLIS =
            MAX_MESSAGE_LATENCY_MILLIS * 3 + 150;

    private Task3ExperimentRunner() {
    }

    public static void main(String[] args) throws IOException {
        List<Integer> nodeCounts = List.of(3, 5, 8, 16);
        List<FrequencyProfile> frequencyProfiles = List.of(
                new FrequencyProfile("fast", 30, 90),
                new FrequencyProfile("medium", 100, 250),
                new FrequencyProfile("slow", 250, 500));

        List<ExperimentRow> rows = new ArrayList<>();
        for (int nodeCount : nodeCounts) {
            for (FrequencyProfile profile : frequencyProfiles) {
                rows.add(runOnce(nodeCount, profile));
            }
        }

        Path resultsDir = task2Directory().resolve("results");
        Files.createDirectories(resultsDir);
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        Path csv = resultsDir.resolve("task3-snapshot-experiment-" + timestamp + ".csv");

        Files.writeString(csv, toCsv(rows));
        System.out.println("Wrote " + csv.toAbsolutePath().normalize());
    }

    private static Path task2Directory() {
        Path current = Path.of("").toAbsolutePath().normalize();
        if (Files.isDirectory(current.resolve("task2"))) {
            return current.resolve("task2");
        }

        Path cursor = current;
        while (cursor != null) {
            if ("task2".equals(cursor.getFileName().toString())) {
                return cursor;
            }
            cursor = cursor.getParent();
        }

        throw new IllegalStateException(
                "Could not locate task2 directory from working directory " + current);
    }

    private static ExperimentRow runOnce(int nodeCount, FrequencyProfile profile) {
        SimulationBehavior.setMessageQueueSelectionDistributionFunction(
                RandomValues.getUniformDistribution());
        SimulationBehavior.setMessageLatencyDistributionFunction(
                RandomValues.getUniformDistribution(),
                MIN_MESSAGE_LATENCY_MILLIS,
                MAX_MESSAGE_LATENCY_MILLIS);

        Simulator simulator = Simulator.getInstance();
        simulator.disableLogging();

        List<String> nodeNames = nodeNames(nodeCount);
        for (String nodeName : nodeNames) {
            new BankNode(
                    nodeName,
                    INITIAL_BALANCE,
                    nodeNames,
                    profile.minWaitMillis(),
                    profile.maxWaitMillis());
        }

        SnapshotCoordinator coordinator = new SnapshotCoordinator(
                SnapshotCoordinator.NODE_NAME,
                nodeNames,
                SNAPSHOT_START_DELAY_MILLIS,
                NAIVE_SNAPSHOT_COUNT,
                NAIVE_SNAPSHOT_INTERVAL_MILLIS,
                SNAPSHOT_CHANNEL_DRAIN_MILLIS,
                nodeCount * INITIAL_BALANCE);

        simulator.simulate(SIMULATION_SECONDS);

        List<SnapshotCoordinator.NaiveSnapshotResult> naiveResults = coordinator.naiveResults();
        SnapshotCoordinator.ColoringSnapshotResult coloringResult = coordinator.coloringResult();
        simulator.shutdown();

        int naiveInconsistent = (int) naiveResults.stream()
                .filter(result -> !result.consistent())
                .count();
        double naiveAverageAbsError = naiveResults.stream()
                .mapToInt(result -> Math.abs(result.expectedTotal() - result.accountSum()))
                .average()
                .orElse(0.0);

        return new ExperimentRow(
                nodeCount,
                profile.name(),
                profile.minWaitMillis(),
                profile.maxWaitMillis(),
                naiveResults.size(),
                naiveInconsistent,
                naiveAverageAbsError,
                coloringResult != null && coloringResult.consistent(),
                coloringResult == null ? -1 : coloringResult.channelSum(),
                coloringResult == null ? -1 : coloringResult.controlMessages(),
                naiveResults.isEmpty() ? -1 : naiveResults.getFirst().controlMessages());
    }

    private static List<String> nodeNames(int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add("P" + i);
        }
        return names;
    }

    private static String toCsv(List<ExperimentRow> rows) {
        StringBuilder csv = new StringBuilder();
        csv.append("node_count,frequency,min_wait_ms,max_wait_ms,naive_snapshots,")
                .append("naive_inconsistent,naive_average_abs_error,")
                .append("coloring_consistent,coloring_channel_sum,")
                .append("coloring_control_messages,naive_control_messages\n");
        for (ExperimentRow row : rows) {
            csv.append(row.nodeCount()).append(',')
                    .append(row.frequency()).append(',')
                    .append(row.minWaitMillis()).append(',')
                    .append(row.maxWaitMillis()).append(',')
                    .append(row.naiveSnapshots()).append(',')
                    .append(row.naiveInconsistent()).append(',')
                    .append(String.format(Locale.ROOT, "%.2f", row.naiveAverageAbsError())).append(',')
                    .append(row.coloringConsistent()).append(',')
                    .append(row.coloringChannelSum()).append(',')
                    .append(row.coloringControlMessages()).append(',')
                    .append(row.naiveControlMessages()).append('\n');
        }
        return csv.toString();
    }

    private record FrequencyProfile(String name, int minWaitMillis, int maxWaitMillis) {
    }

    private record ExperimentRow(
            int nodeCount,
            String frequency,
            int minWaitMillis,
            int maxWaitMillis,
            int naiveSnapshots,
            int naiveInconsistent,
            double naiveAverageAbsError,
            boolean coloringConsistent,
            int coloringChannelSum,
            int coloringControlMessages,
            int naiveControlMessages
    ) {
    }
}
