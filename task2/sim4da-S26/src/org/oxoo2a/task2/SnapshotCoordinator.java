package org.oxoo2a.task2;

import org.oxoo2a.sim4da.NetworkConnection;
import org.oxoo2a.sim4da.ReceivedMessage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SnapshotCoordinator {
    public static final String NODE_NAME = "Coordinator";

    private final NetworkConnection nc;
    private final List<String> bankNodes;
    private final long startDelayMillis;
    private final int naiveSnapshotCount;
    private final long naiveSnapshotIntervalMillis;
    private final long channelDrainMillis;
    private final int expectedTotal;
    private final Map<String, Map<String, Integer>> naiveStates = new ConcurrentHashMap<>();
    private final List<NaiveSnapshotResult> naiveResults = new ArrayList<>();
    private final Map<String, Integer> localStates = new LinkedHashMap<>();
    private final List<BankMessages.ChannelState> channelStates = new ArrayList<>();
    private ColoringSnapshotResult coloringResult = null;
    private volatile boolean snapshotReady = false;
    private boolean drainScheduled = false;

    public SnapshotCoordinator(
            String name,
            List<String> bankNodes,
            long startDelayMillis,
            long channelDrainMillis,
            int expectedTotal
    ) {
        this(name, bankNodes, startDelayMillis, 3, 500, channelDrainMillis, expectedTotal);
    }

    public SnapshotCoordinator(
            String name,
            List<String> bankNodes,
            long startDelayMillis,
            int naiveSnapshotCount,
            long naiveSnapshotIntervalMillis,
            long channelDrainMillis,
            int expectedTotal
    ) {
        this.nc = new NetworkConnection(name);
        this.bankNodes = List.copyOf(bankNodes);
        this.startDelayMillis = startDelayMillis;
        this.naiveSnapshotCount = naiveSnapshotCount;
        this.naiveSnapshotIntervalMillis = naiveSnapshotIntervalMillis;
        this.channelDrainMillis = channelDrainMillis;
        this.expectedTotal = expectedTotal;
        this.nc.engage(this::run);
    }

    private void run() {
        String coloringSnapshotId = "coloring-1";
        Thread coordinatorThread = Thread.currentThread();

        nc.sleep(Math.toIntExact(startDelayMillis));
        for (int i = 1; i <= naiveSnapshotCount; i++) {
            startNaiveSnapshot("naive-" + i);
            nc.sleep(Math.toIntExact(naiveSnapshotIntervalMillis));
        }

        nc.log("starting coloring snapshot " + coloringSnapshotId + " for " + bankNodes.size() + " bank nodes");
        for (String bankNode : bankNodes) {
            nc.send(new BankMessages.StartSnapshot(coloringSnapshotId), bankNode);
        }

        while (true) {
            ReceivedMessage received = nc.receive();
            if (received == null) {
                if (snapshotReady) {
                    printColoringSnapshot(coloringSnapshotId);
                }
                return;
            }

            switch (received.message()) {
                case BankMessages.NaiveSnapshotReply naiveReply ->
                        onNaiveSnapshotReply(naiveReply, received.sender());
                case BankMessages.LocalState localState -> {
                    if (coloringSnapshotId.equals(localState.snapshotId())) {
                        localStates.put(received.sender(), localState.balance());
                        nc.log(String.format("received local state from %s: %d",
                                received.sender(), localState.balance()));
                        scheduleDrainWhenComplete(coordinatorThread);
                    }
                }
                case BankMessages.ChannelState channelState -> {
                    if (coloringSnapshotId.equals(channelState.snapshotId())) {
                        channelStates.add(channelState);
                        nc.log(String.format("received channel state %s -> %s: %d",
                                channelState.from(), channelState.to(), channelState.amount()));
                    }
                }
                default -> nc.log("ignoring unexpected coordinator message " + received.message());
            }
        }
    }

    private void startNaiveSnapshot(String snapshotId) {
        naiveStates.put(snapshotId, new ConcurrentHashMap<>());
        nc.log("starting naive snapshot " + snapshotId);
        for (String bankNode : bankNodes) {
            nc.send(new BankMessages.NaiveSnapshotRequest(snapshotId), bankNode);
        }
    }

    private void onNaiveSnapshotReply(BankMessages.NaiveSnapshotReply reply, String sender) {
        Map<String, Integer> states = naiveStates.get(reply.snapshotId());
        if (states == null) {
            return;
        }

        states.put(sender, reply.balance());
        if (states.size() == bankNodes.size()
                && naiveResults.stream().noneMatch(result -> result.snapshotId().equals(reply.snapshotId()))) {
            int accountSum = states.values().stream().mapToInt(Integer::intValue).sum();
            NaiveSnapshotResult result = new NaiveSnapshotResult(
                    reply.snapshotId(),
                    accountSum,
                    expectedTotal,
                    accountSum == expectedTotal,
                    bankNodes.size() * 2);
            naiveResults.add(result);
            printNaiveSnapshot(result);
        }
    }

    private void scheduleDrainWhenComplete(Thread coordinatorThread) {
        if (drainScheduled || localStates.size() < bankNodes.size()) {
            return;
        }

        drainScheduled = true;
        Thread.ofVirtual().start(() -> {
            nc.sleep(Math.toIntExact(channelDrainMillis));
            snapshotReady = true;
            coordinatorThread.interrupt();
        });
    }

    private void printNaiveSnapshot(NaiveSnapshotResult result) {
        System.out.printf("%n=== Naive snapshot %s ===%n", result.snapshotId());
        System.out.printf("Account sum only: %d%n", result.accountSum());
        System.out.printf("Expected total: %d%n", result.expectedTotal());
        System.out.printf("Consistent: %s%n", result.consistent());
    }

    private void printColoringSnapshot(String snapshotId) {
        int accountSum = 0;
        int channelSum = 0;

        System.out.printf("%n=== Coloring snapshot %s ===%n", snapshotId);
        System.out.println("Local states:");
        for (String bankNode : bankNodes) {
            Integer balance = localStates.get(bankNode);
            if (balance == null) {
                System.out.printf("  %s: missing%n", bankNode);
            } else {
                accountSum += balance;
                System.out.printf("  %s: balance=%d%n", bankNode, balance);
            }
        }

        System.out.println("Channel states:");
        if (channelStates.isEmpty()) {
            System.out.println("  none");
        } else {
            for (BankMessages.ChannelState channelState : channelStates) {
                channelSum += channelState.amount();
                System.out.printf("  %s -> %s: transfer=%d%n",
                        channelState.from(), channelState.to(), channelState.amount());
            }
        }

        int snapshotTotal = accountSum + channelSum;
        coloringResult = new ColoringSnapshotResult(
                snapshotId,
                accountSum,
                channelSum,
                snapshotTotal,
                expectedTotal,
                snapshotTotal == expectedTotal,
                bankNodes.size() * 2 + channelStates.size());
        System.out.printf("Account sum: %d%n", accountSum);
        System.out.printf("Channel sum: %d%n", channelSum);
        System.out.printf("Snapshot total: %d%n", snapshotTotal);
        System.out.printf("Expected total: %d%n", expectedTotal);
        System.out.printf("Consistent: %s%n", coloringResult.consistent());
        nc.log("completed coloring snapshot " + snapshotId + " with total " + snapshotTotal);
    }

    public List<NaiveSnapshotResult> naiveResults() {
        return List.copyOf(naiveResults);
    }

    public ColoringSnapshotResult coloringResult() {
        return coloringResult;
    }

    public record NaiveSnapshotResult(
            String snapshotId,
            int accountSum,
            int expectedTotal,
            boolean consistent,
            int controlMessages
    ) {
    }

    public record ColoringSnapshotResult(
            String snapshotId,
            int accountSum,
            int channelSum,
            int snapshotTotal,
            int expectedTotal,
            boolean consistent,
            int controlMessages
    ) {
    }
}
