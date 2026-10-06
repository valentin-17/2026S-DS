package org.oxoo2a.task2;

import org.oxoo2a.sim4da.Node;
import org.oxoo2a.sim4da.ReceivedMessage;

import java.util.List;
import java.util.Random;

public class BankNode extends Node {
    private static final String COORDINATOR_NODE = "Coordinator";

    private final List<String> peers;
    private final Random random;
    private final int minTransferWaitMillis;
    private final int maxTransferWaitMillis;
    private int balance;
    private BankMessages.SnapshotColor color = BankMessages.SnapshotColor.BLACK;
    private String activeSnapshotId = null;
    private Integer recordedBalance = null;

    public BankNode(String name, int initialBalance, List<String> allNodeNames) {
        this(name, initialBalance, allNodeNames, 100, 500);
    }

    public BankNode(
            String name,
            int initialBalance,
            List<String> allNodeNames,
            int minTransferWaitMillis,
            int maxTransferWaitMillis
    ) {
        super(name);
        this.balance = initialBalance;
        this.peers = allNodeNames.stream()
                .filter(nodeName -> !nodeName.equals(name))
                .toList();
        this.random = new Random(name.hashCode());
        this.minTransferWaitMillis = minTransferWaitMillis;
        this.maxTransferWaitMillis = maxTransferWaitMillis;
    }

    @Override
    protected void engage() {
        Thread sender = Thread.ofVirtual().start(this::transferLoop);

        while (true) {
            ReceivedMessage received = receive();
            if (received == null) {
                sender.interrupt();
                return;
            }

            switch (received.message()) {
                case BankMessages.Transfer transfer -> onTransfer(transfer, received.sender());
                case BankMessages.StartSnapshot start -> onStartSnapshot(start, received.sender());
                case BankMessages.Marker marker -> onMarker(marker, received.sender());
                case BankMessages.NaiveSnapshotRequest request -> onNaiveSnapshotRequest(request, received.sender());
                default -> throw new IllegalStateException("Unexpected message: " + received.message());
            }
        }
    }

    private void transferLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            sleep(randomWaitMillis());
            sendRandomTransfer();
        }
    }

    private synchronized void sendRandomTransfer() {
        if (balance <= 0 || peers.isEmpty()) {
            return;
        }

        int amount = random.nextInt(balance) + 1;
        String receiver = randomPeer();
        BankMessages.SnapshotColor sendColor = color;
        String sendSnapshotId = activeSnapshotId;

        balance -= amount;
        send(new BankMessages.Transfer(amount, sendColor, sendSnapshotId), receiver);
        log(String.format("sent %s transfer %d to %s, balance=%d",
                sendColor, amount, receiver, balance));
    }

    private synchronized String randomPeer() {
        return peers.get(random.nextInt(peers.size()));
    }

    private synchronized int randomWaitMillis() {
        return random.nextInt(minTransferWaitMillis, maxTransferWaitMillis + 1);
    }

    private synchronized int currentBalance() {
        return balance;
    }

    private void onTransfer(BankMessages.Transfer transfer, String sender) {
        SnapshotReport localReport = null;
        BankMessages.ChannelState channelState = null;

        synchronized (this) {
            if (transfer.color() == BankMessages.SnapshotColor.WHITE
                    && color == BankMessages.SnapshotColor.BLACK) {
                localReport = turnWhiteLocked(transfer.snapshotId());
            }

            if (transfer.color() == BankMessages.SnapshotColor.BLACK
                    && color == BankMessages.SnapshotColor.WHITE) {
                channelState = new BankMessages.ChannelState(
                        activeSnapshotId,
                        sender,
                        nodeName(),
                        transfer.amount());
            }

            balance += transfer.amount();
        }

        if (localReport != null) {
            sendLocalState(localReport);
        }
        if (channelState != null) {
            send(channelState, COORDINATOR_NODE);
            log(String.format("recorded in-transit transfer %d from %s to %s for snapshot %s",
                    channelState.amount(), channelState.from(), channelState.to(), channelState.snapshotId()));
        }

        log(String.format("%s received %s transfer %d from %s, balance=%d",
                this.nodeName(), transfer.color(), transfer.amount(), sender, currentBalance()));
    }

    private void onStartSnapshot(BankMessages.StartSnapshot start, String sender) {
        if (start.color() != BankMessages.SnapshotColor.WHITE) {
            throw new IllegalArgumentException("Only WHITE snapshot starts are supported");
        }

        SnapshotReport localReport;
        synchronized (this) {
            if (color == BankMessages.SnapshotColor.WHITE) {
                return;
            }
            localReport = turnWhiteLocked(start.snapshotId());
        }

        sendLocalState(localReport);
        log(String.format("started snapshot %s requested by %s, recorded balance=%d",
                start.snapshotId(), sender, localReport.balance()));
    }

    private void onMarker(BankMessages.Marker marker, String sender) {
        throw new UnsupportedOperationException(
                "Chandy-Lamport markers are intentionally not used for the coloring procedure");
    }

    private void onNaiveSnapshotRequest(BankMessages.NaiveSnapshotRequest request, String sender) {
        send(new BankMessages.NaiveSnapshotReply(request.snapshotId(), currentBalance()), sender);
        log(String.format("reported naive snapshot %s balance=%d to %s",
                request.snapshotId(), currentBalance(), sender));
    }

    private SnapshotReport turnWhiteLocked(String snapshotId) {
        if (snapshotId == null) {
            throw new IllegalArgumentException("Snapshot id must not be null");
        }
        color = BankMessages.SnapshotColor.WHITE;
        activeSnapshotId = snapshotId;
        recordedBalance = balance;
        return new SnapshotReport(snapshotId, recordedBalance);
    }

    private void sendLocalState(SnapshotReport report) {
        send(new BankMessages.LocalState(report.snapshotId(), report.balance()), COORDINATOR_NODE);
    }

    private record SnapshotReport(String snapshotId, int balance) {
    }
}
