package org.oxoo2a.task2;

import org.oxoo2a.sim4da.Message;

public final class BankMessages {
    private BankMessages() {
    }

    public enum SnapshotColor {
        WHITE,
        BLACK
    }

    public record Transfer(int amount, SnapshotColor color, String snapshotId) implements Message {
        public Transfer(int amount) {
            this(amount, SnapshotColor.BLACK, null);
        }

        public Transfer {
            if (amount <= 0) {
                throw new IllegalArgumentException("Transfer amount must be positive");
            }
            if (color == null) {
                throw new IllegalArgumentException("Transfer color must not be null");
            }
            if (color == SnapshotColor.WHITE && snapshotId == null) {
                throw new IllegalArgumentException("White transfers must carry a snapshot id");
            }
        }
    }

    public record StartSnapshot(String snapshotId, SnapshotColor color) implements Message {
        public StartSnapshot(String snapshotId) {
            this(snapshotId, SnapshotColor.WHITE);
        }
    }

    public record Marker(String snapshotId) implements Message {
    }

    public record LocalState(String snapshotId, int balance) implements Message {
    }

    public record ChannelState(String snapshotId, String from, String to, int amount) implements Message {
    }

    public record NaiveSnapshotRequest(String snapshotId) implements Message {
    }

    public record NaiveSnapshotReply(String snapshotId, int balance) implements Message {
    }
}
