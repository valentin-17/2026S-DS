package org.oxoo2a.sim4da.internal;

import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.NetworkConnection;
import org.oxoo2a.sim4da.SimulationBehavior;
import org.oxoo2a.sim4da.UnknownNodeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Network {

    private Network() {
    }

    private record Node (NetworkConnection nc, NodeProxy np ) {}
    private final Map<String,Node> nodes = new HashMap<>();
    private static Network instance = null;
    public static Network getInstance() {
        if (instance == null) {
            synchronized (Network.class) {
                if (instance == null) {
                    instance = new Network();
                }
            }
        }
        return instance;
    }

    public void registerConnection(NetworkConnection networkConnection, NodeProxy nodeProxy) {
        networkConnection.log("registered with the network");
        Node n = new Node(networkConnection, nodeProxy);
        nodes.put(networkConnection.nodeName(), n);
    }

    public List<NetworkConnection> getAllNetworkConnections () {
        List<NetworkConnection> ncs = new ArrayList<>(numberOfNodes());
        for (Node n : nodes.values()) {
            ncs.add(n.nc);
        }
        return ncs;
    }

    public int numberOfNodes() {
        return nodes.size();
    }

    /**
     * True if the calling thread is one of the registered node threads.
     * Used to enforce that simulation-control methods (like stop) are not
     * called from inside an Actor's engage loop — a real distributed
     * system cannot be stopped by one node, only by message-based
     * propagation or by an external trigger.
     */
    public boolean isCurrentThreadANode() {
        return nodes.containsKey(Thread.currentThread().getName());
    }

    public void send (Message message, NetworkConnection sender, String receiverName ) throws UnknownNodeException {
        if (!nodes.containsKey(receiverName)) {
            sender.log("attempted send to non-existent node " + receiverName);
            throw new UnknownNodeException(receiverName);
        }
        MessageInTransit mit = new MessageInTransit(message, sender.nodeName());
        deliver(nodes.get(receiverName).np, mit);
    }

    public void send ( Message message, NetworkConnection sender ) {
        for (Node n : nodes.values()) {
            if (n.nc != sender) {
                MessageInTransit mit = new MessageInTransit(message, sender.nodeName());
                deliver(n.np, mit);
            }
        }
    }

    private void deliver(NodeProxy receiver, MessageInTransit mit) {
        long latencyMillis = SimulationBehavior.selectMessageLatencyMillis();
        if (latencyMillis <= 0) {
            receiver.deliver(mit);
            return;
        }

        Thread.ofVirtual()
                .name("sim4da-delivery-" + mit.sender())
                .start(() -> {
                    try {
                        Thread.sleep(latencyMillis);
                        receiver.deliver(mit);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
    }

    public MessageInTransit receive(NetworkConnection receiver) throws InterruptedException {
        return nodes.get(receiver.nodeName()).np.receive();
    }

    public void shutdown() {
        nodes.clear();
    }
}
