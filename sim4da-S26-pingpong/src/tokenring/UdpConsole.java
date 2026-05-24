package tokenring;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Minimal command-line driver for trying UDP locally.
 */
public final class UdpConsole {
    private UdpConsole() {}

    static void main(String[] args) throws IOException {
        if (args.length < 2) {
            printUsage();
            return;
        }

        switch (args[0]) {
            case "listen" -> listen(parsePort(args[1]));
            case "send" -> send(args);
            default -> printUsage();
        }
    }

    private static void listen(int localPort) throws IOException {
        try (UdpEndpoint endpoint = new UdpEndpoint(localPort)) {
            System.out.printf("Listening for one UDP datagram on port %d%n", endpoint.localPort());
            ReceivedDatagram datagram = endpoint.receiveText();
            System.out.printf("Received \"%s\" from %s%n", datagram.text(), datagram.sender());
        }
    }

    private static void send(String[] args) throws IOException {
        if (args.length < 5) {
            printUsage();
            return;
        }

        int localPort = parsePort(args[1]);
        String peerHost = args[2];
        int peerPort = parsePort(args[3]);
        String text = joinPayload(args);
        InetSocketAddress peer = new InetSocketAddress(peerHost, peerPort);

        try (UdpEndpoint endpoint = new UdpEndpoint(localPort)) {
            endpoint.sendText(text, peer);
            System.out.printf("Sent \"%s\" from port %d to %s%n", text, endpoint.localPort(), peer);
        }
    }

    private static int parsePort(String value) {
        return Integer.parseInt(value);
    }

    private static String joinPayload(String[] args) {
        StringBuilder text = new StringBuilder(args[4]);
        for (int i = 5; i < args.length; i++) {
            text.append(' ').append(args[i]);
        }
        return text.toString();
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  listen <localPort>");
        System.out.println("  send <localPort> <peerHost> <peerPort> <text>");
    }
}
