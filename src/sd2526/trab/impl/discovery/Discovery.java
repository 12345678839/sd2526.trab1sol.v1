package sd2526.trab.impl.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import sd2526.trab.impl.utils.Sleep;

public interface Discovery {

	public void announce(String serviceName, String serviceURI);

	public URI[] knownUrisOf(String serviceName, int minReplies);

	public static Discovery getInstance() {
		return DiscoveryImpl.getInstance();
	}
}

class DiscoveryImpl implements Discovery {

	private static Logger Log = Logger.getLogger(Discovery.class.getName());

	static final int DISCOVERY_RETRY_TIMEOUT = 5000;
	static final int DISCOVERY_ANNOUNCE_PERIOD = 1000;

	private static final String MULTICAST_IP = System.getProperty("DISCOVERY_MULTICAST_IP", "226.226.226.226");
	private static final int MULTICAST_PORT = Integer.parseInt(System.getProperty("DISCOVERY_MULTICAST_PORT", "2266"));
	private static final InetSocketAddress DISCOVERY_ADDR = new InetSocketAddress(MULTICAST_IP, MULTICAST_PORT);

	private static final String DELIMITER = "\t";
	private static final int MAX_DATAGRAM_SIZE = 65536;

	private static Discovery singleton;

	private Map<String, Set<URI>> uris = new ConcurrentHashMap<>();

	synchronized static Discovery getInstance() {
		if (singleton == null) {
			singleton = new DiscoveryImpl();
		}
		return singleton;
	}

	private DiscoveryImpl() {
		this.startListener();
	}

	@Override
	public void announce(String serviceName, String serviceURI) {
		Log.info(String.format("Starting Discovery announcements on: %s for: %s -> %s\n", DISCOVERY_ADDR, serviceName,
				serviceURI));

		var pktBytes = String.format("%s%s%s", serviceName, DELIMITER, serviceURI).getBytes();
		var pkt = new DatagramPacket(pktBytes, pktBytes.length, DISCOVERY_ADDR);

		new Thread(() -> {
			try (var ds = new DatagramSocket()) {
				while (true) {
					try {
						ds.send(pkt);
						Sleep.ms(DISCOVERY_ANNOUNCE_PERIOD);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}

	@Override
	public URI[] knownUrisOf(String serviceName, int minEntries) {
		while (true) {
			var res = uris.getOrDefault(serviceName, Collections.emptySet());
			if (res.size() >= minEntries)
				return res.toArray(new URI[res.size()]);
			else
				Sleep.ms(DISCOVERY_ANNOUNCE_PERIOD);
		}
	}

	private void startListener() {
		Log.info(String.format("Starting discovery on multicast group: %s, port: %d\n", DISCOVERY_ADDR.getAddress(),
				DISCOVERY_ADDR.getPort()));

		new Thread(() -> {
			try (var ms = new MulticastSocket(DISCOVERY_ADDR.getPort())) {
				ms.joinGroup(DISCOVERY_ADDR, pickMulticastInterface(DISCOVERY_ADDR));
				for (;;) {
					try {
						var pkt = new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);
						ms.receive(pkt);
						var msg = new String(pkt.getData(), 0, pkt.getLength());
						Log.finest(String.format("Received: %s", msg));
						var parts = msg.split(DELIMITER);
						if (parts.length == 2) {
							var serviceName = parts[0];
							var uri = URI.create(parts[1]);
							uris.computeIfAbsent(serviceName, (k) -> ConcurrentHashMap.newKeySet()).add(uri);
						}
					} catch (Exception x) {
						x.printStackTrace();
					}
				}
			} catch (Exception x) {
				x.printStackTrace();
			}
		}).start();
	}

	private NetworkInterface pickMulticastInterface(InetSocketAddress group) throws IOException {
		var interfaces = NetworkInterface.getNetworkInterfaces();
		while (interfaces.hasMoreElements()) {
			var iface = interfaces.nextElement();
			if (!iface.isLoopback() && iface.isUp() && iface.supportsMulticast()) {
				return iface;
			}
		}
		return NetworkInterface.getByInetAddress(java.net.InetAddress.getLocalHost());
	}
}