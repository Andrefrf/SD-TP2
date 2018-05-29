package utils;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import api.storage.Datanode;

public class ServiceDiscovery {

	public final static String DATANODE_SERVICE_NAME = "Datanode";
	public final static String NAMENODE_SERVICE_NAME = "Namenode";
	public final static int TIMEOUT = 2000;

	private static Logger logger = Logger.getLogger(Datanode.class.toString());

	public static final String multicastAddress = "224.10.10.10";
	public static final int multicastPort = 6665;
	public static final int MAX_DATAGRAM_SIZE = 65536;

	public static String[] multicastSend(String serviceName) {

		List<String> names = new ArrayList<>();

		System.setProperty("java.net.preferIPv4Stack", "true");
		try (MulticastSocket socket = new MulticastSocket()) {
			DatagramPacket request = new DatagramPacket(serviceName.getBytes(), serviceName.length(),
					InetAddress.getByName(multicastAddress), multicastPort);
			socket.send(request);
			byte[] buffer = new byte[MAX_DATAGRAM_SIZE];
			DatagramPacket response = new DatagramPacket(buffer, MAX_DATAGRAM_SIZE);

			socket.setSoTimeout(TIMEOUT);
			while (true) {
				try {
					socket.receive(response);
					String webServiceAddress = new String(response.getData(), 0, response.getLength());
					logger.info("Discovered " + webServiceAddress);
					names.add(webServiceAddress);
				} catch (SocketTimeoutException e) {
					break;
				}
			}
			if(names.size() > 0)
				return names.toArray(new String[] {});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void multicastReceive(String serviceName, String serviceAddress) {
		System.setProperty("java.net.preferIPv4Stack", "true");
		InetAddress group = null;
		try {
			group = InetAddress.getByName(multicastAddress);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(1);
		}

		if (!group.isMulticastAddress()) {
			System.out.println("Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
			System.exit(1);
		}

		// multicast receive
		try (MulticastSocket socket = new MulticastSocket(multicastPort)) {
			socket.joinGroup(group);
			byte[] buffer = new byte[MAX_DATAGRAM_SIZE];
			byte[] reply = serviceAddress.getBytes();
			while (true) {
				try {			
					DatagramPacket request = new DatagramPacket(buffer, buffer.length);
					socket.receive(request);
					String service = new String(request.getData(), 0, request.getLength());
					if (serviceName.equals(service)) {
						System.out.println(request.getPort());
						DatagramPacket response = new DatagramPacket(reply, reply.length,
								request.getAddress(), request.getPort());
						socket.send(response);
					}
				} catch (Exception e) {
					continue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}