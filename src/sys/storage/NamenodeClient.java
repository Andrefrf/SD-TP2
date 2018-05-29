package sys.storage;

import java.net.URI;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLSession;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import api.storage.Namenode;
import utils.MapReduceCommon;

public class NamenodeClient implements Namenode {

	private static final long SLEEP_TIME = 1000;
	private static final int NUM_TRIES = 5;
	private static Logger logger = Logger.getLogger(DatanodeClient.class.getName());
	private WebTarget target;

	public final String address; //This is here just for debug.
	
	public NamenodeClient(String location) {
		this.address = location;
		ClientConfig config = new ClientConfig();
		configureClinet(config);
		Client client = ClientBuilder.newBuilder().hostnameVerifier((String hostname, SSLSession cts) -> true).build();
		URI baseURI = UriBuilder.fromUri(location).build();
		target = client.target(baseURI);
	}

	private void configureClinet(ClientConfig config) {
		config.property(ClientProperties.CONNECT_TIMEOUT, MapReduceCommon.CONNECT_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, MapReduceCommon.READ_TIMEOUT);
	}

	private <T> T processRequest(RequestHandler<T> requestHandler) {
		for (int i = 0; i < NUM_TRIES; i++) {
			try {
				return requestHandler.execute();
			} catch (ProcessingException pe) {
				logger.log(Level.INFO, String.format("Error contacting server.... retry: %d", i));
				try {
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		logger.log(Level.WARNING, String.format("Aborted request!"));
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<String> list(String prefix) {
		return processRequest(() -> {
			Response response = target.path(Namenode.PATH + "/list/").queryParam("prefix", prefix).request().get();
			if (response.hasEntity()) {
				List<String> data = response.readEntity(List.class);
				return data;
			} else
				System.err.println("NamenodeClient List: " + response.getStatus());
			return null;
		});
	}

	@Override
	public void create(String name, List<String> blocks) {
		processRequest(() -> {
			Response response = target.path(Namenode.PATH + "/" + name).request()
					.post(Entity.entity(blocks, MediaType.APPLICATION_JSON));

			if (response.getStatusInfo().equals(Status.NO_CONTENT)) {
				System.out.println("Namenode create success: " + name);
			} else
				System.err.println("Namenode create " + response.getStatus() + ": " + name);
			return null;
		});
	}

	@Override
	public void delete(String prefix) {
		processRequest(() -> {
			Response response = target.path(Namenode.PATH + "/list/").queryParam("prefix", prefix).request().delete();

			if (response.getStatusInfo().equals(Status.NO_CONTENT)) {
				System.out.println("Namenode delete successfull: " + prefix);
			} else
				System.err.println("Namenode delete " + response.getStatus() + ": " + prefix);
			return null;
		});
	}

	@Override
	public void update(String name, List<String> blocks) {
		processRequest(() -> {
			Response response = target.path(Namenode.PATH + "/" + name).request()
					.put(Entity.entity(blocks, MediaType.APPLICATION_JSON));

			if (response.getStatusInfo().equals(Status.NO_CONTENT)) {
				System.out.println("Namenode update successfull: " + name);
			} else
				System.err.println("Namenode update " + response.getStatus() + ": " + name);
			return null;
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<String> read(String name) {
		return processRequest(() -> {
			Response response = target.path(Namenode.PATH + "/" + name).request().get();

			if (response.hasEntity()) {
				List<String> data = response.readEntity(List.class);
				return data;
			} else
				System.err.println("NamenodeClient read: " + response.getStatus());
			return null;
		});
	}
}
