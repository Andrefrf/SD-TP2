package sys.storage;

import java.net.URI;
import javax.net.ssl.SSLSession;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import api.storage.Datanode;
import utils.MapReduceCommon;

public class DatanodeClient implements Datanode {

	private static final long SLEEP_TIME = 1000;
	private static final int NUM_TRIES = 5;
	private static Logger logger = Logger.getLogger(DatanodeClient.class.getName());
	private Client client;
	private URI baseURI;

	public DatanodeClient(String location) {
		ClientConfig config = new ClientConfig();
		configureClinet(config);
		Client client = ClientBuilder.newBuilder().hostnameVerifier((String hostname, SSLSession cts) -> true).build();
		baseURI = UriBuilder.fromUri(location).build();
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

	@Override
	public String createBlock(byte[] data) {
		return processRequest(() -> {
			Response response = client.target(baseURI).path(Datanode.PATH).request()
					.post(Entity.entity(data, MediaType.APPLICATION_OCTET_STREAM));

			if (response.getStatus() == 200) {
				String id = response.readEntity(String.class);
				logger.info("stored block  wirh id: " + id);
				return id;
			} else
				logger.info("Impossivel to store data block: " + response.getStatus());
			return null;
		});

	}

	@Override
	public void deleteBlock(String block) {
		processRequest(() -> {
			Response response = client.target(baseURI).path(Datanode.PATH + "/" + block).request().delete();
			if (response.getStatusInfo().equals(Status.NO_CONTENT)) {
				logger.info("deleted data resource...");
			} else {
				logger.info("Delete block result: " + response.getStatus());
			}
			return null;
		});
	}

	@Override
	public byte[] readBlock(String block) {
		return processRequest(() -> {
			Response response = null;
			//if(block.startsWith("http://")) {
			//	logger.info(block);
			//	response = client.target(block).request().get();
			//} else {
				logger.info("GET reqeust to: " + baseURI + Datanode.PATH + "/" + block);
				response = client.target(baseURI).path(Datanode.PATH + "/" + block).request().get();
			//}
			if (response.getStatus() == 200) {
				byte[] data = response.readEntity(byte[].class);
				logger.info("data resource length: " + data.length);
				return data;
			} else
				logger.info("Read block result: " + response.getStatus());
			return null;
		});
	}

}
