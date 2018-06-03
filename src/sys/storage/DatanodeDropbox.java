package sys.storage;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import api.storage.Datanode;
import utils.IP;
import utils.JSON;
import utils.ServiceDiscovery;

public class DatanodeDropbox implements Datanode {

	private static final String DATANODE_PORT_DEFAULT = "9991";

	private static final String JSON_CONTENT_TYPE = "application/json";

	private static final String CREATE_FILE_URL = "https://content.dropboxapi.com/2/files/upload";

	private static Logger logger = Logger.getLogger(Datanode.class.getName());

	private String address;
	
	private OAuth2AccessToken accessToken;
	private OAuth20Service service;

	public DatanodeDropbox() throws IOException, InterruptedException, ExecutionException {
		getAutorization();
	}

	private void getAutorization() throws IOException, InterruptedException, ExecutionException {

		OAuthCallbackServlet.start();

		service = new ServiceBuilder().apiKey("3a1fbzzm7k7igae").apiSecret("fo890efedbyi2s3")
				.callback(OAuthCallbackServlet.CALLBACK_URI).build(DropboxApi20.INSTANCE);

//		String authorizationURL = service.getAuthorizationUrl();
//		System.out.println("Open the following URL in a browser: " + authorizationURL);
//
//		String authorizationID;
//		try (Scanner sc = new Scanner(System.in)) {
//			System.out.print("Authorization code: ");
//			authorizationID = sc.nextLine();
//		}

		accessToken = new OAuth2AccessToken("oPYGpkJu1cUAAAAAAAAH7BF6opWzGPb0KD9Y5-zr4ZACp2xy6gcOVu1yJgO9QtQA");

	}

	@Override
	public String createBlock(byte[] data) {
		String blockId = null;
		OAuthRequest newBlock = new OAuthRequest(Verb.POST, CREATE_FILE_URL);
		newBlock.addHeader("Content-Type", JSON_CONTENT_TYPE);
		newBlock.addHeader("Dropbox-API-Arg", JSON.encode(new CreateFileArgs("/xpto")));
		newBlock.setPayload( data);

		service.signRequest(accessToken, newBlock);

		Response r;
		try {
			r = service.execute(newBlock);
			if(r.getCode()!= 200) {
				throw new RuntimeException(r.getMessage());
			}
			blockId = r.getMessage();
			System.out.println(r.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return address + PATH + "/" + blockId; // Returns the full url to access the block

	}

	@Override
	public void deleteBlock(String block) {

	}

	@Override
	public byte[] readBlock(String block) {
		logger.log(Level.FINE, String.format("Reading block with id %s.", block));
		byte[] data = BlockIO.readBlock(block); // Reads block from the disk (null if the file is not found)

		return data;
	}

	public static void main(String[] args) throws URISyntaxException, NoSuchAlgorithmException, IOException, InterruptedException, ExecutionException {
		System.setProperty("java.net.preferIPv4Stack", "true");

		String port = DATANODE_PORT_DEFAULT;
		if (args.length > 0 && args[0] != null) {
			port = args[0];
		}
		String URI_BASE = "https://0.0.0.0:" + port + "/Datanode";
		ResourceConfig config = new ResourceConfig();
		String myAddress = "https://" + IP.hostAddress() + ":" + port;
		config.register(new DatanodeDropbox());
		JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config, SSLContext.getDefault());

		System.err.println("Datanode ready....");
		ServiceDiscovery.multicastReceive(ServiceDiscovery.DATANODE_SERVICE_NAME, myAddress + "/");
	}

	@Path("/")
	public static class OAuthCallbackServlet {
		public static final String CALLBACK_URI = "http://localhost:5555/";

		@GET
		public String callback(@QueryParam("code") String code) {
			return String.format("<html>Authorization-Code: %s</html>", code);
		}

		public static void start() {
			ResourceConfig config = new ResourceConfig();
			config.register(new OAuthCallbackServlet());
			JdkHttpServerFactory.createHttpServer(URI.create(CALLBACK_URI), config);
		}
	}

}
