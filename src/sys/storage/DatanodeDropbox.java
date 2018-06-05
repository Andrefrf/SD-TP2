package sys.storage;


import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import utils.IP;
import utils.ServiceDiscovery;

import javax.net.ssl.SSLContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class DatanodeDropbox {

    private static final String DATANODE_PORT_DEFAULT = "55555";

    public static void main(String[] args) throws NoSuchAlgorithmException {
        System.setProperty("java.net.preferIPv4Stack", "true");

        String port = DATANODE_PORT_DEFAULT;
        if (args.length > 0 && args[0] != null) {
            port = args[0];
        }
        String URI_BASE = "https://0.0.0.0:" + port + "/";
        ResourceConfig config = new ResourceConfig();
        String myAddress = "https://" + IP.hostAddress() + ":" + port;

        config.register(new DatanodeDropboxResources("oPYGpkJu1cUAAAAAAAAH7asWaP03G5FalUWGcObNPnOzRLudXMcCawfxJofwYgFu", myAddress));

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

		public static void start(OAuthCallbackServlet oAuthCallbackServlet) {
			ResourceConfig config = new ResourceConfig();
			config.register(new OAuthCallbackServlet());
			JdkHttpServerFactory.createHttpServer(URI.create(CALLBACK_URI), config);
		}
	}
}
