package sys.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.bson.Document;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import api.storage.Datanode;
import api.storage.Namenode;
import utils.IP;
import utils.ServiceDiscovery;

public class NamenodeRest implements Namenode {

	private static final String NAMENODE_PORT_DEFAULT = "9981";

	private static Logger logger = Logger.getLogger(NamenodeClient.class.toString());

	// Trie<String, List<String>> names = new PatriciaTrie<>();

	static MongoCollection<Document> table;

	Map<String, Datanode> datanodes;

	Set<String> registeredBlocks;

	public NamenodeRest() {
		registeredBlocks = new ConcurrentSkipListSet<String>();

		datanodes = new ConcurrentHashMap<String, Datanode>();
		Thread dataNodeDiscovery = new Thread() {
			public void run() {
				while (true) {
					String[] datanodeNames = ServiceDiscovery.multicastSend(ServiceDiscovery.DATANODE_SERVICE_NAME);
					if (datanodeNames != null) {
						for (String datanode : datanodeNames) {
							if (!datanodes.containsKey(datanode)) {
								logger.info("New Datanode discovered: " + datanode);
								datanodes.put(datanode, new DatanodeClient(datanode));
							}
						}
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// nothing to be done
					}
				}
			}
		};
		dataNodeDiscovery.start();

	}

	@Override
	public synchronized List<String> list(String prefix) {
		return new ArrayList<>();
	}

	@Override
	public synchronized void create(String name, List<String> blocks) {
		Document doc = new Document();
		doc.put("Name", name);
		doc.put("Blocks", blocks);
		Document serch = new Document();
		serch.put("Name", name);
		for (Document d : table.find(serch)) {
			if (d != null) {
				logger.info("Namenode create CONFLICT");
				throw new WebApplicationException(Status.CONFLICT);
			}
		}
		table.insertOne(doc);
	}

	@Override
	public synchronized void delete(String prefix) {
		Pattern regex = Pattern.compile(prefix);
		Iterable<Document> docs = table.find(Filters.eq(regex));
		if (docs != null) {
			table.deleteMany(Filters.eq(regex));
		} else {
			logger.info("Namenode delete NOT FOUND");
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

	@Override
	public synchronized void update(String name, List<String> blocks) {
		if (table.find(Filters.eq("Name", name)) != null) {
			Document doc = new Document();
			doc.put("Name", name);
			doc.put("Blocks", blocks);
			table.updateOne(Filters.eq("Name", name), doc);
		} else {
			logger.info("Namenode update NOT FOUND");
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

	@Override
	public synchronized List<String> read(String name) {
		Document doc = table.find(Filters.eq("Name", name)).first();
		return null;
	}

	public static void main(String[] args) throws UnknownHostException, URISyntaxException, NoSuchAlgorithmException {

		MongoClientURI uri = new MongoClientURI("mongodb://mongo1,mongo2,mongo3/?w=2&readPreference=secondary");
		try (MongoClient mongo = new MongoClient(uri)) {

			MongoDatabase db = mongo.getDatabase("testDB");

			table = db.getCollection("col");
		}

		System.setProperty("java.net.preferIPv4Stack", "true");
		String port = NAMENODE_PORT_DEFAULT;
		if (args.length > 0 && args[0] != null) {
			port = args[0];
		}
		String URI_BASE = "https://0.0.0.0:" + port + "/";
		String myAddress = "https://" + IP.hostAddress() + ":" + port;
		ResourceConfig config = new ResourceConfig();
		config.register(new NamenodeRest());
		JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config, SSLContext.getDefault());

		System.err.println("Namenode ready....");
		ServiceDiscovery.multicastReceive(ServiceDiscovery.NAMENODE_SERVICE_NAME, myAddress + "/");
	}

}
