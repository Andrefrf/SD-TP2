package sys.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import api.storage.Datanode;
import api.storage.Namenode;
import utils.IP;
import utils.ServiceDiscovery;
import org.apache.kafka.clients.producer.*;

public class NamenodeRest implements Namenode {

	private static final String NAMENODE_PORT_DEFAULT = "9981";

	private static Logger logger = Logger.getLogger(NamenodeClient.class.toString());

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
		Pattern regex = Pattern.compile("(^" + prefix + ")");
		ArrayList<String> res = new ArrayList<>();
		for (Document d : table.find(Filters.eq("_id", regex))) {
			String h = d.getString("_id");
			res.add(h);
			System.out.println(h);
		}
		return res;
	}

	@Override
	public synchronized void create(String name, List<String> blocks) {
		Document doc = new Document();
		doc.put("_id", name);
		doc.put("Blocks", blocks);
		try {
			
			table.insertOne(doc);
		} catch (Exception e) {
			throw new WebApplicationException(Status.CONFLICT);
		}
	}

	@Override
	public synchronized void delete(String prefix) {
		Pattern regex = Pattern.compile("(^" + prefix + ")");
		if (table.find(Filters.eq("_id", regex)).first() != null) {
			for (Document d : table.find(Filters.eq("_id", regex))) {
				table.findOneAndDelete(d);
			}
		} else {
			logger.info("Namenode delete NOT FOUND");
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

	@Override
	public synchronized void update(String name, List<String> blocks) {
		try {
			Document doc = new Document();
			doc.put("_id", name);
			doc.put("Blocks", blocks);
			table.findOneAndUpdate(Filters.eq("_id", name), doc);
		} catch (Exception e) {
			logger.info("Namenode update NOT FOUND");
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

	@Override
	public synchronized List<String> read(String name) {
		Document doc = table.find(Filters.eq("_id", name)).first();
		return (List<String>) doc.get("Blocks");
	}

	public static void main(String[] args) throws UnknownHostException, URISyntaxException, NoSuchAlgorithmException {
		Properties props = new Properties();

		// Localização dos servidores kafka (lista de máquinas + porto)
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");

		// Classe para serializar as chaves dos eventos (string)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		MongoClientURI uri = new MongoClientURI("mongodb://mongo1,mongo2,mongo3/?w=2&readPreference=secondary");
		try (MongoClient mongo = new MongoClient(uri)) {
			try (Producer<String, String> producer = new KafkaProducer<>(props)) {

				MongoDatabase db = mongo.getDatabase("testDB");

				table = db.getCollection("col");

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
	}

}