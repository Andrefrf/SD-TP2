package sys.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;
import utils.MapReduceCommon;
import utils.ServiceDiscovery;

public class RemoteBlobStorage implements BlobStorage {
	private static final int BLOCK_SIZE = 512;

	private static Logger logger = Logger.getLogger(RemoteBlobStorage.class.getName());

	Namenode namenode;
	Map<String, Datanode> datanodes;

	public RemoteBlobStorage() {
		datanodes = new ConcurrentHashMap<String,Datanode>();
		Thread dataNodeDiscovery = new Thread() {
			public void run() {
				while(true) {
					String[] datanodeNames = ServiceDiscovery.multicastSend(ServiceDiscovery.DATANODE_SERVICE_NAME);
					if(datanodeNames != null) {
						for(String datanode: datanodeNames) {
							if(!datanodes.containsKey(datanode)) {
								logger.info("New Datanode discovered: " + datanode);
								datanodes.put(datanode, new DatanodeClient(datanode));
							}
						}
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						//nothing to be done
					}
				}
			}
		};
		dataNodeDiscovery.start();

		while(true) {
			String[] namenodename = ServiceDiscovery.multicastSend(ServiceDiscovery.NAMENODE_SERVICE_NAME);
			if(namenodename != null) {
				logger.info("Namenode discovered: " + namenodename[0]);
				this.namenode = new NamenodeClient(namenodename[0]);
			}
			break;
		}		
	}

	@Override
	public List<String> listBlobs(String prefix) {
		return namenode.list(prefix);
	}

	@Override
	public void deleteBlobs(String prefix) {
		namenode.list(prefix).forEach(blob -> {
			namenode.read(blob).forEach(block -> {
				String[] components = MapReduceCommon.getAddressFromBlockUUID(block);
				if(components != null && datanodes.containsKey(components[0])) {
					datanodes.get(components[0]).deleteBlock(components[1]);
				}
			});
		});
		namenode.delete(prefix);
	}


	@Override
	public BlobReader readBlob(String name) {
		logger.info("readBlob(" + name + ")");
		logger.info("Namenode is: " + ((NamenodeClient) namenode).address);
		for(String addr: datanodes.keySet()) {
			logger.info("Datanode: " + addr);
		}
		return new BufferedBlobReader(name, namenode, datanodes);
	}

	@Override
	public BlobWriter blobWriter(String name) {
		return new BufferedBlobWriter(name, namenode, datanodes, BLOCK_SIZE);
	}
}
