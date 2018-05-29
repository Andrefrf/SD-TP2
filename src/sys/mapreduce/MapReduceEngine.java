package sys.mapreduce;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Lists;

public class MapReduceEngine {

	final String worker;
	final BlobStorage storage;
	
	public MapReduceEngine( String worker, BlobStorage storage) {
		this.worker = worker;
		this.storage = storage;
	}
	
	public void executeJob( String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionSize ) {
		new MapperTask(worker, storage, jobClassBlob, inputPrefix, outputPrefix).execute();
		
		Set<String> reduceKeyPrefixes = storage.listBlobs(outputPrefix + "-map-").stream()
			.map( blob -> blob.substring( 0, blob.lastIndexOf('-')))
			.collect( Collectors.toSet() );
		
		
		AtomicInteger partitionCounter = new AtomicInteger(0);
		Lists.partition( new ArrayList<>( reduceKeyPrefixes), outputPartitionSize).forEach(partitionKeyList -> {
				
				String partitionOutputBlob = String.format("%s-part%04d", outputPrefix, partitionCounter.incrementAndGet());
				
				BlobWriter writer = storage.blobWriter(partitionOutputBlob);

				partitionKeyList.forEach( keyPrefix -> {
					new ReducerTask("client", storage, jobClassBlob, keyPrefix, outputPrefix).execute(writer);			
				});			
				
				writer.close();
			});
	}
}
