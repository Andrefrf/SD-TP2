package sys.mapreduce;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import utils.Base58;
import utils.JSON;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MapperTask extends MapReduceTask {

	private final String MapOutputBlobNameFormat;
	
	protected MapperTask(String worker, BlobStorage storage, String jobClassBlob, String inputPrefix , String outputPrefix ) {		
		super( worker, storage, jobClassBlob, inputPrefix, outputPrefix );		
		MapOutputBlobNameFormat = outputPrefix + "-map-%s-" + worker;
	}
	
	public void execute() {
		MapReducer job = Jobs.newJobInstance(storage, jobClassBlob).instance;
		
		job.setYielder( (key,val) -> jsonValueWriterFor( key ).write(val));
		
		job.map_init();					
		storage.listBlobs( inputPrefix ).stream()
			.forEach( blob -> {
					storage.readBlob( blob ).forEach( line -> job.map( blob, line ) );
			});
		job.map_end();
		
		writers.values().forEach( JsonBlobWriter::close );					
		writers.clear();
	}

	private JsonBlobWriter jsonValueWriterFor(Object key ) {
		JsonBlobWriter res = writers.get( key );
		if( res == null ) {			
			String b58key = Base58.encode( JSON.encode( key ) );			
			BlobWriter out = storage.blobWriter( String.format(MapOutputBlobNameFormat, b58key));
			writers.put(key,  new JsonBlobWriter(out));
		}
		return writers.get(key);
	}	
	private Map<Object, JsonBlobWriter> writers = new ConcurrentHashMap<>();
}
