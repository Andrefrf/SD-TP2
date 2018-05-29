package sys.mapreduce;

import api.storage.BlobStorage.BlobWriter;
import utils.JSON;

/*
 * 
 * Wraps a BlobWriter to allow writing one json object per blob line.
 * 
 */
final public class JsonBlobWriter {
	
	private final BlobWriter out;
	
	JsonBlobWriter( BlobWriter out ) {
		this.out = out;
	}
	
	<T> void write( T obj ) {	
		out.writeLine( JSON.encode(obj) );
	}
		
	void close() {
		out.close();
	}
}
