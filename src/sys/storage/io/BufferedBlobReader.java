package sys.storage.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import api.storage.BlobStorage.BlobReader;
import api.storage.Datanode;
import api.storage.Namenode;
import utils.MapReduceCommon;

/*
 * 
 * Implements BlobReader.
 * 
 * Allows reading or iterating the lines of Blob one at a time, by fetching each block on demand.
 * 
 * Intended to allow streaming the lines of a large blob (with mamy (large) blocks) without reading it all first into memory.
 */
public class BufferedBlobReader implements BlobReader {

	final String name;
	final Namenode namenode; 
	final Map<String, Datanode> datanodes;
	
	final Iterator<String> blocks;

	final LazyBlockReader lazyBlockIterator;
	
	private static Logger logger = Logger.getLogger(BufferedBlobReader.class.getName());
	
	public BufferedBlobReader( String name, Namenode namenode, Map<String, Datanode> datanodes ) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = datanodes;
		
		this.blocks = this.namenode.read( name ).iterator();
		this.lazyBlockIterator = new LazyBlockReader();
	}
	
	@Override
	public String readLine() {		
		String ret = lazyBlockIterator.hasNext() ? lazyBlockIterator.next() : null ;
		logger.info("readLine(): Ret -> " + ret );
		return ret;
	}
	
	@Override
	public Iterator<String> iterator() {
		return lazyBlockIterator;
	}
	
	private Iterator<String> nextBlockLines() {
		if( blocks.hasNext() )
			return fetchBlockLines( blocks.next() ).iterator();
		else 
			return Collections.emptyIterator();
	} 

	private List<String> fetchBlockLines(String block) {
		logger.info("accessing block on URL: " + block);
		try { 
		String[] components = MapReduceCommon.getAddressFromBlockUUID(block);
		Datanode datanode = datanodes.get(components[0]);
		if(datanode == null) 
			logger.info("Unknown datanode: " + components[0]);
		logger.info("Known datanodes:");
		for(String dn: datanodes.keySet()) {
			logger.info(dn);
		}
		logger.info("Trying to access target block: " + components[1]);
		byte[] data = datanode.readBlock( components[1] );
		return Arrays.asList( new String(data).split("\\R"));
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<String>();
		}
	}
	
	private class LazyBlockReader implements Iterator<String> {
		
		Iterator<String> lines;
		
		LazyBlockReader() {
			this.lines = nextBlockLines();
		}
		
		@Override
		public String next() {
			return lines.next();
		}

		@Override
		public boolean hasNext() {
			return lines.hasNext() || (lines = nextBlockLines()).hasNext();
		}	
	}
}

