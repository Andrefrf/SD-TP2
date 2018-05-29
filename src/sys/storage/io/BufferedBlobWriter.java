package sys.storage.io;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import api.storage.BlobStorage.BlobWriter;
import api.storage.Datanode;
import api.storage.Namenode;
import utils.IO;

/*
 * 
 * Implements a ***centralized*** BlobWriter.
 * 
 * Accumulates lines in a list of blocks, avoids splitting a line across blocks.
 * When the BlobWriter is closed, the Blob (and its blocks) is published in the Namenode.
 * 
 */
public class BufferedBlobWriter implements BlobWriter {
	final String name;
	final int blockSize;
	final ByteArrayOutputStream buf;

	final Namenode namenode; 
	final Map<String, Datanode> datanodes;
	final List<Datanode> datanodesList;
	final List<String> blocks = new LinkedList<>();
	
	private final static Random gen = new Random();
	
	public BufferedBlobWriter(String name, Namenode namenode, Map<String, Datanode> datanodes, int blockSize ) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = datanodes;
		this.datanodesList = new ArrayList<>(datanodes.values());
		this.blockSize = blockSize;
		this.buf = new ByteArrayOutputStream( blockSize );
	}

	private void flush( byte[] data, boolean eob ) {
		int idx = gen.nextInt(datanodes.size());
		blocks.add( datanodesList.get(idx).createBlock(data)  );
		if( eob ) {
			namenode.create(name, blocks);
			blocks.clear();
		}
	}

	@Override
	public void writeLine(String line) {
		if( buf.size() + line.length() > blockSize - 1 ) {
			this.flush(buf.toByteArray(), false);
			buf.reset();
		}
		IO.write( buf, line.getBytes() );
		IO.write( buf, '\n');
	}

	@Override
	public void close() {
		flush( buf.toByteArray(), true );
	}
}