package sys.mapreduce;

import java.util.Iterator;
import java.util.stream.Collectors;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Iterators;
import sys.mapreduce.Jobs.JobInstance;
import utils.Base58;
import utils.JSON;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ReducerTask extends MapReduceTask {

	protected ReducerTask(String worker, BlobStorage storage, String jobClassBlob, String inputPrefix, String outputPrefix) {
		super(worker, storage, jobClassBlob, inputPrefix, outputPrefix);

	}
	
	void execute( BlobWriter writer ) {
		JobInstance job = Jobs.newJobInstance(storage, jobClassBlob);

		String reduceKey = inputPrefix.substring( inputPrefix.lastIndexOf('-')+1);
		
		job.instance.setYielder((k, v) -> writer.writeLine(k + "\t" + v));
		job.instance.reduce_init();

		Object key = JSON.decode(Base58.decode(reduceKey), job.reducerKeyType());
		
		Iterator<JsonDecoder> valuesIterators = storage.listBlobs( inputPrefix ).stream()
			.map( name -> new JsonDecoder(storage.readBlob(name).iterator(), job.reducerValueType()))
			.collect( Collectors.toList() ).iterator();
		
		job.instance.reduce(key, () -> Iterators.concat(valuesIterators));
		job.instance.reduce_end();

		storage.deleteBlobs(inputPrefix);
	}
}
