package sys.mapreduce;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Lists;
import sys.storage.RemoteBlobStorage;

@WebService(serviceName = ComputeNode.NAME, targetNamespace = ComputeNode.NAMESPACE, endpointInterface = ComputeNode.INTERFACE)
public class ComputeNodeImpl implements ComputeNode {

	private String worker;
	private BlobStorage storage;

	public ComputeNodeImpl(String worker, BlobStorage storage) {
		this.worker = worker;
		this.storage = storage;
	}

	@Override
	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize)
			throws InvalidArgumentException {
		//Test received parameters
		if(jobClassBlob == null || inputPrefix == null || outputPrefix == null)
			throw new InvalidArgumentException();
		
		new MapperTask(worker, storage, jobClassBlob, inputPrefix, outputPrefix).execute();

		Set<String> reduceKeyPrefixes = storage.listBlobs(outputPrefix + "-map-").stream()
				.map(blob -> blob.substring(0, blob.lastIndexOf('-'))).collect(Collectors.toSet());

		AtomicInteger partitionCounter = new AtomicInteger(0);
		Lists.partition(new ArrayList<>(reduceKeyPrefixes), outPartSize).forEach(partitionKeyList -> {

			String partitionOutputBlob = String.format("%s-part%04d", outputPrefix, partitionCounter.incrementAndGet());

			BlobWriter writer = storage.blobWriter(partitionOutputBlob);

			partitionKeyList.forEach(keyPrefix -> {
				new ReducerTask("client", storage, jobClassBlob, keyPrefix, outputPrefix).execute(writer);
			});

			writer.close();
		});
	}


	public static void main(String[] args) {
		String baseURI = "http://0.0.0.0:9999/mapreduce/";
		Endpoint.publish(baseURI, new ComputeNodeImpl("Remote", new RemoteBlobStorage()));
		System.err.println("SOAP ComputeNode Server ready...");
	}

}
