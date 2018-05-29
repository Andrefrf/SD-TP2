package sys.mapreduce;

import api.storage.BlobStorage;

abstract class MapReduceTask {

	protected final String worker;
	protected final String inputPrefix;
	protected final String outputPrefix;

	protected final BlobStorage storage;
	protected final String jobClassBlob;

	protected MapReduceTask(String worker, BlobStorage storage, String jobClassBlob, String inputPrefix, String outputPrefix) {
		this.worker = worker;
		this.storage = storage;
		this.inputPrefix = inputPrefix;
		this.outputPrefix = outputPrefix;
		this.jobClassBlob = jobClassBlob;
	}
}
