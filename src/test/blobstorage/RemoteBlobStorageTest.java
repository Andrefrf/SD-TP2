package test.blobstorage;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobReader;
import api.storage.BlobStorage.BlobWriter;
import sys.storage.RemoteBlobStorage;

public class RemoteBlobStorageTest {		
		
		/*
		 * Exemplifies the use of BlobStorage by Applications (such as the MapReduce engine)
		 * 
		 */
		public static void main(String[] args) throws Exception {

			//1. Get an implementation of the storage. LocalBlobStorage implements everything locally, in memory, sadly...
			BlobStorage storage = new RemoteBlobStorage();

			//2. We can list ALL the blobs already stored in the storage, using "" as the name prefix.
			List<String> blobs = storage.listBlobs("");
			
			assert blobs.isEmpty(); //At this stage we expect an empty list.
			
			//3. Write a blob containing a single line of text. We must not forget to close the blob, at the end.
			BlobWriter writer = storage.blobWriter("1-line-blob");
			writer.writeLine("it works!");
			writer.close();
			
			//4. Listing again, should return 1 and only blob.
			assert storage.listBlobs("").size() == 1;

			//5. Read the contents of the blob to the standard output.
			BlobReader reader = storage.readBlob("1-line-blob");
			for( String line : reader )
				System.out.println( line  );
			
			//6. Read the contents of the blob to the standard output, Java 8+ way.
			storage.readBlob("1-line-blob").forEach( System.out::println );
			
			//7. Read a text file and save it as a blob to the storage:			
			BlobWriter writer2 = storage.blobWriter("WordCount");
			for( String line : Files.readAllLines(new File("WordCount.java").toPath()))
				writer2.writeLine( line );
			writer2.close();
			
			
			//8. Read a text file and save it as a blob to the storage. Java 8+ way		
			BlobWriter writer3 = storage.blobWriter("WordCount2");
			Files.readAllLines(new File("WordCount.java").toPath()).stream().forEach( writer3::writeLine );
			writer3.close();
			
			//9. Read the contents of blobs, whose names start with "Word" to the standard output. Java 8+ way.
			storage.listBlobs("Word").forEach( blob -> {
				storage.readBlob( blob ).forEach( System.out::println );
			});

			//10. Delete all blobs starting with "W"
			storage.deleteBlobs("W");

			//11. Delete all blobs found in storage.
			storage.deleteBlobs("");
		}
}
