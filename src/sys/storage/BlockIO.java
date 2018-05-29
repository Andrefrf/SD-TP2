package sys.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BlockIO {

	public static final String BASE_PATH = "./saved/";

	public static void checkAndCreateDirectory() {
		File directory = new File(BASE_PATH);
		if(!directory.exists())
			directory.mkdirs();
	}
	
	public static boolean writeBlock(String blockId, byte[] content) throws IOException {
		File outputFile = new File(BASE_PATH + "" + blockId);
		if(outputFile.exists()) 
			return false;
		FileOutputStream os = new FileOutputStream(outputFile);
		os.write(content, 0, content.length);
		os.close();
		return true;
	}

	public static byte[] readBlock(String blockId) {
		try {
			Path inputFile = Paths.get(BASE_PATH + blockId);
			if (inputFile.toFile().exists()) {
				byte[] bytes = Files.readAllBytes(inputFile);
				return bytes;
			}
		} catch (IOException e) {

		}
		return null;
	}

	public static boolean deleteBlock(String blockId) {
		File inputFile = new File(BASE_PATH + "" + blockId);
		if (inputFile.exists()) {
			return inputFile.delete();
		}
		return false;
	}

}
