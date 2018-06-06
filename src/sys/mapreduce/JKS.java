package sys.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

public class JKS {

	public static KeyStore load( String filename, String password ) throws Exception {
		File f = new File("/home/sd2018/Desktop/sd/SD-TP2/tls/" + filename);
		try (InputStream ksIs = new FileInputStream(f)) {
			KeyStore ks = KeyStore.getInstance("JKS");
			ks.load(ksIs, password.toCharArray());
			return ks;
		}
	}
}
