package sys.mapreduce;

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.jws.WebService;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;
import com.sun.net.httpserver.*;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Lists;

@WebService(serviceName = ComputeNode.NAME, targetNamespace = ComputeNode.NAMESPACE, endpointInterface = ComputeNode.INTERFACE)
public class ComputeNodeSide implements ComputeNode {

	private static final String SERVER_KEYSTORE = "blobstorage.jks";
	private static final String SERVER_KEYSTORE_PWD = "blobstorage";
	private static final String SERVER_TRUSTSTORE = "blobstorage-ts.jks";
	private static final String SERVER_TRUSTSTORE_PWD = "blobstorage";
	private String worker;
	private BlobStorage storage;

	public ComputeNodeSide(String worker, BlobStorage storage) {
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


	@SuppressWarnings("restriction")
	public static void main(String[] args) throws Exception {
        List<X509Certificate> trustedCertificates = new ArrayList<>();

		KeyStore ks = JKS.load(SERVER_KEYSTORE, SERVER_KEYSTORE_PWD);
		KeyStore ts = JKS.load(SERVER_TRUSTSTORE, SERVER_TRUSTSTORE_PWD);

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(ks, SERVER_KEYSTORE_PWD.toCharArray());

		SSLContext ctx = SSLContext.getInstance("TLS");

		TrustManagerFactory tmf2 = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf2.init(ts);

		for (TrustManager tm : tmf2.getTrustManagers()) {
			if (tm instanceof X509TrustManager)
				trustedCertificates.addAll(Arrays.asList(((X509TrustManager) tm).getAcceptedIssuers()));
		}

		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {

			@Override
			public void checkClientTrusted(X509Certificate[] certs, String authType) {
				System.err.println(certs[0].getSubjectX500Principal());
			}

			@Override
			public void checkServerTrusted(X509Certificate[] certs, String authType) {
			}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return trustedCertificates.toArray(new X509Certificate[0]);
			}
		} };

		ctx.init(kmf.getKeyManagers(), trustAllCerts, null);

		HttpsServer httpsServer = HttpsServer.create( new InetSocketAddress("0.0.0.0", 6666), -1);
		httpsServer.setHttpsConfigurator(new HttpsConfigurator( ctx ));
		
		httpsServer.setHttpsConfigurator (new HttpsConfigurator(ctx) {
		     @Override
			public void configure (HttpsParameters params) {
		    	 SSLParameters sslparams = ctx.getDefaultSSLParameters();
		    	 sslparams.setNeedClientAuth(true);
				params.setSSLParameters( sslparams );
		     }
		 });
		httpsServer.start();
	}

}
