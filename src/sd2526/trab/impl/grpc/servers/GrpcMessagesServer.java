package sd2526.trab.impl.grpc.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import sd2526.trab.impl.utils.SslContextFactory;

public class GrpcMessagesServer extends AbstractGrpcServer {
	public static final int PORT = 8084;
	private static Logger Log = Logger.getLogger(GrpcMessagesServer.class.getName());

	public GrpcMessagesServer(String domain) {
		super(Log, "Messages@" + domain, PORT);
	}

	public static void main(String[] args) throws Exception {
		String domain = args.length > 0 ? args[0] : "ourorg0";
		String keystoreFile = String.format("messages0.%s.jks", domain);

		SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
		new GrpcMessagesServer(domain).start(new GrpcMessagesController(), sslContext);
	}
}