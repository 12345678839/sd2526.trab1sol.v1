package sd2526.trab.impl.grpc.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import sd2526.trab.impl.utils.SslContextFactory;

public class GrpcUsersServer extends AbstractGrpcServer {
	public static final int PORT = 8083;
	private static Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());

	public GrpcUsersServer(String domain) {
		super(Log, "Users@" + domain, PORT);
	}

	public static void main(String[] args) throws Exception {
		String domain = args.length > 0 ? args[0] : "ourorg0";
		String keystoreFile = String.format("users.%s.jks", domain);

		SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
		new GrpcUsersServer(domain).start(new GrpcUsersController(), sslContext);
	}
}