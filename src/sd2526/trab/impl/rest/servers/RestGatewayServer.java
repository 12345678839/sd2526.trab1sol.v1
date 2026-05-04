package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.impl.utils.SslContextFactory;

public class RestGatewayServer extends AbstractRestServer {

	public static final int PORT = 6666;
	private static Logger Log = Logger.getLogger(RestGatewayServer.class.getName());

	RestGatewayServer() {
		super(Log, "gateway", PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.registerInstances(new RestUsersResource(true), new RestMessagesResource(true));
	}

	public static void main(String[] args) throws Exception {
		String keystoreFile = args.length > 0 ? args[0] : "users.ourorg0.jks";
		SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
		new RestGatewayServer().start(sslContext);
	}
}