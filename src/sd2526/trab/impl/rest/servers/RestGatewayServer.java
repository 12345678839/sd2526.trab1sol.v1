package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.impl.utils.SslContextFactory;

public class RestGatewayServer extends AbstractRestServer {

	public static final int PORT = 8082;
	private static Logger Log = Logger.getLogger(RestGatewayServer.class.getName());

	RestGatewayServer() {
		super(Log, "Messages", PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(new RestUsersResource(true));
		config.register(new RestMessagesResource(true));
	}

	public static void main(String[] args) throws Exception {
		String domain = args.length > 0 ? args[0] : "ourorg0";
		String keystoreFile = String.format("messages1.%s.jks", domain);
		SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
		new RestGatewayServer().start(sslContext);
	}
}