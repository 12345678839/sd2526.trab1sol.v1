package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.impl.utils.SslContextFactory;

public class RestMessagesServer extends AbstractRestServer {
	public static final int PORT = 8080;
	private static Logger Log = Logger.getLogger(RestMessagesServer.class.getName());

	public RestMessagesServer(String domain) {
		super(Log, "Messages@" + domain, PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestMessagesResource.class);
	}

	public static void main(String[] args) throws Exception {
		String domain = args.length > 0 ? args[0] : "ourorg0";
		String keystoreFile = "messages0." + domain + ".jks";

		SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
		new RestMessagesServer(domain).start(sslContext);
	}
}