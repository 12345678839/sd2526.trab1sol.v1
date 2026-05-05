package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.SslContextFactory;

public class RestUsersServer extends AbstractRestServer {
	public static final int PORT = 8081;
	private static Logger Log = Logger.getLogger(RestUsersServer.class.getName());

	public RestUsersServer(String domain) {
		super(Log, "Users@" + domain, PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestUsersResource.class);
	}

	public static void main(String[] args) throws Exception {
		String hostname = IP.hostname();
		String domain = hostname.contains(".") ? hostname.substring(hostname.lastIndexOf('.') + 1) : "ourorg0";
		String keystoreFile = String.format("users.%s.jks", domain);

		SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
		new RestUsersServer(domain).start(sslContext);
	}
}