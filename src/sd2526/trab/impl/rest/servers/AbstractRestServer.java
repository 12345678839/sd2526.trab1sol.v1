package sd2526.trab.impl.rest.servers;

import java.net.URI;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.discovery.Discovery; // Adicionado

public abstract class AbstractRestServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "https://%s:%s/rest";

	protected AbstractRestServer(Logger log, String service, int port) {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port));
	}

	@Override
	public void start() {
		this.start(null);
	}

	public void start(SSLContext sslContext) {
		ResourceConfig config = new ResourceConfig();
		registerResources(config);

		JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, sslContext);

		Discovery.getInstance().announce(service, serverURI);

		Log.info(String.format("%s Server ready @ %s\n", getClass().getSimpleName(), serverURI));
	}

	abstract void registerResources(ResourceConfig config);
}