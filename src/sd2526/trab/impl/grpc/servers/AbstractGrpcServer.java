package sd2526.trab.impl.grpc.servers;

import java.io.IOException;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;

public abstract class AbstractGrpcServer extends AbstractServer {
	protected static final String SERVER_BASE_URI = "grpc://%s:%s/grpc";

	protected AbstractGrpcServer(Logger log, String service, int port) {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port));
	}

	protected void start(BindableService service, SSLContext sslContext) throws IOException {
		int port = Integer.parseInt(serverURI.split(":")[2].split("/")[0]);

		Server server = NettyServerBuilder.forPort(port)
				.addService(service)
				.build()
				.start();

		Discovery.getInstance().announce(this.service, serverURI);

		Log.info(String.format("%s gRPC Server ready @ %s\n", getClass().getSimpleName(), serverURI));

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			server.shutdown();
		}));
	}

	@Override
	public void start() {
	}
}