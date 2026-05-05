package sd2526.trab.impl.grpc.servers;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.logging.Logger;
import javax.net.ssl.KeyManagerFactory;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;

public abstract class AbstractGrpcServer extends AbstractServer {
    protected static final String SERVER_BASE_URI = "grpc://%s:%s/grpc";

    protected AbstractGrpcServer(Logger log, String service, int port) {
        super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port));
    }

    protected void start(String keystorePath, String password, BindableService... services) throws Exception {
        int port = Integer.parseInt(serverURI.split(":")[2].split("/")[0]);
        NettyServerBuilder builder = NettyServerBuilder.forPort(port);

        for (BindableService service : services)
            builder.addService(service);

        if (keystorePath != null) {
            KeyStore ks = KeyStore.getInstance("JKS");
            try (FileInputStream fis = new FileInputStream("/home/sd/" + keystorePath)) {
                ks.load(fis, password.toCharArray());
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, password.toCharArray());

            SslContextBuilder sslBuilder = SslContextBuilder.forServer(kmf);
            GrpcSslContexts.configure(sslBuilder);
            builder.sslContext(sslBuilder.build());
        }

        Server server = builder.build().start();
        Discovery.getInstance().announce(this.service, serverURI);

        Log.info(String.format("%s Server ready @ %s\n", this.service, serverURI));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
        }));
    }

    protected void start(BindableService service, String keystorePath, String password) throws Exception {
        start(keystorePath, password, service);
    }

    @Override
    public void start() {
    }
}