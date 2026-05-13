package sd2526.trab.impl.grpc.servers;

import java.util.logging.Logger;

import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.utils.IP;

public class GrpcUsersServer extends AbstractGrpcServer {
    public static final int PORT = 8083;
    private static Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());

    public GrpcUsersServer(String domain) {
        super(Log, "Users@" + domain, PORT);
    }

    public static void main(String[] args) throws Exception {
        String hostname = IP.hostname();
        String domain = hostname.contains(".") ? hostname.substring(hostname.lastIndexOf('.') + 1) : "ourorg0";
        String keystoreFile = String.format("users.%s.jks", domain);

        GrpcUsersServer server = new GrpcUsersServer(domain);

        new Thread(() -> {
            try {
                server.start(keystoreFile, "changeit",
                        new GrpcUsersController(),
                        new GrpcAdminUsersController());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);
        Discovery.getInstance().announce(server.serviceName(),
                "grpc://" + hostname + ":" + PORT + "/grpc");
        Thread.currentThread().join();
    }
}