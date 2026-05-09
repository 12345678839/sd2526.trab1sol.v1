package sd2526.trab.impl.grpc.servers;

import java.util.logging.Logger;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.utils.IP;

public class GrpcMessagesServer extends AbstractGrpcServer {

    public static final int PORT = 8084;
    private static Logger Log = Logger.getLogger(GrpcMessagesServer.class.getName());

    public GrpcMessagesServer(String domain) {
        super(Log, "Messages@" + domain, PORT);
    }

    public static void main(String[] args) throws Exception {
        String hostname = IP.hostname();
        String domain = hostname.contains(".") ? hostname.substring(hostname.lastIndexOf('.') + 1) : "ourorg0";
        String keystoreFile = hostname + ".jks";

        JavaMessages.getInstance();

        new GrpcMessagesServer(domain).start(keystoreFile, "changeit",
                new GrpcMessagesController(),
                new GrpcAdminMessagesController());
    }
}