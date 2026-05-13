package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.SslContextFactory;
import sd2526.trab.kafka.KafkaReplicaManager;

public class RestMessagesRepServer extends AbstractRestServer {

  private static Logger Log = Logger.getLogger(RestMessagesRepServer.class.getName());

  public RestMessagesRepServer(String domain, int port) {
    super(Log, "MessagesRep@" + domain, port);
  }

  @Override
  void registerResources(ResourceConfig config) {
    config.register(RestMessagesRepResource.class);
  }

  public static void main(String[] args) throws Exception {
    String hostname = IP.hostname();
    String domain = hostname.contains(".")
        ? hostname.substring(hostname.lastIndexOf('.') + 1)
        : "ourorg0";
    String keystoreFile = hostname + ".jks";

    int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;

    JavaMessages.getInstance();

    KafkaReplicaManager.init(domain);

    SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
    new RestMessagesRepServer(domain, port).start(sslContext);
  }
}