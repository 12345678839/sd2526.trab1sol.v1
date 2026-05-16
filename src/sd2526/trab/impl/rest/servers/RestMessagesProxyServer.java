package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.SslContextFactory;
import sd2526.trab.kafka.KafkaReplicaManager;

public class RestMessagesProxyServer extends AbstractRestServer {
  public static final int PORT = 8085;
  private static Logger Log = Logger.getLogger(RestMessagesProxyServer.class.getName());

  public RestMessagesProxyServer(String domain) {
    super(Log, "Messages@" + domain, PORT);
  }

  @Override
  void registerResources(ResourceConfig config) {
    config.register(RestMessagesRepResource.class); // era RestMessagesResource.class
  }

  public static void main(String[] args) throws Exception {
    String hostname = IP.hostname();
    String domain = hostname.contains(".") ? hostname.substring(hostname.lastIndexOf('.') + 1) : "ourorg0";
    String keystoreFile = hostname + ".jks";

    boolean freshStart = args.length == 0 || Boolean.parseBoolean(args[0]);

    Log.info("RestMessagesProxyServer starting - freshStart=" + freshStart);

    KafkaReplicaManager.init(domain, freshStart);

    KafkaReplicaManager.waitForReplay();

    SSLContext sslContext = SslContextFactory.getContext(keystoreFile, "changeit");
    new RestMessagesProxyServer(domain).start(sslContext);
  }
}