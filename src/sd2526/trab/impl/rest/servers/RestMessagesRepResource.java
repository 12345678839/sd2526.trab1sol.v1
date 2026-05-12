package sd2526.trab.impl.rest.servers;

import jakarta.inject.Singleton;
import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.kafka.KafkaReplicaManager;
import sd2526.trab.impl.utils.IP;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class RestMessagesRepResource extends RestMessagesResource {

  public RestMessagesRepResource() {
    super();
  }

  @Override
  public String postMessage(String pwd, Message msg) {
    long currentCount = 0;
    AtomicLong counter = null;

    try {
      Field f = JavaMessages.class.getDeclaredField("counter");
      f.setAccessible(true);
      counter = (AtomicLong) f.get(JavaMessages.getInstance());
      currentCount = counter.get();
    } catch (Exception ignore) {
    }

    KafkaReplicaManager.publish("post", null, pwd, null, msg);

    if (counter != null) {
      try {
        for (int i = 0; i < 30; i++) {
          if (counter.get() > currentCount) {
            break;
          }
          Thread.sleep(100);
        }
      } catch (Exception ignore) {
      }
    } else {
      try {
        Thread.sleep(1000);
      } catch (Exception ignore) {
      }
    }

    String domain = IP.domain();
    if (domain == null)
      domain = "ourorg0";

    return domain + "+" + String.format("%04d", currentCount + 1);
  }

  @Override
  public void removeFromUserInbox(String name, String mid, String pwd) {
    KafkaReplicaManager.publish("remove", name, pwd, mid, null);
    try {
      Thread.sleep(500);
    } catch (Exception ignore) {
    }
  }

  @Override
  public void deleteMessage(String name, String mid, String pwd) {
    KafkaReplicaManager.publish("delete", name, pwd, mid, null);
    try {
      Thread.sleep(500);
    } catch (Exception ignore) {
    }
  }
}