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
    if (msg.getId() != null) {
      return msg.getId();
    }

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

    String domain = IP.domain();
    if (domain == null)
      domain = "ourorg0";

    String expectedId = domain + "+" + String.format("%04d", currentCount + 1);

    JavaMessages javaMessages = JavaMessages.getInstance();
    for (int i = 0; i < 50; i++) {
      if (javaMessages.deliveredMessages.containsKey(expectedId)) {
        javaMessages.deliveredMessages.remove(expectedId);
        break;
      }
      try {
        Thread.sleep(100);
      } catch (Exception ignore) {
      }
    }

    try {
      Thread.sleep(400);
    } catch (Exception ignore) {
    }

    return expectedId;
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