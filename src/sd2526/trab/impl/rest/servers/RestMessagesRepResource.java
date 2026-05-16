package sd2526.trab.impl.rest.servers;

import jakarta.inject.Singleton;
import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.kafka.KafkaReplicaManager;

@Singleton
public class RestMessagesRepResource extends RestMessagesResource {

  public RestMessagesRepResource() {
    super();
  }

  @Override
  public String postMessage(String pwd, Message msg) {
    if (msg.getId() != null && !msg.getId().isEmpty()) {
      return msg.getId();
    }

    JavaMessages jm = JavaMessages.getInstance();
    String msgId = jm.generateNextId();
    msg.setId(msgId);

    KafkaReplicaManager.publish("post", null, pwd, msgId, msg);

    for (int i = 0; i < 50; i++) {
      if (jm.messageExists(msgId))
        break;
      try {
        Thread.sleep(100);
      } catch (Exception ignore) {
      }
    }

    try {
      Thread.sleep(1000);
    } catch (Exception ignore) {
    }

    return msgId;
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
    Message msg = JavaMessages.getInstance().getMessageFromCache(mid);
    KafkaReplicaManager.publish("delete", name, pwd, mid, msg);
    try {
      Thread.sleep(500);
    } catch (Exception ignore) {
    }
  }

  @Override
  public void remotePostMessage(Message m) {
    KafkaReplicaManager.publish("remotepost", null, null, null, m);
    try {
      Thread.sleep(500);
    } catch (Exception ignore) {
    }
  }

  @Override
  public void remoteDeleteMessage(String mid) {
    KafkaReplicaManager.publish("remotedelete", null, null, mid, null);
    try {
      Thread.sleep(500);
    } catch (Exception ignore) {
    }
  }

  @Override
  public void remoteDeleteUserInbox(String name) {
    super.resultOrThrow(JavaMessages.getInstance().remoteDeleteUserInbox(name));
  }
}