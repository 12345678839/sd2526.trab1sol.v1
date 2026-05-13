package sd2526.trab.kafka;

import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.logging.Logger;

import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.kafka.KafkaPublisher;
import sd2526.trab.kafka.KafkaSubscriber;
import sd2526.trab.impl.utils.IP;

public class KafkaReplicaManager {

  private static final String KAFKA_BROKERS = "kafka:9092";
  private static final Logger Log = Logger.getLogger(KafkaReplicaManager.class.getName());

  private static KafkaPublisher publisher;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static String topic;

  public static class RepOp {
    public String type;
    public String name, pwd, mid;
    public Message msg;

    public RepOp() {
    }
  }

  public static void init(String domain) {
    topic = "replica-msgs-" + domain;
    publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);

    KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(
        KAFKA_BROKERS,
        List.of(topic),
        IP.hostname());

    try {
      JavaMessages.getInstance().syncCounterFromDB();
    } catch (Exception ignore) {
    }

    subscriber.start(record -> {
      try {
        RepOp op = mapper.readValue(record.value(), RepOp.class);
        JavaMessages db = JavaMessages.getInstance();

        switch (op.type) {
          case "post":
            if (op.msg != null && op.msg.getId() != null) {
              try {
                java.lang.reflect.Method m = JavaMessages.class.getDeclaredMethod("directPost", String.class,
                    Message.class);
                m.invoke(db, op.pwd, op.msg);
              } catch (Exception e) {
                try {
                  db.postMessage(op.pwd, op.msg);
                } catch (Exception ignore) {
                }
              }
            } else {
              try {
                db.postMessage(op.pwd, op.msg);
              } catch (Exception ignore) {
              }
            }
            break;
          case "remove":
            try {
              db.removeInboxMessage(op.name, op.mid, op.pwd);
            } catch (Exception ignore) {
            }
            break;
          case "delete":
            try {
              db.deleteMessage(op.name, op.mid, op.pwd);
            } catch (Exception ignore) {
            }
            break;
        }

        try {
          JavaMessages.getInstance().syncCounterFromDB();
        } catch (Exception ignore) {
        }

      } catch (Exception e) {
      }
    });
  }

  public static void publish(String type, String name, String pwd, String mid, Message msg) {
    try {
      RepOp op = new RepOp();
      op.type = type;
      op.name = name;
      op.pwd = pwd;
      op.mid = mid;
      op.msg = msg;
      publisher.publish(topic, mapper.writeValueAsString(op));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}