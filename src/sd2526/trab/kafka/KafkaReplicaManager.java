package sd2526.trab.kafka;

import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;

import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.kafka.KafkaPublisher;
import sd2526.trab.kafka.KafkaSubscriber;

public class KafkaReplicaManager {
  private static final String KAFKA_BROKERS = "kafka:9092";
  private static KafkaPublisher publisher;
  private static ObjectMapper mapper = new ObjectMapper();
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

    KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(topic));
    subscriber.start(record -> {
      try {
        RepOp op = mapper.readValue(record.value(), RepOp.class);
        JavaMessages db = JavaMessages.getInstance();

        switch (op.type) {
          case "post":
            db.postMessage(op.pwd, op.msg);
            break;
          case "remove":
            db.removeInboxMessage(op.name, op.mid, op.pwd);
            break;
          case "delete":
            db.deleteMessage(op.name, op.mid, op.pwd);
            break;
        }
      } catch (Exception e) {
        e.printStackTrace();
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