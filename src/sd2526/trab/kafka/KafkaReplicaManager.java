package sd2526.trab.kafka;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
  private static String localDomain;

  public static class RepOp {
    public String type;
    public String name;
    public String pwd;
    public String mid;
    public Message msg;

    public RepOp() {
    }
  }

  public static void init(String domain) {
    localDomain = domain;
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
            if (op.mid != null && op.msg != null) {
              op.msg.setId(op.mid);
            }
            try {
              db.postMessage(op.pwd, op.msg);
            } catch (Exception ignore) {
            }

            if (op.msg != null) {
              propagateToRemoteDomains(op.msg);
            }
            break;

          case "remotepost":
            if (op.msg != null) {
              try {
                db.remotePostMessage(op.msg);
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
              db.deleteLocalOnly(op.mid);
            } catch (Exception ignore) {
            }
            if (op.msg != null) {
              propagateDeleteToRemoteDomains(op.mid, op.msg);
            }
            break;

          case "remotedelete":
            if (op.mid != null) {
              try {
                db.remoteDeleteMessage(op.mid);
              } catch (Exception ignore) {
              }
            }
            break;

          default:
            Log.warning("Unknown operation type: " + op.type);
            break;
        }

        try {
          JavaMessages.getInstance().syncCounterFromDB();
        } catch (Exception ignore) {
        }

      } catch (Exception e) {
        Log.warning("Error processing Kafka record: " + e.getMessage());
      }
    });
  }

  private static void propagateToRemoteDomains(Message msg) {
    if (msg == null || msg.getDestination() == null || localDomain == null)
      return;

    Set<String> remoteDomains = msg.getDestination().stream()
        .filter(addr -> addr != null && addr.contains("@"))
        .map(addr -> addr.split("@")[1])
        .filter(domain -> !domain.equals(localDomain))
        .collect(Collectors.toSet());

    for (String remoteDomain : remoteDomains) {
      try {
        RepOp remoteOp = new RepOp();
        remoteOp.type = "remotepost";
        remoteOp.msg = msg;
        String remoteTopic = "replica-msgs-" + remoteDomain;
        publisher.publish(remoteTopic, mapper.writeValueAsString(remoteOp));
        Log.info("Propagated remotepost " + msg.getId() + " to " + remoteDomain);
      } catch (Exception e) {
        Log.warning("Failed to propagate post to " + remoteDomain + ": " + e.getMessage());
      }
    }
  }

  private static void propagateDeleteToRemoteDomains(String mid, Message msg) {
    if (msg == null || msg.getDestination() == null || localDomain == null)
      return;

    Set<String> remoteDomains = msg.getDestination().stream()
        .filter(addr -> addr != null && addr.contains("@"))
        .map(addr -> addr.split("@")[1])
        .filter(domain -> !domain.equals(localDomain))
        .collect(Collectors.toSet());

    for (String remoteDomain : remoteDomains) {
      try {
        RepOp remoteOp = new RepOp();
        remoteOp.type = "remotedelete";
        remoteOp.mid = mid;
        String remoteTopic = "replica-msgs-" + remoteDomain;
        publisher.publish(remoteTopic, mapper.writeValueAsString(remoteOp));
        Log.info("Propagated remotedelete " + mid + " to " + remoteDomain);
      } catch (Exception e) {
        Log.warning("Failed to propagate delete to " + remoteDomain + ": " + e.getMessage());
      }
    }
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
      Log.warning("Error publishing to Kafka: " + e.getMessage());
      e.printStackTrace();
    }
  }
}