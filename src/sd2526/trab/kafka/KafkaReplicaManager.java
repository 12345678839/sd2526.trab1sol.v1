package sd2526.trab.kafka;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import sd2526.trab.api.Message;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.utils.IP;

public class KafkaReplicaManager {

  private static final String KAFKA_BROKERS = "kafka:9092";
  private static final Logger Log = Logger.getLogger(KafkaReplicaManager.class.getName());

  private static KafkaPublisher publisher;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static String topic;
  private static String localDomain;
  private static CountDownLatch replayLatch;

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
    init(domain, true);
  }

  public static void init(String domain, boolean freshStart) {
    localDomain = domain;
    topic = "replica-msgs-" + domain;
    publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);

    String groupId = freshStart
        ? IP.hostname()
        : IP.hostname() + "-restart-" + System.currentTimeMillis();

    KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(
        KAFKA_BROKERS,
        List.of(topic),
        groupId);

    try {
      JavaMessages.getInstance().syncCounterFromDB();
    } catch (Exception ignore) {
    }

    if (!freshStart) {
      long endOffset = getTopicEndOffset();
      if (endOffset > 0) {
        replayLatch = new CountDownLatch(1);
        final long[] consumed = { 0 };

        subscriber.start(record -> {
          processRecord(record);
          consumed[0]++;
          if (replayLatch != null && consumed[0] >= endOffset) {
            replayLatch.countDown();
            replayLatch = null;
          }
        });
      } else {
        replayLatch = null;
        subscriber.start(record -> processRecord(record));
      }
    } else {
      replayLatch = null;
      subscriber.start(record -> processRecord(record));
    }
  }

  public static void waitForReplay() throws InterruptedException {
    if (replayLatch != null) {
      Log.info("Waiting for Kafka replay to complete...");
      replayLatch.await();
      Log.info("Kafka replay complete.");
    }
  }

  private static long getTopicEndOffset() {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA_BROKERS);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("group.id", "offset-checker-" + System.currentTimeMillis());

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      var partitions = consumer.partitionsFor(topic);
      if (partitions == null || partitions.isEmpty())
        return 0;

      var tps = partitions.stream()
          .map(p -> new TopicPartition(p.topic(), p.partition()))
          .collect(Collectors.toList());

      var endOffsets = consumer.endOffsets(tps);
      return endOffsets.values().stream().mapToLong(Long::longValue).sum();
    } catch (Exception e) {
      Log.warning("Failed to get topic end offset: " + e.getMessage());
      return 0;
    }
  }

  private static void processRecord(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
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