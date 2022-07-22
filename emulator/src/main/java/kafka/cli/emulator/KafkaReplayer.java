package kafka.cli.emulator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import kafka.context.KafkaContexts;
import kafka.context.sr.SchemaRegistryContexts;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaReplayer {

  static final Logger LOG = LoggerFactory.getLogger(KafkaReplayer.class);

  /**
   * Read zip archive files and produce records into Kafka topics with the frequency defined in the
   * archive.
   */
  public void replay(
    Properties properties,
    EmulatorArchive archive,
    Map<String, String> topicMap,
    boolean noWait,
    boolean dryRun
  ) throws InterruptedException {
    // create producer
    var keySerializer = new ByteArraySerializer();
    var valueSerializer = new ByteArraySerializer();
    properties.put("acks", "1");
    KafkaProducer<byte[], byte[]> producer = null;
    if (!dryRun) producer =
      new KafkaProducer<>(properties, keySerializer, valueSerializer);
    // per partition
    final var topicPartitionNumber = archive.topicPartitionNumber();
    // prepare topics
    if (!dryRun) {
      try (var admin = AdminClient.create(properties)) {
        var topics = admin.listTopics().names().get();
        var newTopics = new ArrayList<NewTopic>();
        for (var t : topicPartitionNumber.keySet()) {
          final var topicName = topicMap.getOrDefault(t, t);
          if (!topics.contains(topicName)) {
            final var newTopic = new NewTopic(
              topicName,
              Optional.of(topicPartitionNumber.get(t)),
              Optional.empty()
            );
            newTopics.add(newTopic);
          }
        }
        admin.createTopics(newTopics).all().get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    archive.registerSchemas(topicMap);

    final var size = archive.topicPartitions().size();
    var executor = Executors.newFixedThreadPool(size);
    var countDownLatch = new CountDownLatch(size);
    for (var topicPartition : archive.topicPartitions()) {
      var rs = archive.records(topicPartition);
      final var finalProducer = producer;
      executor.submit(() -> {
        try {
          long prevTime = System.currentTimeMillis();
          // per record
          for (var r : rs) {
            // prepare record
            final var topicName = topicMap.getOrDefault(r.topic(), r.topic());
            var record = new ProducerRecord<>(
              topicName,
              r.partition(),
              prevTime + r.afterMs(),
              archive.key(topicName, r),
              archive.value(topicName, r)
            );
            // wait
            var wait = (prevTime + r.afterMs()) - System.currentTimeMillis();
            if (!noWait && wait > 0) {
              LOG.info(
                "{}:{}:{}: waiting {} ms.",
                topicName,
                r.partition(),
                r.offset(),
                r.afterMs()
              );
              Thread.sleep(r.afterMs());
            } else {
              LOG.info(
                "{}:{}:{}: no waiting (expected after: {} ms.)",
                topicName,
                r.partition(),
                r.offset(),
                r.afterMs()
              );
            }
            if (!dryRun) {
              var meta = finalProducer.send(record).get();
              prevTime = meta.timestamp();
            } else prevTime = System.currentTimeMillis();
          }
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        countDownLatch.countDown();
        LOG.info("Replay for {} finished", topicPartition);
      });
    }
    countDownLatch.await();
    LOG.info("Replay finished");
    executor.shutdown();
  }

  public static void main(String[] args) throws IOException {
    var store = new SqliteStore(Path.of("test.db"));
    var emulator = new KafkaReplayer();
    var context = KafkaContexts.load().get("local");
    var props = context.properties();
    var sr = SchemaRegistryContexts.load().get("local");
    props.putAll(sr.properties());

    final var archive = store.load(props);

    try {
      emulator.replay(props, archive, Map.of("t_sr_1", "t_sr_5"), false, false);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
