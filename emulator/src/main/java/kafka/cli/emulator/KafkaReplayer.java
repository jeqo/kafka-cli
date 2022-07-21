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

    final var size = archive.topicPartitions().size();
    var executor = Executors.newFixedThreadPool(size);
    var countDownLatch = new CountDownLatch(size);
    for (var topicPartition : archive.topicPartitions()) {
      var rs = archive.records(topicPartition);
      final var finalProducer = producer;
      executor.submit(() -> {
        long prevTime = System.currentTimeMillis();
        // per record
        for (var r : rs) {
          // prepare record
          final var topicName = topicMap.getOrDefault(r.topic(), r.topic());
          var record = new ProducerRecord<>(
            topicName,
            r.partition(),
            prevTime + r.afterMs(),
            r.key(),
            r.value()
          );
          try {
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
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
        countDownLatch.countDown();
        LOG.info("Replay for {} finished", topicPartition);
      });
    }
    countDownLatch.await();
    LOG.info("Replay finished");
    executor.shutdown();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    var archiveLoader = new SqliteStore(Path.of("test.db"));
    var emulator = new KafkaReplayer();
    var context = KafkaContexts.load().get("local");
    var props = context.properties();
    //    emulator.record(
    //      props,
    //      List.of("t5"),
    //      EmulatorArchive.FieldFormat.STRING,
    //      EmulatorArchive.FieldFormat.STRING,
    //      RecordStartFrom.of(),
    //      RecordEndAt.of(10)
    //    );

    emulator.replay(props, archiveLoader.load(), Map.of("t5", "t14"), false, true);
    // Check Thread handling by parallel stream
    //    final var map = Map.of("s1", "a", "s2", "b", "s3", "c");
    //    map.keySet().parallelStream()
    //        .forEach(
    //            k -> {
    //              System.out.println(Thread.currentThread().getName());
    //              try {
    //                Thread.sleep(1000);
    //              } catch (InterruptedException e) {
    //                throw new RuntimeException(e);
    //              }
    //              System.out.println(map.get(k));
    //            });
  }
}
