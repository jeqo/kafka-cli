package kafka.emulator;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import kafka.context.KafkaContext;
import kafka.context.KafkaContexts;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEmulator {

  static final Logger LOG = LoggerFactory.getLogger(KafkaEmulator.class);
  final ArchiveStore archiveLoader;

  public KafkaEmulator(ArchiveStore archiveLoader) {
    this.archiveLoader = archiveLoader;
  }

  /**
   * Read and package topics records into an archive.
   *
   * <p>Based on the start and end conditions, start a consumer and poll records per partition.
   * Transform into archive records and append to the archive. Once the end condition is given, the
   * results are flushed into the zip archive files.
   *
   */
  public void record(
    KafkaContext kafkaContext,
    List<String> topics,
    EmulatorArchive.FieldFormat keyFormat,
    EmulatorArchive.FieldFormat valueFormat,
    StartFromOption startFrom,
    EndAtOption endAt
  ) throws IOException {
    final var endTime = System.currentTimeMillis();
    // create consumer
    final var properties = kafkaContext.properties();
    properties.put("group.id", "emulator-" + endTime);
    final var keyDeserializer = new ByteArrayDeserializer();
    final var valueDeserializer = new ByteArrayDeserializer();
    var consumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
    // set offsets from
    // consumer loop
    var latestTimestamps = new HashMap<TopicPartition, Long>();
    final var archive = EmulatorArchive.create();
    var listTopics = consumer.listTopics();
    var topicPartitions = new ArrayList<TopicPartition>();
    for (var topic : topics) {
      var partitionsInfo = listTopics.get(topic);
      topicPartitions.addAll(
        partitionsInfo
          .stream()
          .map(info -> new TopicPartition(info.topic(), info.partition()))
          .toList()
      );
    }
    consumer.assign(topicPartitions);
    if (!startFrom.offsets().isEmpty()) {
      startFrom.offsets().forEach(consumer::seek);
    }
    if (startFrom.timestamp().isPresent()) {
      final var ts = startFrom.timestamp().getAsLong();
      final var offsets = consumer.offsetsForTimes(
        topicPartitions.stream().collect(Collectors.toMap(tp -> tp, tp -> ts))
      );
      offsets.forEach((tp, offsetAndTimestamp) ->
        consumer.seek(tp, offsetAndTimestamp.offset())
      );
    } else {
      consumer.seekToBeginning(topicPartitions);
    }

    var allDone = false;
    var done = topicPartitions.stream().collect(Collectors.toMap(tp -> tp, tp -> false));
    while (!allDone) {
      // set offsets to
      // break by topic-partition
      var records = consumer.poll(Duration.ofSeconds(5));
      for (var partition : records.partitions()) {
        if (done.get(partition)) break;
        // start: per partition
        var perPartition = records.records(partition);
        for (var record : perPartition) {
          // transform to ZipRecord
          var latestTimestamp = latestTimestamps.getOrDefault(partition, -1L);
          final var currentTimestamp = record.timestamp();
          if (currentTimestamp >= endTime) {
            break;
          }
          final long afterMs;
          if (latestTimestamp < 0) {
            afterMs = 0;
          } else {
            afterMs = currentTimestamp - latestTimestamp;
          }
          var zipRecord = EmulatorArchive.EmulatorRecord.from(
            record,
            keyFormat,
            valueFormat,
            afterMs
          );
          // append to topic-partition file
          archive.append(partition, zipRecord);
          latestTimestamps.put(partition, currentTimestamp);
          if (isDone(partition, archive, endAt)) {
            done.put(partition, true);
            break;
          }
        }
        // end: per partition
      }
      if (records.isEmpty()) {
        allDone = true;
      } else {
        allDone = done.values().stream().reduce((r, l) -> r && l).orElse(false);
      }
    }
    archiveLoader.save(archive);
  }

  private boolean isDone(TopicPartition tp, EmulatorArchive archive, EndAtOption endAt) {
    if (!endAt.now()) {
      if (endAt.recordsPerPartition().isPresent()) {
        var enough = true;
        final var total = endAt.recordsPerPartition().getAsInt();
        final var size = archive.records(tp).size();
        if (total > size) {
          enough = false;
        }
        return enough;
      }

      if (!endAt.offsets.isEmpty()) {
        return archive.oldestOffsets(tp) >= endAt.offsets().get(tp);
      }

      if (endAt.timestamp().isPresent()) {
        return archive.oldestTimestamps(tp) >= endAt.timestamp().getAsLong();
      }
    }
    return false;
  }

  /**
   * Read zip archive files and produce records into Kafka topics with the frequency defined in the
   * archive.
   */
  public void replay(
    KafkaContext kafkaContext,
    Map<String, String> topicMap,
    boolean noWait
  ) throws InterruptedException {
    // load archive
    var archive = archiveLoader.load();
    // create producer
    var keySerializer = new ByteArraySerializer();
    var valueSerializer = new ByteArraySerializer();
    final var props = kafkaContext.properties();
    props.put("acks", "1");
    var producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
    // per partition
    final var topicPartitionNumber = archive.topicPartitionNumber();
    // prepare topics
    try (var admin = AdminClient.create(kafkaContext.properties())) {
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

    final var size = archive.topicPartitions().size();
    var executor = Executors.newFixedThreadPool(size);
    var countDownLatch = new CountDownLatch(size);
    for (var topicPartition : archive.topicPartitions()) {
      var rs = archive.records(topicPartition);
      executor.submit(() -> {
        long prevTime = 0L;
        // per record
        for (var r : rs) {
          // prepare record
          final var topicName = topicMap.getOrDefault(r.topic(), r.topic());
          var record = new ProducerRecord<>(topicName, r.partition(), r.key(), r.value());
          try {
            // wait
            var wait = (prevTime + r.afterMs()) - System.currentTimeMillis();
            if (!noWait && wait > 0) {
              LOG.info("{}:{}: waiting {} ms.", topicPartition, r.offset(), r.afterMs());
              Thread.sleep(r.afterMs());
            } else {
              LOG.info(
                "{}:{}: no waiting (after: {} ms.)",
                topicPartition,
                r.offset(),
                r.afterMs()
              );
            }
            var meta = producer.send(record).get();
            prevTime = meta.timestamp();
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

  record StartFromOption(
    boolean beginning,
    Map<TopicPartition, Long> offsets,
    OptionalLong timestamp
  ) {
    public static StartFromOption of() {
      return new StartFromOption(true, Map.of(), OptionalLong.empty());
    }
    public static StartFromOption of(Map<TopicPartition, Long> offsets) {
      return new StartFromOption(true, offsets, OptionalLong.empty());
    }
    public static StartFromOption of(Map<TopicPartition, Long> offsets, long timestamp) {
      return new StartFromOption(false, offsets, OptionalLong.of(timestamp));
    }
    public static StartFromOption of(Instant timestamp) {
      return new StartFromOption(
        false,
        Map.of(),
        OptionalLong.of(timestamp.toEpochMilli())
      );
    }
  }

  record EndAtOption(
    boolean now,
    OptionalInt recordsPerPartition,
    Map<TopicPartition, Long> offsets,
    OptionalLong timestamp
  ) {
    public static EndAtOption of() {
      return new EndAtOption(true, OptionalInt.empty(), Map.of(), OptionalLong.empty());
    }
    public static EndAtOption of(int recordsPerPartition) {
      return new EndAtOption(
        false,
        OptionalInt.of(recordsPerPartition),
        Map.of(),
        OptionalLong.empty()
      );
    }

    public static EndAtOption of(Instant timestamp) {
      return new EndAtOption(
        false,
        OptionalInt.empty(),
        Map.of(),
        OptionalLong.of(timestamp.toEpochMilli())
      );
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    var archiveLoader = new ArchiveStore.SqliteArchiveLoader(Path.of("test.db"));
    var emulator = new KafkaEmulator(archiveLoader);
    var context = KafkaContexts.load().get("local");

    emulator.record(
      context,
      List.of("t5"),
      EmulatorArchive.FieldFormat.STRING,
      EmulatorArchive.FieldFormat.STRING,
      StartFromOption.of(),
      EndAtOption.of(10)
    );

    emulator.replay(context, Map.of("t5", "t14"), true);
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
