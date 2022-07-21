package kafka.cli.emulator;

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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
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

  public void init() {
    final var archive = EmulatorArchive.create();
    archiveLoader.save(archive);
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
    Properties properties,
    List<String> topics,
    EmulatorArchive.FieldFormat keyFormat,
    EmulatorArchive.FieldFormat valueFormat,
    RecordStartFrom startFrom,
    RecordEndAt endAt
  ) {
    final var endTime = System.currentTimeMillis();
    // create consumer
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

  private boolean isDone(TopicPartition tp, EmulatorArchive archive, RecordEndAt endAt) {
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
    Properties properties,
    List<String> include,
    List<String> exclude,
    Map<String, String> topicMap,
    boolean noWait,
    boolean dryRun
  ) throws InterruptedException {
    // load archive
    var archive = archiveLoader.load();
    archive.setIncludeTopics(include);
    archive.setExcludeTopics(exclude);
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

  record RecordStartFrom(
    boolean beginning,
    Map<TopicPartition, Long> offsets,
    OptionalLong timestamp
  ) {
    public static RecordStartFrom of() {
      return new RecordStartFrom(true, Map.of(), OptionalLong.empty());
    }
    public static RecordStartFrom of(Map<TopicPartition, Long> offsets) {
      return new RecordStartFrom(true, offsets, OptionalLong.empty());
    }
    public static RecordStartFrom of(Map<TopicPartition, Long> offsets, long timestamp) {
      return new RecordStartFrom(false, offsets, OptionalLong.of(timestamp));
    }
    public static RecordStartFrom of(Instant timestamp) {
      return new RecordStartFrom(
        false,
        Map.of(),
        OptionalLong.of(timestamp.toEpochMilli())
      );
    }
  }

  record RecordEndAt(
    boolean now,
    OptionalInt recordsPerPartition,
    Map<TopicPartition, Long> offsets,
    OptionalLong timestamp
  ) {
    public static RecordEndAt of() {
      return new RecordEndAt(true, OptionalInt.empty(), Map.of(), OptionalLong.empty());
    }
    public static RecordEndAt of(int recordsPerPartition) {
      return new RecordEndAt(
        false,
        OptionalInt.of(recordsPerPartition),
        Map.of(),
        OptionalLong.empty()
      );
    }

    public static RecordEndAt of(Instant timestamp) {
      return new RecordEndAt(
        false,
        OptionalInt.empty(),
        Map.of(),
        OptionalLong.of(timestamp.toEpochMilli())
      );
    }

    public static RecordEndAt of(Map<TopicPartition, Long> offsets) {
      return new RecordEndAt(false, OptionalInt.empty(), offsets, OptionalLong.empty());
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    var archiveLoader = new ArchiveStore.SqliteArchiveLoader(Path.of("test.db"));
    var emulator = new KafkaEmulator(archiveLoader);
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

    emulator.replay(props, List.of(), List.of(), Map.of("t5", "t14"), false, true);
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
