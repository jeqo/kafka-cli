package kafka.cli.emulator;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.stream.Collectors;
import kafka.context.KafkaContexts;
import kafka.context.sr.SchemaRegistryContexts;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecorder {

  static final Logger LOG = LoggerFactory.getLogger(KafkaRecorder.class);

  /**
   * Read and package topics records into an archive.
   *
   * <p>Based on the start and end conditions, start a consumer and poll records per partition.
   * Transform into archive records and append to the archive. Once the end condition is given, the
   * results are flushed into the zip archive files.
   *
   */
  public EmulatorArchive record(
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
    final var archive = EmulatorArchive.with(keyFormat, valueFormat, properties);
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
          // append to topic-partition file
          archive.append(partition, record, afterMs);
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
    return archive;
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

  public static void main(String[] args) throws IOException {
    var emulator = new KafkaRecorder();
    var context = KafkaContexts.load().get("local");
    var props = context.properties();
    var sr = SchemaRegistryContexts.load().get("local");
    props.putAll(sr.properties());
    var archive = emulator.record(
      props,
      List.of("t_sr_1"),
      EmulatorArchive.FieldFormat.STRING,
      EmulatorArchive.FieldFormat.SR_AVRO,
      RecordStartFrom.of(),
      RecordEndAt.of()
    );
    new SqliteStore(Path.of("test.db")).save(archive);
  }
}
