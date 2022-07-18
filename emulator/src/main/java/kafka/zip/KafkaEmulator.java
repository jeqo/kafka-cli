package kafka.zip;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import kafka.context.KafkaContexts;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KafkaEmulator {

  /**
   * Read and package topics records into an archive.
   *
   * <p>Based on the start and end conditions, start a consumer and poll records per partition.
   * Transform into archive records and append to the archive. Once the end condition is given, the
   * results are flushed into the zip archive files.
   *
   * @param kafkaContext
   * @param topics
   * @param fromBeginning // TODO replace with startCondition // TODO add endCondition
   * @return
   */
  public RecordingResult record(
      KafkaContexts.KafkaContext kafkaContext, List<String> topics, boolean fromBeginning)
      throws IOException {
    final var endTime = System.currentTimeMillis();
    // create consumer
    final var properties = kafkaContext.properties();
    properties.put("group.id", "emulator-" + endTime);
    final var keyDeserializer = new ByteArrayDeserializer();
    final var valueDeserializer = new ByteArrayDeserializer();
    var consumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
    // set offsets from
    // consumer loop
    var done = false;
    var latestTimestamps = new HashMap<TopicPartition, Long>();
    final var archive = EmulatorArchive.create();
    var listTopics = consumer.listTopics();
    var topicPartitions = new ArrayList<TopicPartition>();
    for (var topic : topics) {
      var partitionsInfo = listTopics.get(topic);
      topicPartitions.addAll(
          partitionsInfo.stream()
              .map(info -> new TopicPartition(info.topic(), info.partition()))
              .toList());
    }
    consumer.assign(topicPartitions);
    consumer.seekToBeginning(topicPartitions);
    while (!done) {
      // set offsets to
      // break by topic-partition
      var records = consumer.poll(Duration.ofSeconds(5));
      for (var partition : records.partitions()) {
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
          var zipRecord = EmulatorArchive.EmulatorRecord.from(record, afterMs);
          // append to topic-partition file
          archive.append(partition, zipRecord);
          latestTimestamps.put(partition, currentTimestamp);
        }
        if (isDone(archive)) {
          done = true;
          break;
        }
        // end: per partition
      }
      if (records.isEmpty()) {
        done = true;
      }
    }
    return new RecordingResult(archive);
  }

  private boolean isDone(EmulatorArchive archive) {
    return false;
  }

  /**
   * Read zip archive files and produce records into Kafka topics with the frequency defined in the
   * archive.
   *
   * @param kafkaContext
   * @return
   */
  public ReplayResult replay(KafkaContexts.KafkaContext kafkaContext, Path directory) throws IOException {
    //create producer
    var keySerializer = new ByteArraySerializer();
    var valueSerializer = new ByteArraySerializer();
    var producer = new KafkaProducer<>(kafkaContext.properties(), keySerializer, valueSerializer);
    //per partition
    //per record
    //prepare record
    //wait
    return null;
  }

  private class RecordingResult {
    final EmulatorArchive archive;

    private RecordingResult(EmulatorArchive archive) {
      this.archive = archive;
    }
  }

  private class ReplayResult {}

  public static void main(String[] args) throws IOException {
    var zip = new KafkaEmulator();
    var context = KafkaContexts.load().get("local");
    var result = zip.record(context, List.of("t1"), true);
    result.archive.save();
  }
}
