package kafka.zip;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.context.KafkaContexts;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class KafkaZip {

  /**
   * Read and package topics records into an archive.
   *
   * Based on the start and end conditions, start a consumer and poll records per partition.
   * Transform into archive records and append to the archive.
   * Once the end condition is given, the results are flushed into the zip archive files.
   *
   * @param kafkaContext
   * @param topics
   * @param fromBeginning // TODO replace with startCondition
   *                      // TODO add endCondition
   * @return
   * @throws IOException
   */
  public PackResult pack(
      KafkaContexts.KafkaContext kafkaContext, List<String> topics, boolean fromBeginning)
      throws IOException {
    // create consumer
    final var properties = kafkaContext.properties();
    final var keyDeserializer = new ByteArrayDeserializer();
    final var valueDeserializer = new ByteArrayDeserializer();
    var consumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
    // set offsets from
    // consumer loop
    boolean done = false;
    long endTime = System.currentTimeMillis();
    Map<TopicPartition, Long> latestTimestamps = new HashMap<>();
    var archive = ZipArchive.create();
    while (!done) {
      // set offsets to
      // break by topic-partition
      var records = consumer.poll(Duration.ofMinutes(1));
      for (var partition : records.partitions()) {
        // start: per partition
        var perPartition = records.records(partition);
        for (var record : perPartition) {
          // transform to ZipRecord
          var latestTimestamp = latestTimestamps.getOrDefault(partition, -1L);
          final long afterMs;
          if (latestTimestamp < 0) {
            afterMs = 0;
          } else {
            afterMs = record.timestamp() - latestTimestamp;
          }
          var zipRecord = ZipArchive.ZipRecord.from(record, afterMs);
          // append to topic-partition file
          archive.append(partition, zipRecord);
          latestTimestamps.put(partition, record.timestamp());
        }
        if (isDone(archive)) {
          done = true;
          break;
        }
        // end: per partition
      }
    }
    return null;
  }

  private boolean isDone(ZipArchive archive) {
    return false;
  }

  /**
   * Read zip archive files and produce records into Kafka topics with the frequency defined in the archive.
   *
   *
   * @param kafkaContext
   * @return
   */
  public UnpackResult unpack(KafkaContexts.KafkaContext kafkaContext) {
    return null;
  }

  private class PackResult {}

  private class UnpackResult {}

  public static void main(String[] args) throws IOException {
    var zip = new KafkaZip();
    var context = KafkaContexts.load().get("local");
    var result = zip.pack(context, List.of(""), true);
  }
}
