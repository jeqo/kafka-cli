package kafka.emulator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class EmulatorArchive {

  private final Map<TopicPartition, List<EmulatorRecord>> records = new HashMap<>();
  private final Map<TopicPartition, Long> oldestOffsets = new HashMap<>();
  private final Map<TopicPartition, Long> oldestTimestamps = new HashMap<>();

  public static EmulatorArchive create() {
    return new EmulatorArchive();
  }

  public void append(TopicPartition topicPartition, EmulatorRecord zipRecord) {
    records.computeIfPresent(
      topicPartition,
      (tp, zipRecords) -> {
        zipRecords.add(zipRecord);
        oldestOffsets.put(
          tp,
          oldestOffsets.get(tp) < zipRecord.offset()
            ? zipRecord.offset()
            : oldestOffsets.get(tp)
        );
        oldestTimestamps.put(
          tp,
          oldestTimestamps.get(tp) < zipRecord.timestamp()
            ? zipRecord.timestamp()
            : oldestTimestamps.get(tp)
        );
        return zipRecords;
      }
    );
    records.computeIfAbsent(
      topicPartition,
      tp -> {
        final var zipRecords = new ArrayList<EmulatorRecord>();
        zipRecords.add(zipRecord);
        oldestOffsets.put(tp, zipRecord.offset());
        oldestTimestamps.put(tp, zipRecord.timestamp());
        return zipRecords;
      }
    );
  }

  public Set<TopicPartition> topicPartitions() {
    return records.keySet();
  }

  public Map<String, Integer> topicPartitionNumber() {
    final var map = new HashMap<String, Integer>();
    for (var tp : records.keySet()) {
      final var partitions = tp.partition() + 1;
      map.computeIfPresent(tp.topic(), (t, p) -> partitions > p ? partitions : p);
      map.putIfAbsent(tp.topic(), partitions);
    }
    return map;
  }

  public Collection<List<EmulatorRecord>> all() {
    return records.values();
  }

  public List<EmulatorRecord> records(TopicPartition tp) {
    return records.get(tp);
  }

  public Long oldestOffsets(TopicPartition tp) {
    return oldestOffsets.get(tp);
  }

  public Long oldestTimestamps(TopicPartition tp) {
    return oldestTimestamps.get(tp);
  }

  record EmulatorRecord(
    String topic,
    int partition,
    long offset,
    long timestamp,
    long afterMs,
    FieldFormat keyFormat,
    byte[] key,
    FieldFormat valueFormat,
    byte[] value
  ) {
    public static EmulatorRecord from(
      ConsumerRecord<byte[], byte[]> record,
      FieldFormat keyFormat,
      FieldFormat valueFormat,
      long afterMs
    ) {
      return new EmulatorRecord(
        record.topic(),
        record.partition(),
        record.offset(),
        record.timestamp(),
        afterMs,
        keyFormat,
        record.key(),
        valueFormat,
        record.value()
      );
    }
  }

  enum FieldFormat {
    STRING,
    LONG,
    INTEGER,
    BYTES,
  }
}
