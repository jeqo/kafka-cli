package kafka.zip;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class EmulatorArchive {
  Map<TopicPartition, List<EmulatorRecord>> records = new HashMap<>();

  public static EmulatorArchive create() {
    return new EmulatorArchive();
  }

  public void append(TopicPartition topicPartition, EmulatorRecord zipRecord) {
    records.computeIfPresent(topicPartition, (topicPartition1, zipRecords) -> {
      zipRecords.add(zipRecord);
      return zipRecords;
    });
    records.computeIfAbsent(topicPartition, tp -> {
      final var zipRecords = new ArrayList<EmulatorRecord>();
      zipRecords.add(zipRecord);
      return zipRecords;
    });
  }

  public boolean isDone() {
    return false;
  }

  record EmulatorRecord(
      String topic,
      int partition,
      long afterMs,
      FieldFormat keyFormat,
      byte[] key,
      FieldFormat valueFormat,
      byte[] value) {
    public static EmulatorRecord from(ConsumerRecord<byte[], byte[]> record, long afterMs) {
      return new EmulatorRecord(record.topic(), record.partition(), afterMs, FieldFormat.BYTES, record.key(), FieldFormat.BYTES, record.value());
    }
  }

  enum FieldFormat {
    STRING,
    LONG,
    BYTES
  }
}
