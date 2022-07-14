package kafka.zip;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class ZipArchive {
  Map<TopicPartition, List<ZipRecord>> records;

  public static ZipArchive create() {
    return new ZipArchive();
  }

  public void append(TopicPartition topicPartition, ZipRecord zipRecord) {
    records.computeIfPresent(topicPartition, (topicPartition1, zipRecords) -> {
      zipRecords.add(zipRecord);
      return zipRecords;
    });
    records.computeIfAbsent(topicPartition, tp -> {
      final var zipRecords = new ArrayList<ZipRecord>();
      zipRecords.add(zipRecord);
      return zipRecords;
    });
  }

  public boolean isDone() {
    return false;
  }

  record ZipRecord(
      String topic,
      int partition,
      long afterMs,
      FieldFormat keyFormat,
      byte[] key,
      FieldFormat valueFormat,
      byte[] value) {
    public static ZipRecord from(ConsumerRecord<byte[], byte[]> record, long afterMs) {
      return new ZipRecord(record.topic(), record.partition(), afterMs, FieldFormat.BYTES, record.key(), FieldFormat.BYTES, record.value());
    }
  }

  enum FieldFormat {
    STRING,
    LONG,
    BYTES
  }
}
