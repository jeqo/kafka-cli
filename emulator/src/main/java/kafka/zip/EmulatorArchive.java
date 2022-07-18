package kafka.zip;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class EmulatorArchive {
  Map<TopicPartition, List<EmulatorRecord>> records = new HashMap<>();

  public static EmulatorArchive load(Path directory) throws IOException {
    try (var list = Files.list(directory)) {
      var tpToPath = list
            .filter(p -> p.endsWith(".txt"))
            .filter(p -> p.getFileName().toString().contains("-"))
            .collect(Collectors.toMap(p -> {
              var filename = p.getFileName().toString();
              var topic = filename.substring(0, filename.lastIndexOf("-") - 1);
              var partition = Integer.parseInt(
                      filename.substring(filename.lastIndexOf("-"), filename.lastIndexOf(".")));
              return new TopicPartition(topic, partition);
              }, p -> p));

    }
    return null;
  }

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

  public void save() throws IOException {
    for (var tp : records.keySet()) {
      var tpPath = Path.of(tp.topic() + "-" + tp.partition() + ".csv");
      Files.writeString(tpPath, "after_ms,key,value\n", StandardOpenOption.CREATE);
      for (var record : records.get(tp)) {
        Files.writeString(tpPath, record.toLine(), StandardOpenOption.APPEND);
      }
    }
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

    public String toLine () {
      var k = key == null ? "" : Base64.getEncoder().encodeToString(key);
      var v = value == null ? "" : Base64.getEncoder().encodeToString(value);
      return afterMs
              + "," + k
              + "," + v
              + "\n";
    }
  }

  enum FieldFormat {
    STRING,
    LONG,
    BYTES
  }
}
