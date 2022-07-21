package kafka.cli.emulator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class EmulatorArchive {

  private final Map<TopicPartition, List<EmulatorRecord>> records = new HashMap<>();
  private final Map<TopicPartition, Long> oldestOffsets = new HashMap<>();
  private final Map<TopicPartition, Long> oldestTimestamps = new HashMap<>();

  List<String> includeTopics = new ArrayList<>();
  List<String> excludeTopics = new ArrayList<>();

  FieldFormat keyFormat;
  FieldFormat valueFormat;

  final StringDeserializer stringDeserializer = new StringDeserializer();
  final LongDeserializer longDeserializer = new LongDeserializer();
  final IntegerDeserializer intDeserializer = new IntegerDeserializer();

  final StringSerializer stringSerializer = new StringSerializer();
  final LongSerializer longSerializer = new LongSerializer();
  final IntegerSerializer intSerializer = new IntegerSerializer();

  public static EmulatorArchive create() {
    return new EmulatorArchive();
  }

  public void setExcludeTopics(List<String> excludeTopics) {
    this.excludeTopics = excludeTopics;
  }

  public void setIncludeTopics(List<String> includeTopics) {
    this.includeTopics = includeTopics;
  }

  public static EmulatorArchive with(FieldFormat keyFormat, FieldFormat valueFormat) {
    final var emulatorArchive = new EmulatorArchive();
    emulatorArchive.keyFormat = keyFormat;
    emulatorArchive.valueFormat = valueFormat;
    return emulatorArchive;
  }

  public void append(
    String topic,
    int partition,
    long offset,
    long timestamp,
    long afterMs,
    FieldFormat keyFormat,
    FieldFormat valueFormat,
    byte[] keyBytes,
    byte[] valueBytes,
    String keyString,
    String valueString,
    int keyInt,
    int valueInt,
    long keyLong,
    long valueLong
  ) {
    final var key =
      switch (keyFormat) {
        case BYTES -> keyBytes;
        case INTEGER -> intSerializer.serialize("", keyInt);
        case LONG -> longSerializer.serialize("", keyLong);
        case STRING -> stringSerializer.serialize("", keyString);
      };
    final var value =
      switch (valueFormat) {
        case BYTES -> valueBytes;
        case INTEGER -> intSerializer.serialize("", valueInt);
        case LONG -> longSerializer.serialize("", valueLong);
        case STRING -> stringSerializer.serialize("", valueString);
      };

    var emuRecord = new EmulatorRecord(
      topic,
      partition,
      offset,
      timestamp,
      afterMs,
      keyFormat,
      key,
      valueFormat,
      value
    );

    append(new TopicPartition(topic, partition), emuRecord);
  }

  public void append(
    TopicPartition topicPartition,
    ConsumerRecord<byte[], byte[]> record,
    long afterMs
  ) {
    var emuRecord = new EmulatorRecord(
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
    append(topicPartition, emuRecord);
  }

  public void append(TopicPartition topicPartition, EmulatorRecord record) {
    records.computeIfPresent(
      topicPartition,
      (tp, records) -> {
        records.add(record);
        oldestOffsets.put(
          tp,
          oldestOffsets.get(tp) < record.offset()
            ? record.offset()
            : oldestOffsets.get(tp)
        );
        oldestTimestamps.put(
          tp,
          oldestTimestamps.get(tp) < record.timestamp()
            ? record.timestamp()
            : oldestTimestamps.get(tp)
        );
        return records;
      }
    );
    records.computeIfAbsent(
      topicPartition,
      tp -> {
        final var records = new ArrayList<EmulatorRecord>();
        records.add(record);
        oldestOffsets.put(tp, record.offset());
        oldestTimestamps.put(tp, record.timestamp());
        return records;
      }
    );
  }

  public Set<TopicPartition> topicPartitions() {
    return records
      .keySet()
      .stream()
      .filter(topicPartition ->
        includeTopics.isEmpty() || includeTopics.contains(topicPartition.topic())
      )
      .filter(topicPartition ->
        excludeTopics.isEmpty() || !excludeTopics.contains(topicPartition.topic())
      )
      .collect(Collectors.toSet());
  }

  public Map<String, Integer> topicPartitionNumber() {
    final var map = new HashMap<String, Integer>();
    for (var tp : topicPartitions()) {
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

  public int keyAsInt(EmulatorRecord r) {
    return intDeserializer.deserialize(r.topic(), r.key());
  }

  public long keyAsLong(EmulatorRecord r) {
    return longDeserializer.deserialize(r.topic(), r.key());
  }

  public String keyAsString(EmulatorRecord r) {
    return stringDeserializer.deserialize(r.topic(), r.key());
  }

  public int valueAsInt(EmulatorRecord r) {
    return intDeserializer.deserialize(r.topic(), r.value());
  }

  public long valueAsLong(EmulatorRecord r) {
    return longDeserializer.deserialize(r.topic(), r.value());
  }

  public String valueAsString(EmulatorRecord r) {
    return stringDeserializer.deserialize(r.topic(), r.value());
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
