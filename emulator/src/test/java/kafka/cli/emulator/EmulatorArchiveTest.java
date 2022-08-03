package kafka.cli.emulator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import kafka.cli.emulator.EmulatorArchive.EmulatorRecord;
import kafka.cli.emulator.EmulatorArchive.FieldFormat;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class EmulatorArchiveTest {

  @Test
  void shouldFilterIncludedTopicPartitions() {
    var tp1 = new TopicPartition("t1", 0);
    var tp2 = new TopicPartition("t2", 0);
    var archive = EmulatorArchive.create();
    final var r1 = new EmulatorRecord("t1", 0, 0L, 0L, 0L, FieldFormat.BYTES, null, FieldFormat.BYTES, null);
    final var r2 = new EmulatorRecord("t2", 0, 0L, 0L, 0L, FieldFormat.BYTES, null, FieldFormat.BYTES, null);
    archive.append(tp1, r1);
    archive.append(tp2, r2);

    archive.setIncludeTopics(List.of("t1"));

    var tps = archive.topicPartitions();
    assertThat(tps).containsExactly(tp1);
  }

  @Test
  void shouldFilterExcludedTopicPartitions() {
    var tp1 = new TopicPartition("t1", 0);
    var tp2 = new TopicPartition("t2", 0);
    var archive = EmulatorArchive.create();
    final var r1 = new EmulatorRecord("t1", 0, 0L, 0L, 0L, FieldFormat.BYTES, null, FieldFormat.BYTES, null);
    final var r2 = new EmulatorRecord("t2", 0, 0L, 0L, 0L, FieldFormat.BYTES, null, FieldFormat.BYTES, null);
    archive.append(tp1, r1);
    archive.append(tp2, r2);

    archive.setExcludeTopics(List.of("t1"));

    var tps = archive.topicPartitions();
    assertThat(tps).containsExactly(tp2);
  }

  @Test
  void shouldGetAllTopicPartitions() {
    var tp1 = new TopicPartition("t1", 0);
    var tp2 = new TopicPartition("t2", 0);
    var archive = EmulatorArchive.create();
    final var r1 = new EmulatorRecord("t1", 0, 0L, 0L, 0L, FieldFormat.BYTES, null, FieldFormat.BYTES, null);
    final var r2 = new EmulatorRecord("t2", 0, 0L, 0L, 0L, FieldFormat.BYTES, null, FieldFormat.BYTES, null);
    archive.append(tp1, r1);
    archive.append(tp2, r2);

    var tps = archive.topicPartitions();
    assertThat(tps).containsExactly(tp1, tp2);
  }
}
