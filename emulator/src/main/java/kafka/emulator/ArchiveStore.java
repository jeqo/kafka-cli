package kafka.emulator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public interface ArchiveStore {
  EmulatorArchive load();

  void save(EmulatorArchive archive);

  class SqliteArchiveLoader implements ArchiveStore {

    final StringDeserializer stringDeserializer = new StringDeserializer();
    final LongDeserializer longDeserializer = new LongDeserializer();
    final IntegerDeserializer intDeserializer = new IntegerDeserializer();
    final StringSerializer stringSerializer = new StringSerializer();
    final LongSerializer longSerializer = new LongSerializer();
    final IntegerSerializer intSerializer = new IntegerSerializer();

    final Path archivePath;

    public SqliteArchiveLoader(Path archivePath) {
      this.archivePath = archivePath;
    }

    @Override
    public EmulatorArchive load() {
      final var db = "jdbc:sqlite:" + archivePath.toAbsolutePath();
      try (final var conn = DriverManager.getConnection(db)) {
        final var archive = EmulatorArchive.create();
        final var st = conn.createStatement();
        final var rs = st.executeQuery(
          """
                        SELECT *
                        FROM records_v1
                        ORDER BY offset ASC"""
        );
        while (rs.next()) {
          final var tp = new TopicPartition(
            rs.getString("topic"),
            rs.getInt("partition")
          );
          final var keyFormat = EmulatorArchive.FieldFormat.valueOf(
            rs.getString("key_format")
          );
          final var key =
            switch (keyFormat) {
              case BYTES -> rs.getBytes("key_bytes");
              case INTEGER -> intSerializer.serialize("", rs.getInt("key_int"));
              case LONG -> longSerializer.serialize("", rs.getLong("key_long"));
              case STRING -> stringSerializer.serialize("", rs.getString("key_string"));
            };
          final var valueFormat = EmulatorArchive.FieldFormat.valueOf(
            rs.getString("value_format")
          );
          final var value =
            switch (valueFormat) {
              case BYTES -> rs.getBytes("value_bytes");
              case INTEGER -> intSerializer.serialize("", rs.getInt("value_int"));
              case LONG -> longSerializer.serialize("", rs.getLong("value_long"));
              case STRING -> stringSerializer.serialize("", rs.getString("value_string"));
            };
          final var record = new EmulatorArchive.EmulatorRecord(
            tp.topic(),
            tp.partition(),
            rs.getLong("offset"),
            rs.getLong("timestamp"),
            rs.getLong("after_ms"),
            keyFormat,
            key,
            valueFormat,
            value
          );
          archive.append(tp, record);
        }
        return archive;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void save(EmulatorArchive archive) {
      final var db = "jdbc:sqlite:" + archivePath.toAbsolutePath();
      try (final var conn = DriverManager.getConnection(db)) {
        final var st = conn.createStatement();
        // Prepare schema
        st.executeUpdate(
          """
                        CREATE TABLE IF NOT EXISTS records_v1
                        (
                            topic text not null,
                            partition int not null,
                            offset long,
                            timestamp long,
                            after_ms long not null,
                            key_format text not null,
                            value_format text not null,
                            key_bytes bytes,
                            value_bytes bytes,
                            key_string text,
                            value_string text,
                            key_int int,
                            value_int int,
                            key_long long,
                            value_long long
                        )"""
        );
        st.executeUpdate(
          """
                        CREATE INDEX IF NOT EXISTS records_v1_topic
                        ON records_v1 (topic)"""
        );
        st.executeUpdate(
          """
                        CREATE INDEX IF NOT EXISTS records_v1_partition
                        ON records_v1 (partition)"""
        );
        st.executeUpdate(
          """
                        CREATE INDEX IF NOT EXISTS records_v1_offset
                        ON records_v1 (offset)"""
        );
        // prepare batch
        final var ps = conn.prepareStatement(
          """
                                        INSERT INTO records_v1 (
                                          topic,
                                          partition,
                                          offset,
                                          timestamp,
                                          after_ms,
                                          key_format,
                                          value_format,
                                          key_bytes,
                                          value_bytes,
                                          key_string,
                                          value_string,
                                          key_int,
                                          value_int,
                                          key_long,
                                          value_long
                                        )
                                        VALUES (
                                          ?, ?, ?, ?, ?, ?, ?,
                                          ?, ?,
                                          ?, ?,
                                          ?, ?,
                                          ?, ?
                                        )"""
        );
        archive
          .all()
          .parallelStream()
          .flatMap(Collection::stream)
          .forEach(r -> {
            try {
              ps.setString(1, r.topic());
              ps.setInt(2, r.partition());
              ps.setLong(3, r.offset());
              ps.setLong(4, r.timestamp());
              ps.setLong(5, r.afterMs());
              ps.setString(6, r.keyFormat().name());
              ps.setString(7, r.valueFormat().name());
              if (r.key() != null) {
                switch (r.keyFormat()) {
                  case BYTES -> ps.setBytes(8, r.key());
                  case STRING -> ps.setString(
                    10,
                    stringDeserializer.deserialize("", r.key())
                  );
                  case INTEGER -> ps.setInt(12, intDeserializer.deserialize("", r.key()));
                  case LONG -> ps.setLong(14, longDeserializer.deserialize("", r.key()));
                }
              }
              if (r.value() != null) {
                switch (r.valueFormat()) {
                  case BYTES -> ps.setBytes(9, r.value());
                  case STRING -> ps.setString(
                    11,
                    stringDeserializer.deserialize("", r.value())
                  );
                  case INTEGER -> ps.setInt(
                    13,
                    intDeserializer.deserialize("", r.value())
                  );
                  case LONG -> ps.setLong(
                    15,
                    longDeserializer.deserialize("", r.value())
                  );
                }
              }
              ps.addBatch();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          });
        ps.executeBatch();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    public static void main(String[] args) {
      var loader = new SqliteArchiveLoader(Path.of("test.db"));
      var archive = EmulatorArchive.create();
      archive.append(
        new TopicPartition("t1", 0),
        new EmulatorArchive.EmulatorRecord(
          "t1",
          0,
          0L,
          System.currentTimeMillis(),
          100L,
          EmulatorArchive.FieldFormat.BYTES,
          "s".getBytes(StandardCharsets.UTF_8),
          EmulatorArchive.FieldFormat.BYTES,
          "v".getBytes(StandardCharsets.UTF_8)
        )
      );
      loader.save(archive);

      var archive2 = loader.load();

      System.out.println(archive2.equals(archive));
    }
  }
}
