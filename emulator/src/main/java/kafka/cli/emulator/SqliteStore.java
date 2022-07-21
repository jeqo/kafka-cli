package kafka.cli.emulator;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;

public class SqliteStore {

  final Path archivePath;

  public SqliteStore(Path archivePath) {
    this.archivePath = archivePath;
  }

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
        final var keyFormat = EmulatorArchive.FieldFormat.valueOf(
          rs.getString("key_format")
        );
        final var valueFormat = EmulatorArchive.FieldFormat.valueOf(
          rs.getString("value_format")
        );

        archive.append(
          rs.getString("topic"),
          rs.getInt("partition"),
          rs.getLong("offset"),
          rs.getLong("timestamp"),
          rs.getLong("after_ms"),
          keyFormat,
          valueFormat,
          rs.getBytes("key_bytes"),
          rs.getBytes("value_bytes"),
          rs.getString("key_string"),
          rs.getString("value_string"),
          rs.getInt("key_int"),
          rs.getInt("value_int"),
          rs.getLong("key_long"),
          rs.getLong("value_long")
        );
      }
      return archive;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

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
                            value_long long,
                            key_sr_avro text,
                            value_sr_avro text
                        )"""
      );
      st.executeUpdate("""
              CREATE TABLE IF NOT EXISTS schemas_v1
              (
                topic text not null,
                is_key boolean not null,
                schema_type text not null,
                schema text not null
              )""");
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
      st.executeUpdate(
              """
                              CREATE INDEX IF NOT EXISTS schemas_v1_topic
                              ON schemas_v1 (topic)"""
      );
      // prepare schemas batch
      final var schemaPs = conn.prepareStatement("""
              INSERT INTO schemas_v1 (
                topic,
                is_key,
                schema_type,
                schema
              )
              VALUES (
                ?, ?, ?, ?
              )""");
      archive.keySchemas.forEach((s, schema) -> {
        try {
          schemaPs.setString(1, schema.topic());
          schemaPs.setBoolean(2, schema.isKey());
          schemaPs.setString(3, schema.type());
          schemaPs.setString(4, schema.schema());
          schemaPs.addBatch();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
      archive.valueSchemas.forEach((s, schema) -> {
        try {
          schemaPs.setString(1, schema.topic());
          schemaPs.setBoolean(2, schema.isKey());
          schemaPs.setString(3, schema.type());
          schemaPs.setString(4, schema.schema());
          schemaPs.addBatch();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
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
                                          value_long,
                                          key_sr_avro,
                                          value_sr_avro
                                        )
                                        VALUES (
                                          ?, ?, ?, ?, ?, ?, ?,
                                          ?, ?,
                                          ?, ?,
                                          ?, ?,
                                          ?, ?,
                                          ?, ?
                                        )"""
      );
      schemaPs.executeBatch();
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
                case STRING -> ps.setString(10, archive.keyAsString(r));
                case INTEGER -> ps.setInt(12, archive.keyAsInt(r));
                case LONG -> ps.setLong(14, archive.keyAsLong(r));
                case SR_AVRO -> ps.setString(16, archive.keyAsString(r));
              }
            }
            if (r.value() != null) {
              switch (r.valueFormat()) {
                case BYTES -> ps.setBytes(9, r.value());
                case STRING -> ps.setString(11, archive.valueAsString(r));
                case INTEGER -> ps.setInt(13, archive.valueAsInt(r));
                case LONG -> ps.setLong(15, archive.valueAsLong(r));
                case SR_AVRO -> ps.setString(17, archive.valueAsString(r));
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
}
