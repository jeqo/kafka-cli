package kafka.cli.emulator;

import static java.lang.System.err;
import static java.lang.System.out;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.context.KafkaContexts;
import kafka.context.sr.SchemaRegistryContexts;
import org.apache.kafka.common.TopicPartition;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
  name = "kfk-emulator",
  descriptionHeading = "Kafka emulator",
  description = """
                Record and replay topic events
                """,
  versionProvider = Cli.VersionProviderWithConfigProvider.class,
  mixinStandardHelpOptions = true,
  subcommands = { Cli.InitCommand.class, Cli.RecordCommand.class, Cli.ReplayCommand.class }
)
public class Cli implements Callable<Integer> {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cli()).setCaseInsensitiveEnumValuesAllowed(true).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    CommandLine.usage(this, out);
    return 0;
  }

  @CommandLine.Command(
    name = "init",
    description = """
                Initialize emualator archive file
                """
  )
  static class InitCommand implements Callable<Integer> {

    @CommandLine.Parameters(
      index = "0",
      description = """
                    Path to emulator archive""",
      defaultValue = "./kfk-emulator.db"
    )
    Path archivePath;

    @Override
    public Integer call() {
      try {
        final var store = new SqliteStore(archivePath);
        store.save(EmulatorArchive.create());
        return 0;
      } catch (Exception e) {
        e.printStackTrace();
        return 1;
      }
    }
  }

  @CommandLine.Command(name = "record", description = """
                Record topic events
                """)
  static class RecordCommand implements Callable<Integer> {

    @CommandLine.ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @CommandLine.Parameters(
      index = "0",
      description = """
                    Path to emulator archive""",
      defaultValue = "./kfk-emulator.db"
    )
    Path archivePath;

    @CommandLine.Option(names = { "-t", "--topic" })
    List<String> topics;

    @CommandLine.Option(
      names = { "-k", "--key-format" },
      description = "Key format recorded, valid values: ${COMPLETION-CANDIDATES}"
    )
    Optional<EmulatorArchive.FieldFormat> keyFormat;

    @CommandLine.Option(
      names = { "-v", "--value-format" },
      description = "Value format recorded, valid values: ${COMPLETION-CANDIDATES}"
    )
    Optional<EmulatorArchive.FieldFormat> valueFormat;

    @CommandLine.Option(
      names = { "-p", "--poll-timeout" },
      description = "Seconds to wait for data to arrive to topic partitions",
      defaultValue = "5"
    )
    int pollTimeout;

    @CommandLine.ArgGroup
    StartFromOption startFrom = new StartFromOption();

    @CommandLine.ArgGroup
    EndAtOption endAt = new EndAtOption();

    @Override
    public Integer call() {
      try {
        final var store = new SqliteStore(archivePath);
        final var emu = new KafkaRecorder();
        final var archive = emu.record(
          propertiesOption.load(),
          topics,
          pollTimeout,
          keyFormat.orElse(EmulatorArchive.FieldFormat.BYTES),
          valueFormat.orElse(EmulatorArchive.FieldFormat.BYTES),
          startFrom.build(),
          endAt.build()
        );
        store.save(archive);
        return 0;
      } catch (Exception e) {
        e.printStackTrace();
        return 1;
      }
    }
  }

  @CommandLine.Command(name = "replay", description = """
                Replay topic events
                """)
  static class ReplayCommand implements Callable<Integer> {

    @CommandLine.Parameters(
      index = "0",
      description = """
                    Path to emulator archive""",
      defaultValue = "./kfk-emulator.db"
    )
    Path archivePath;

    @CommandLine.Option(names = { "-t", "--topic-mapping" })
    Map<String, String> topicMap = new HashMap<>();

    @Option(names = { "--include" })
    List<String> includes = new ArrayList<>();

    @Option(names = { "--exclude" })
    List<String> excludes = new ArrayList<>();

    @CommandLine.ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @CommandLine.Option(names = { "--dry-run" })
    boolean dryRun; // false

    @CommandLine.Option(names = { "--no-wait" })
    boolean noWait; // false

    @Override
    public Integer call() {
      try {
        final var store = new SqliteStore(archivePath);
        final var properties = propertiesOption.load();
        // load archive
        var archive = store.load(properties);
        archive.setIncludeTopics(includes);
        archive.setExcludeTopics(excludes);
        final var emu = new KafkaReplayer();
        emu.replay(properties, archive, topicMap, noWait, dryRun);
        return 0;
      } catch (Exception e) {
        e.printStackTrace();
        return 1;
      }
    }
  }

  static class StartFromOption {

    @CommandLine.Option(names = { "--start-from-ts" })
    Optional<LocalDateTime> fromTime;

    @Option(names = { "--start-from-offsets" })
    Map<String, Long> offsets = new HashMap<>();

    public KafkaRecorder.RecordStartFrom build() {
      if (fromTime.isPresent()) {
        return KafkaRecorder.RecordStartFrom.of(fromTime.get().toInstant(ZoneOffset.UTC));
      }
      if (!offsets.isEmpty()) {
        return KafkaRecorder.RecordStartFrom.of(
          offsets
            .keySet()
            .stream()
            .collect(
              toMap(
                k -> {
                  var t = k.substring(0, k.lastIndexOf(":"));
                  var o = Integer.parseInt(k.substring(k.lastIndexOf(":") + 1));
                  return new TopicPartition(t, o);
                },
                k -> offsets.get(k)
              )
            )
        );
      }
      return KafkaRecorder.RecordStartFrom.of();
    }
  }

  static class EndAtOption {

    @Option(names = { "--end-now" }, defaultValue = "false")
    boolean endNow;

    @Option(names = { "-n", "--records" }, description = "Per partition")
    Optional<Integer> recordsPerPartition;

    @CommandLine.Option(names = { "--end-at-ts" }, description = "Local date time format, e.g. ")
    Optional<LocalDateTime> toTime;

    @Option(names = { "--end-at-offsets" })
    Map<String, Long> offsets = new HashMap<>();

    public KafkaRecorder.RecordEndAt build() {
      if (toTime.isPresent()) {
        return KafkaRecorder.RecordEndAt.of(toTime.get().toInstant(ZoneOffset.UTC));
      }
      if (recordsPerPartition.isPresent()) {
        return KafkaRecorder.RecordEndAt.of(recordsPerPartition.get());
      }
      if (!offsets.isEmpty()) {
        return KafkaRecorder.RecordEndAt.of(
          offsets
            .keySet()
            .stream()
            .collect(
              toMap(
                k -> {
                  var t = k.substring(0, k.lastIndexOf(":"));
                  var o = Integer.parseInt(k.substring(k.lastIndexOf(":") + 1));
                  return new TopicPartition(t, o);
                },
                k -> offsets.get(k)
              )
            )
        );
      }
      return KafkaRecorder.RecordEndAt.of(endNow);
    }
  }

  static class PropertiesOption {

    @CommandLine.Option(
      names = { "-c", "--config" },
      description = "Client configuration properties file." + "Must include connection to Kafka"
    )
    Optional<Path> configPath;

    @CommandLine.ArgGroup(exclusive = false)
    ContextOption contextOption;

    public Properties load() {
      return configPath
        .map(path -> {
          try {
            final var p = new Properties();
            p.load(Files.newInputStream(path));
            return p;
          } catch (Exception e) {
            throw new IllegalArgumentException("ERROR: properties file at %s is failing to load".formatted(path));
          }
        })
        .orElseGet(() -> {
          try {
            return contextOption.load();
          } catch (IOException e) {
            throw new IllegalArgumentException("ERROR: loading contexts");
          }
        });
    }
  }

  static class ContextOption {

    @CommandLine.Option(names = "--kafka", description = "Kafka context name", required = true)
    String kafkaContextName;

    @CommandLine.Option(names = "--sr", description = "Schema Registry context name")
    Optional<String> srContextName;

    public Properties load() throws IOException {
      final var kafkas = KafkaContexts.load();
      final var props = new Properties();
      if (kafkas.has(kafkaContextName)) {
        final var kafka = kafkas.get(kafkaContextName);
        final var kafkaProps = kafka.properties();
        props.putAll(kafkaProps);

        if (srContextName.isPresent()) {
          final var srs = SchemaRegistryContexts.load();
          final var srName = srContextName.get();
          if (srs.has(srName)) {
            final var sr = srs.get(srName);
            final var srProps = sr.properties();
            props.putAll(srProps);
          } else {
            err.printf("WARN: Schema Registry context `%s` not found. Proceeding without it.%n", srName);
          }
        }

        return props;
      } else {
        err.printf("ERROR: Kafka context `%s` not found. Check that context already exist.%n", kafkaContextName);
        return null;
      }
    }
  }

  static class VersionProviderWithConfigProvider implements CommandLine.IVersionProvider {

    @Override
    public String[] getVersion() throws IOException {
      final var url = VersionProviderWithConfigProvider.class.getClassLoader().getResource("cli.properties");
      if (url == null) {
        return new String[] { "No cli.properties file found in the classpath." };
      }
      final var properties = new Properties();
      properties.load(url.openStream());
      return new String[] {
        properties.getProperty("appName") + " version " + properties.getProperty("appVersion") + "",
        "Built: " + properties.getProperty("appBuildTime"),
      };
    }
  }
}
