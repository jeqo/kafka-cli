package kafka.cli.producer.datagen;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.cli.producer.datagen.Cli.VersionProviderWithConfigProvider;
import kafka.cli.producer.datagen.command.IntervalCommand;
import kafka.cli.producer.datagen.command.ListTopicsCommand;
import kafka.cli.producer.datagen.command.PerfCommand;
import kafka.cli.producer.datagen.command.ProduceOnceCommand;
import kafka.cli.producer.datagen.command.SampleCommand;
import kafka.context.KafkaContexts;
import kafka.context.sr.SchemaRegistryContexts;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

@CommandLine.Command(
  name = "kfk-producer-datagen",
  versionProvider = VersionProviderWithConfigProvider.class,
  mixinStandardHelpOptions = true,
  descriptionHeading = "Kafka CLI - Producer Datagen",
  description = "Kafka Producer with Data generation",
  subcommands = {
    PerfCommand.class,
    IntervalCommand.class,
    ProduceOnceCommand.class,
    SampleCommand.class,
    ListTopicsCommand.class,
  }
)
public class Cli implements Callable<Integer> {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cli()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    CommandLine.usage(this, out);
    return 0;
  }

  public static class PropertiesOption {

    @CommandLine.Option(
      names = { "-c", "--config" },
      description = "Client configuration properties file." +
      "Must include connection to Kafka and Schema Registry"
    )
    Optional<Path> configPath;

    @ArgGroup(exclusive = false)
    ContextOption contextOption;

    public Properties load() {
      return configPath
        .map(path -> {
          try {
            final var p = new Properties();
            p.load(Files.newInputStream(path));
            return p;
          } catch (Exception e) {
            throw new IllegalArgumentException(
              "ERROR: properties file at %s is failing to load".formatted(path)
            );
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

    @Option(names = "--kafka", description = "Kafka context name", required = true)
    String kafkaContextName;

    @Option(names = "--sr", description = "Schema Registry context name")
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
            err.printf(
              "WARN: Schema Registry context `%s` not found. Proceeding without it.%n",
              srName
            );
          }
        }

        return props;
      } else {
        err.printf(
          "ERROR: Kafka context `%s` not found. Check that context already exist.%n",
          kafkaContextName
        );
        return null;
      }
    }
  }

  public static class SchemaSourceOption {

    @Option(
      names = { "-q", "--quickstart" },
      description = "Quickstart name. Valid values:  ${COMPLETION-CANDIDATES}"
    )
    public Optional<PayloadGenerator.Quickstart> quickstart;

    @Option(
      names = { "-s", "--schema" },
      description = "Path to Avro schema to use for generating records."
    )
    public Optional<Path> schemaPath;
  }

  static class VersionProviderWithConfigProvider implements IVersionProvider {

    @Override
    public String[] getVersion() throws IOException {
      final var url =
        VersionProviderWithConfigProvider.class.getClassLoader()
          .getResource("cli.properties");
      if (url == null) {
        return new String[] { "No cli.properties file found in the classpath." };
      }
      final var properties = new Properties();
      properties.load(url.openStream());
      return new String[] {
        properties.getProperty("appName") +
        " version " +
        properties.getProperty("appVersion") +
        "",
        "Built: " + properties.getProperty("appBuildTime"),
      };
    }
  }
}
