package kafka.emulator;

import static java.lang.System.err;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import kafka.context.KafkaContexts;
import kafka.context.sr.SchemaRegistryContexts;
import picocli.CommandLine;

public class Cli {

  static class PackCommand {

    PropertiesOption propertiesOption;
    List<String> topics;
  }

  static class UnpackCommand {

    PropertiesOption propertiesOption;
    boolean dryRun; // false
    RepeatOptions repeatOptions;

    static class RepeatOptions {

      boolean repeat; // false
      long afterMs;
    }
  }

  static class PropertiesOption {

    @CommandLine.Option(
      names = { "-c", "--config" },
      description = "Client configuration properties file." +
      "Must include connection to Kafka"
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

    @CommandLine.Option(
      names = "--kafka",
      description = "Kafka context name",
      required = true
    )
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

  static class VersionProviderWithConfigProvider implements CommandLine.IVersionProvider {

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
