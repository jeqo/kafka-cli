package kafka.cli.producer.datagen;

import static java.lang.System.err;
import static java.lang.System.out;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import kafka.cli.producer.datagen.Cli.Interval;
import kafka.cli.producer.datagen.Cli.ListTopics;
import kafka.cli.producer.datagen.Cli.Perf;
import kafka.cli.producer.datagen.Cli.ProduceOnce;
import kafka.cli.producer.datagen.Cli.Sample;
import kafka.cli.producer.datagen.Cli.VersionProviderWithConfigProvider;
import kafka.cli.producer.datagen.PayloadGenerator.Config;
import kafka.cli.producer.datagen.PayloadGenerator.Format;
import kafka.cli.producer.datagen.TopicAndSchema.SubjectSchemas;
import kafka.context.KafkaContexts;
import kafka.context.SchemaRegistryContexts;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "kfk-producer-datagen",
    versionProvider = VersionProviderWithConfigProvider.class,
    mixinStandardHelpOptions = true,
    descriptionHeading = "Kafka CLI - Producer Datagen",
    description = "Kafka Producer with Data generation",
    subcommands = {Perf.class, Interval.class, ProduceOnce.class, Sample.class, ListTopics.class})
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

  @CommandLine.Command(name = "perf", description = "run performance tests")
  static class Perf implements Callable<Integer> {

    @CommandLine.Option(
        names = {"-t", "--topic"},
        description = "target Kafka topic name",
        required = true)
    String topicName;

    @CommandLine.Option(
        names = {"-n", "--num-records"},
        description = "Number of records to produce",
        required = true)
    long numRecords;

    @CommandLine.Option(
        names = {"-k", "--throughput"},
        description = "Number of target records per second to produce",
        defaultValue = "-1")
    long throughput = -1L;

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @ArgGroup(multiplicity = "1")
    SchemaSourceOption schemaSource;

    @Option(
        names = {"-f", "--format"},
        description = "Record value format",
        defaultValue = "JSON")
    Format format;

    @Option(
        names = {"-p", "--prop"},
        description = "Additional client properties")
    Map<String, String> additionalProperties = new HashMap<>();

    int reportingInterval = 5_000;
    boolean shouldPrintMetrics = false;

    boolean transactionEnabled = false;
    long transactionDuration = 100L;

    @Override
    public Integer call() {
      var producerConfig = propertiesOption.load();
      if (producerConfig == null) return 1;
      producerConfig.putAll(additionalProperties);
      var keySerializer = new StringSerializer();
      Serializer<Object> valueSerializer;
      if (format.equals(Format.AVRO)) {
        valueSerializer = new KafkaAvroSerializer();
        valueSerializer.configure(
            producerConfig.keySet().stream()
                .collect(Collectors.toMap(String::valueOf, producerConfig::get)),
            false);
      } else {
        valueSerializer = (Serializer) new StringSerializer();
      }
      try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
        final var records = numRecords;
        final var targetThroughput = throughput;
        var pp =
            new PerformanceRun(
                new PerformanceRun.Config(
                    records,
                    topicName,
                    transactionEnabled,
                    transactionDuration,
                    shouldPrintMetrics),
                producer,
                new PayloadGenerator(
                    new PayloadGenerator.Config(
                        Optional.empty(),
                        schemaSource.quickstart,
                        schemaSource.schemaPath,
                        Optional.empty(),
                        records,
                        format)),
                new ThroughputThrottler(System.currentTimeMillis(), targetThroughput),
                new Stats(records, reportingInterval));
        pp.start();
      }
      return 0;
    }
  }

  @CommandLine.Command(name = "interval", description = "run producer with interval")
  static class Interval implements Callable<Integer> {

    @CommandLine.Option(
        names = {"-t", "--topic"},
        description = "target Kafka topic name",
        required = true)
    String topicName;

    @CommandLine.Option(
        names = {"-n", "--num-records"},
        description = "Number of records to produce",
        required = true)
    long numRecords;

    @CommandLine.Option(
        names = {"-i", "--interval"},
        description = "Maximum interval between producer send",
        defaultValue = "5000")
    long interval;

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @Option(
        names = {"-f", "--format"},
        description = "Record value format",
        defaultValue = "JSON")
    Format format;

    @ArgGroup(multiplicity = "1")
    SchemaSourceOption schemaSource;

    @Option(
        names = {"-p", "--prop"},
        description = "Additional client properties")
    Map<String, String> additionalProperties = new HashMap<>();

    int reportingInterval = 5_000;

    @Override
    public Integer call() throws Exception {
      var producerConfig = propertiesOption.load();
      if (producerConfig == null) return 1;
      producerConfig.putAll(additionalProperties);
      var keySerializer = new StringSerializer();
      Serializer<Object> valueSerializer;
      if (format.equals(Format.AVRO)) {
        valueSerializer = new KafkaAvroSerializer();
        valueSerializer.configure(
            producerConfig.keySet().stream()
                .collect(Collectors.toMap(String::valueOf, producerConfig::get)),
            false);
      } else {
        valueSerializer = (Serializer) new StringSerializer();
      }
      try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
        final var maxRecords = numRecords;
        final var maxInterval = interval;
        final var payloadGenerator =
            new PayloadGenerator(
                new Config(
                    Optional.empty(),
                    schemaSource.quickstart,
                    schemaSource.schemaPath,
                    Optional.empty(),
                    maxRecords,
                    format));
        var pp =
            new IntervalRun(
                new IntervalRun.Config(topicName, maxRecords, maxInterval),
                producer,
                payloadGenerator,
                new Stats(maxRecords, reportingInterval));
        pp.start();
      }
      return 0;
    }
  }

  @CommandLine.Command(name = "once", description = "produce once")
  static class ProduceOnce implements Callable<Integer> {

    @CommandLine.Option(
        names = {"-t", "--topic"},
        description = "target Kafka topic name",
        required = true)
    String topicName;

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @ArgGroup(multiplicity = "1")
    SchemaSourceOption schemaSource;

    @Option(
        names = {"-f", "--format"},
        description = "Record value format",
        defaultValue = "JSON")
    Format format;

    @Option(
        names = {"-p", "--prop"},
        description = "Additional client properties")
    Map<String, String> additionalProperties = new HashMap<>();

    @Override
    public Integer call() throws Exception {
      var producerConfig = propertiesOption.load();
      if (producerConfig == null) return 1;
      producerConfig.putAll(additionalProperties);
      var keySerializer = new StringSerializer();
      Serializer<Object> valueSerializer;
      if (format.equals(Format.AVRO)) {
        valueSerializer = new KafkaAvroSerializer();
        valueSerializer.configure(
            producerConfig.keySet().stream()
                .collect(Collectors.toMap(String::valueOf, producerConfig::get)),
            false);
      } else {
        valueSerializer = (Serializer) new StringSerializer();
      }
      try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
        var pg =
            new PayloadGenerator(
                new PayloadGenerator.Config(
                    Optional.empty(),
                    schemaSource.quickstart,
                    schemaSource.schemaPath,
                    Optional.empty(),
                    10,
                    format));
        var record = pg.get();
        Object value;
        if (format.equals(Format.JSON)) {
          value = pg.toJson(record);
        } else {
          value = record;
        }
        var meta = producer.send(new ProducerRecord<>(topicName, pg.key(record), value)).get();
        out.println("Record sent. " + meta);
      }
      return 0;
    }
  }

  @Command(name = "sample", description = "Get a sample of the quickstart")
  static class Sample implements Callable<Integer> {
    @ArgGroup(multiplicity = "1")
    SchemaSourceOption exclusive;

    @Option(
        names = {"--pretty"},
        defaultValue = "false",
        description = "Print pretty/formatted JSON")
    boolean pretty;

    final ObjectMapper json = new ObjectMapper();

    @Override
    public Integer call() throws Exception {
      final var payloadGenerator =
          new PayloadGenerator(
              new Config(
                  Optional.empty(),
                  exclusive.quickstart,
                  exclusive.schemaPath,
                  Optional.empty(),
                  1,
                  Format.JSON));

      final var sample = json.readTree(payloadGenerator.sample());
      if (pretty) {
        out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(sample));
      } else {
        out.println(json.writeValueAsString(sample));
      }
      return 0;
    }
  }

  @Command(name = "topics", description = "List topics and subjectSchemas available in a cluster")
  static class ListTopics implements Callable<Integer> {

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @Option(
        names = {"--pretty"},
        defaultValue = "false",
        description = "Print pretty/formatted JSON")
    boolean pretty;

    @Option(
        names = {"-p", "--prop"},
        description = "Additional client properties")
    Map<String, String> additionalProperties = new HashMap<>();

    @Option(
        names = {"--prefix"},
        description = "Topic name prefix")
    Optional<String> prefix;

    final ObjectMapper json = new ObjectMapper();

    @Override
    public Integer call() throws Exception {
      var props = propertiesOption.load();
      if (props == null) return 1;
      props.putAll(additionalProperties);
      final var kafkaAdminClient = AdminClient.create(props);
      final var topics =
          kafkaAdminClient.listTopics().names().get().stream()
              .filter(t -> prefix.map(t::startsWith).orElse(true))
              .toList();

      final var schemaRegistryUrl = props.getProperty("schema.registry.url");
      final Optional<SchemaRegistryClient> schemaRegistryClient;
      if (schemaRegistryUrl != null && !schemaRegistryUrl.isBlank()) {
        schemaRegistryClient =
            Optional.of(
                new CachedSchemaRegistryClient(
                    schemaRegistryUrl,
                    10,
                    props.keySet().stream()
                        .collect(
                            Collectors.toMap(
                                Object::toString, k -> props.getProperty(k.toString())))));
      } else {
        schemaRegistryClient = Optional.empty();
      }
      final var result = new ArrayList<TopicAndSchema>(topics.size());
      for (final var topic : topics) {
        var subject =
            schemaRegistryClient
                .map(
                    c -> {
                      try {
                        final var allSubjectsByPrefix = c.getAllSubjectsByPrefix(topic);
                        final var subjects = new HashMap<String, List<ParsedSchema>>();
                        for (final var s : allSubjectsByPrefix) {
                          try {
                            final var schemas = c.getSchemas(s, false, true);
                            subjects.put(s, schemas);
                          } catch (IOException | RestClientException e) {
                            throw new RuntimeException(e);
                          }
                        }
                        return subjects;
                      } catch (IOException | RestClientException e) {
                        throw new RuntimeException(e);
                      }
                    })
                .map(
                    parsedSchemas ->
                        parsedSchemas.entrySet().stream().map(SubjectSchemas::from).toList())
                .orElse(List.of());
        result.add(new TopicAndSchema(topic, subject));
      }
      final var array = json.createArrayNode();
      result.forEach(t -> array.add(t.toJson()));
      if (pretty) {
        out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(array));
      } else {
        out.println(json.writeValueAsString(array));
      }
      return 0;
    }
  }

  static class PropertiesOption {

    @CommandLine.Option(
        names = {"-c", "--config"},
        description =
            "Client configuration properties file."
                + "Must include connection to Kafka and Schema Registry")
    Optional<Path> configPath;

    @ArgGroup(exclusive = false)
    ContextOption contextOption;

    public Properties load() {
      return configPath
          .map(
              path -> {
                try {
                  final var p = new Properties();
                  p.load(Files.newInputStream(path));
                  return p;
                } catch (Exception e) {
                  throw new IllegalArgumentException(
                      "ERROR: properties file at %s is failing to load".formatted(path));
                }
              })
          .orElseGet(
              () -> {
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
                "WARN: Schema Registry context `%s` not found. Proceeding without it.%n", srName);
          }
        }

        return props;
      } else {
        err.printf(
            "ERROR: Kafka context `%s` not found. Check that context already exist.%n",
            kafkaContextName);
        return null;
      }
    }
  }

  static class SchemaSourceOption {

    @Option(
        names = {"-q", "--quickstart"},
        description = "Quickstart name. Valid values:  ${COMPLETION-CANDIDATES}")
    Optional<Quickstart> quickstart;

    @Option(
        names = {"-s", "--schema"},
        description = "Path to Avro schema to use for generating records.")
    Optional<Path> schemaPath;
  }

  static class VersionProviderWithConfigProvider implements IVersionProvider {

    @Override
    public String[] getVersion() throws IOException {
      final var url =
          VersionProviderWithConfigProvider.class.getClassLoader().getResource("cli.properties");
      if (url == null) {
        return new String[] {"No cli.properties file found in the classpath."};
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
