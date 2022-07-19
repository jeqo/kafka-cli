package kafka.cli.producer.datagen.command;

import static java.lang.System.out;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import kafka.cli.producer.datagen.Cli;
import kafka.cli.producer.datagen.IntervalRunner;
import kafka.cli.producer.datagen.PayloadGenerator;
import kafka.cli.producer.datagen.Stats;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@CommandLine.Command(name = "interval", description = "run producer with interval")
public class IntervalCommand implements Callable<Integer> {

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
  long intervalMs;

  @CommandLine.ArgGroup(multiplicity = "1")
  Cli.PropertiesOption propertiesOption;

  @CommandLine.Option(
      names = {"-f", "--format"},
      description = "Record value format",
      defaultValue = "JSON")
  PayloadGenerator.Format format;

  @CommandLine.ArgGroup(multiplicity = "1")
  Cli.SchemaSourceOption schemaSource;

  @CommandLine.Option(
      names = {"-p", "--prop"},
      description = "Additional client properties")
  Map<String, String> additionalProperties = new HashMap<>();

  int reportingIntervalMs = 5_000;

  @Override
  public Integer call() throws Exception {
    var producerConfig = propertiesOption.load();
    if (producerConfig == null) return 1;
    producerConfig.putAll(additionalProperties);

    var keySerializer = new StringSerializer();
    Serializer<Object> valueSerializer = PayloadGenerator.valueSerializer(format, producerConfig);

    try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
      final var payloadGenerator =
          new PayloadGenerator(
              new PayloadGenerator.Config(
                  Optional.empty(),
                  schemaSource.quickstart,
                  schemaSource.schemaPath,
                  numRecords,
                  format));
      final var stats = new Stats(numRecords, reportingIntervalMs);
      final var config = new IntervalRunner.Config(topicName, numRecords, intervalMs);

      out.println("Avro Schema used to generate records:");
      out.println(payloadGenerator.schema());

      var pp = new IntervalRunner(config, producer, payloadGenerator, stats);
      pp.start();
    }
    return 0;
  }
}
