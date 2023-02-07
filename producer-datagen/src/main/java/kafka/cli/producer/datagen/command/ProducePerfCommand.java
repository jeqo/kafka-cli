package kafka.cli.producer.datagen.command;

import static java.lang.System.out;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import kafka.cli.producer.datagen.Cli;
import kafka.cli.producer.datagen.PayloadGenerator;
import kafka.cli.producer.datagen.PerformanceRunner;
import kafka.cli.producer.datagen.Stats;
import kafka.cli.producer.datagen.ThroughputThrottler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@CommandLine.Command(name = "perf", description = "run performance tests")
public class ProducePerfCommand implements Callable<Integer> {

  @CommandLine.Option(names = { "-t", "--topic" }, description = "target Kafka topic name", required = true)
  String topicName;

  @CommandLine.Option(names = { "-n", "--num-records" }, description = "Number of records to produce", required = true)
  long numRecords;

  @CommandLine.Option(
    names = { "-k", "--throughput" },
    description = "Number of target records per second to produce",
    defaultValue = "-1"
  )
  long throughput = -1L;

  @CommandLine.ArgGroup(multiplicity = "1", exclusive = false)
  Cli.PropertiesOption propertiesOption;

  @CommandLine.ArgGroup(exclusive = false)
  Cli.SchemaOptions schemaOpts;

  @CommandLine.Option(names = { "-p", "--prop" }, description = "Additional client properties")
  Map<String, String> additionalProperties = new HashMap<>();

  int reportingIntervalMs = 5_000;
  boolean shouldPrintMetrics = false;

  boolean transactionEnabled = false;
  long transactionDurationMs = 100L;

  @CommandLine.Option(names = {"--hist"}, description = "HdrHistogram format")
  Optional<Path> hdrHistogram;

  @Override
  public Integer call() {
    var producerConfig = propertiesOption.load();
    if (producerConfig == null) return 1;
    producerConfig.putAll(additionalProperties);

    var keySerializer = new StringSerializer();
    var valueSerializer = new ByteArraySerializer();

    try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
      final var config = new PerformanceRunner.Config(
        numRecords,
        topicName,
        transactionEnabled,
        transactionDurationMs,
        shouldPrintMetrics,
        hdrHistogram
      );
      final var payloadGenerator = new PayloadGenerator(schemaOpts.config(), producerConfig);
      final var throughputThrottler = new ThroughputThrottler(System.currentTimeMillis(), throughput);
      final var stats = new Stats(numRecords, reportingIntervalMs);

      out.println("Avro Schema used to generate records:");
      out.println(payloadGenerator.schema());
      out.printf("With field [%s] as key%n", payloadGenerator.keyFieldName());

      var pp = new PerformanceRunner(config, producer, payloadGenerator, throughputThrottler, stats);
      pp.start();
    }
    return 0;
  }
}
