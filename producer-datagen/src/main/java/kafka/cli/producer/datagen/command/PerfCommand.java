package kafka.cli.producer.datagen.command;

import static java.lang.System.out;

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
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@CommandLine.Command(name = "perf", description = "run performance tests")
public class PerfCommand implements Callable<Integer> {

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

  @CommandLine.ArgGroup(multiplicity = "1")
  Cli.PropertiesOption propertiesOption;

  @CommandLine.ArgGroup(multiplicity = "1")
  Cli.SchemaSourceOption schemaSource;

  @CommandLine.Option(names = { "-f", "--format" }, description = "Record value format", defaultValue = "JSON")
  PayloadGenerator.Format format;

  @CommandLine.Option(names = { "-p", "--prop" }, description = "Additional client properties")
  Map<String, String> additionalProperties = new HashMap<>();

  int reportingIntervalMs = 5_000;
  boolean shouldPrintMetrics = false;

  boolean transactionEnabled = false;
  long transactionDurationMs = 100L;

  @Override
  public Integer call() {
    var producerConfig = propertiesOption.load();
    if (producerConfig == null) return 1;
    producerConfig.putAll(additionalProperties);

    var keySerializer = new StringSerializer();
    var valueSerializer = PayloadGenerator.valueSerializer(format, producerConfig);

    try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
      final var config = new PerformanceRunner.Config(
        numRecords,
        topicName,
        transactionEnabled,
        transactionDurationMs,
        shouldPrintMetrics
      );
      final var payloadGenerator = new PayloadGenerator(
        new PayloadGenerator.Config(
          Optional.empty(),
          schemaSource.quickstart,
          schemaSource.schemaPath,
          numRecords,
          format
        )
      );
      final var throughputThrottler = new ThroughputThrottler(System.currentTimeMillis(), throughput);
      final var stats = new Stats(numRecords, reportingIntervalMs);

      out.println("Avro Schema used to generate records:");
      out.println(payloadGenerator.schema());

      var pp = new PerformanceRunner(config, producer, payloadGenerator, throughputThrottler, stats);
      pp.start();
    }
    return 0;
  }
}
