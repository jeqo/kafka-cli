package kafka.cli.producer.datagen;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import kafka.cli.producer.datagen.PayloadGenerator.Format;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

// Mostly from:
// https://github.com/apache/kafka/blob/d706d6cac4622153973d131417e809ee57c60de0/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java
public class PerformanceRun {

  final Config config;
  final KafkaProducer<String, Object> producer;
  final PayloadGenerator payloadGenerator;
  final ThroughputThrottler throttler;
  final Stats stats;

  public PerformanceRun(
      final Config config,
      final KafkaProducer<String, Object> producer,
      final PayloadGenerator payloadGenerator,
      final ThroughputThrottler throughputThrottler,
      final Stats stats) {
    this.config = config;
    this.producer = producer;
    this.payloadGenerator = payloadGenerator;
    this.throttler = throughputThrottler;
    this.stats = stats;
  }

  void start() {

    GenericRecord payload;
    Object value;
    String key;
    ProducerRecord<String, Object> record;

    int currentTransactionSize = 0;
    long transactionStartTime = 0;

    var sample = payloadGenerator.sample();

    for (long i = 0; i < config.records(); i++) {
      payload = payloadGenerator.get();
      key = payloadGenerator.key(payload);

      if (payloadGenerator.config.format().equals(Format.AVRO)) {
        value = payload;
      } else {
        value = payloadGenerator.toJson(payload);
      }

      if (config.transactionsEnabled() && currentTransactionSize == 0) {
        producer.beginTransaction();
        transactionStartTime = System.currentTimeMillis();
      }

      record = new ProducerRecord<>(config.topicName(), key, value);

      var sendStartMs = System.currentTimeMillis();
      var cb = stats.nextCompletion(sendStartMs, sample.length, stats);
      producer.send(record, cb);

      currentTransactionSize++;
      if (config.transactionsEnabled()
          && config.transactionDurationMs() <= (sendStartMs - transactionStartTime)) {
        producer.commitTransaction();
        currentTransactionSize = 0;
      }

      if (throttler.shouldThrottle(i, sendStartMs)) {
        throttler.throttle();
      }
    }

    if (config.transactionsEnabled() && currentTransactionSize != 0) producer.commitTransaction();

    if (!config.shouldPrintMetrics()) {
      producer.close();

      /* print final results */
      stats.printTotal();
    } else {
      // Make sure all messages are sent before printing out the stats and the metrics
      // We need to do this in a different branch for now since
      // tests/kafkatest/sanity_checks/test_performance_services.py
      // expects this class to work with older versions of the client jar that don't support
      // flush().
      producer.flush();

      /* print final results */
      stats.printTotal();

      /* print out metrics */
      ToolsUtils.printMetrics(producer.metrics());
      producer.close();
    }
  }

  record Config(
      long records,
      String topicName,
      boolean transactionsEnabled,
      long transactionDurationMs,
      boolean shouldPrintMetrics) {}

  public static void main(String[] args) throws IOException {
    var producerConfig = new Properties();
    producerConfig.load(Files.newInputStream(Path.of("client.properties")));
    var producer = new KafkaProducer<String, Object>(producerConfig);
    final var records = 1_000_000;
    final var targetThroughput = 10_000;
    var pp =
        new PerformanceRun(
            new Config(records, "jeqo-test-v1", false, 100, false),
            producer,
            new PayloadGenerator(
                new PayloadGenerator.Config(
                    Optional.empty(),
                    Optional.of(Quickstart.CLICKSTREAM),
                    Optional.empty(),
                    Optional.empty(),
                    records,
                    Format.AVRO)),
            new ThroughputThrottler(System.currentTimeMillis(), targetThroughput),
            new Stats(records, 5000));
    pp.start();
  }
}
