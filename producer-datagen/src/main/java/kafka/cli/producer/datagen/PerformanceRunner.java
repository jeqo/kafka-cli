package kafka.cli.producer.datagen;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

// Mostly from:
// https://github.com/apache/kafka/blob/d706d6cac4622153973d131417e809ee57c60de0/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java
public class PerformanceRunner {

  final Config config;
  final KafkaProducer<String, byte[]> producer;
  final PayloadGenerator payloadGenerator;
  final ThroughputThrottler throttler;
  final Stats stats;

  public PerformanceRunner(
    final Config config,
    final KafkaProducer<String, byte[]> producer,
    final PayloadGenerator payloadGenerator,
    final ThroughputThrottler throughputThrottler,
    final Stats stats
  ) {
    this.config = config;
    this.producer = producer;
    this.payloadGenerator = payloadGenerator;
    this.throttler = throughputThrottler;
    this.stats = stats;
  }

  public void start() {
    GenericRecord payload;
    Object value;
    String key;
    ProducerRecord<String, byte[]> record;

    int currentTransactionSize = 0;
    long transactionStartTime = 0;

    var sample = payloadGenerator.sample();

    for (long i = 0; i < config.records(); i++) {
      if (config.transactionsEnabled() && currentTransactionSize == 0) {
        producer.beginTransaction();
        transactionStartTime = System.currentTimeMillis();
      }

      record = payloadGenerator.record(config.topicName());

      var sendStartMs = System.currentTimeMillis();
      var cb = stats.nextCompletion(sendStartMs, sample.length, stats);
      producer.send(record, cb);

      currentTransactionSize++;
      if (config.transactionsEnabled() && config.transactionDurationMs() <= (sendStartMs - transactionStartTime)) {
        producer.commitTransaction();
        currentTransactionSize = 0;
      }

      if (throttler.shouldThrottle(i, sendStartMs)) {
        throttler.throttle();
      }
    }

    if (config.transactionsEnabled() && currentTransactionSize != 0) producer.commitTransaction();

    if (!config.shouldPrintMetrics()) {
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

      printMetrics(producer.metrics());
    }
    if (config.hdrHistogram().isPresent()) {
      try {
        stats.printHdrHistogram(new PrintStream(config.hdrHistogram().get().toFile()));
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * print out the metrics in alphabetical order
   *
   * @param metrics the metrics to be printed out
   */
  public static void printMetrics(Map<MetricName, ? extends Metric> metrics) {
    if (metrics != null && !metrics.isEmpty()) {
      int maxLengthOfDisplayName = 0;
      TreeMap<String, Object> sortedMetrics = new TreeMap<>();
      for (Metric metric : metrics.values()) {
        MetricName mName = metric.metricName();
        String mergedName = mName.group() + ":" + mName.name() + ":" + mName.tags();
        maxLengthOfDisplayName = Math.max(maxLengthOfDisplayName, mergedName.length());
        sortedMetrics.put(mergedName, metric.metricValue());
      }
      String doubleOutputFormat = "%-" + maxLengthOfDisplayName + "s : %.3f";
      String defaultOutputFormat = "%-" + maxLengthOfDisplayName + "s : %s";
      System.out.printf("\n%-" + maxLengthOfDisplayName + "s   %s%n", "Metric Name", "Value");

      for (Map.Entry<String, Object> entry : sortedMetrics.entrySet()) {
        String outputFormat;
        if (entry.getValue() instanceof Double) outputFormat = doubleOutputFormat; else outputFormat =
          defaultOutputFormat;
        System.out.printf((outputFormat) + "%n", entry.getKey(), entry.getValue());
      }
    }
  }

  public record Config(
    long records,
    String topicName,
    boolean transactionsEnabled,
    long transactionDurationMs,
    boolean shouldPrintMetrics,
    Optional<Path> hdrHistogram
  ) {
    static Config create(long records, String topicName) {
      return new Config(records, topicName, false, -1L, false, Optional.empty());
    }

    static Config create(long records, String topicName, boolean shouldPrintMetrics) {
      return new Config(records, topicName, false, -1L, shouldPrintMetrics, Optional.empty());
    }

    static Config create(long records, String topicName, long transactionDuration, boolean shouldPrintMetrics) {
      return new Config(records, topicName, true, transactionDuration, shouldPrintMetrics, Optional.empty());
    }

    static Config create(long records, String topicName, long transactionDuration) {
      return new Config(records, topicName, true, transactionDuration, false, Optional.empty());
    }
  }
}
