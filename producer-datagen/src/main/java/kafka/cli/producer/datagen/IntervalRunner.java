package kafka.cli.producer.datagen;

import java.io.IOException;
import org.apache.kafka.clients.producer.KafkaProducer;

public class IntervalRunner {

  final Config config;
  final KafkaProducer<String, byte[]> producer;
  final PayloadGenerator payloadGenerator;
  final Stats stats;

  public IntervalRunner(
    Config config,
    KafkaProducer<String, byte[]> producer,
    PayloadGenerator payloadGenerator,
    Stats stats
  ) {
    this.config = config;
    this.producer = producer;
    this.payloadGenerator = payloadGenerator;
    this.stats = stats;
  }

  byte[] sample;

  public void start() throws IOException {
    sample = payloadGenerator.sample();
    loop(0);
  }

  void loop(int count) {
    if (config.maxInterval() > 0) {
      try {
        Thread.sleep((long) (config.maxInterval() * Math.random()));
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
    }

    if (config.maxRecords() > 0 && count >= config.maxRecords()) {
      stats.printTotal();

      return;
    }

    runOnce();

    count++;
    loop(count);
  }

  void runOnce() {
    var sendStartMs = System.currentTimeMillis();
    var cb = stats.nextCompletion(sendStartMs, sample.length, stats);
    var record = payloadGenerator.record(config.topicName());

    producer.send(record, cb);

    long seed = payloadGenerator.random.nextLong();
    payloadGenerator.random.setSeed(seed);
  }

  public record Config(String topicName, long maxRecords, long maxInterval) {}
}
