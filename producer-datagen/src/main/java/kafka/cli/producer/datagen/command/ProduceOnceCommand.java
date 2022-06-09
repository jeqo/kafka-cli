package kafka.cli.producer.datagen.command;

import static java.lang.System.out;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import kafka.cli.producer.datagen.Cli;
import kafka.cli.producer.datagen.PayloadGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@CommandLine.Command(name = "once", description = "produce once")
public class ProduceOnceCommand implements Callable<Integer> {

  @CommandLine.Option(
      names = {"-t", "--topic"},
      description = "target Kafka topic name",
      required = true)
  String topicName;

  @CommandLine.ArgGroup(multiplicity = "1")
  Cli.PropertiesOption propertiesOption;

  @CommandLine.ArgGroup(multiplicity = "1")
  Cli.SchemaSourceOption schemaSource;

  @CommandLine.Option(
      names = {"-f", "--format"},
      description = "Record value format",
      defaultValue = "JSON")
  PayloadGenerator.Format format;

  @CommandLine.Option(
      names = {"-p", "--prop"},
      description = "Additional client properties")
  Map<String, String> additionalProperties = new HashMap<>();

  @Override
  public Integer call() throws Exception {
    var producerConfig = propertiesOption.load();
    if (producerConfig == null) return 1;
    producerConfig.putAll(additionalProperties);
    var keySerializer = new StringSerializer();
    Serializer<Object> valueSerializer = PayloadGenerator.valueSerializer(format, producerConfig);

    try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
      var pg =
          new PayloadGenerator(
              new PayloadGenerator.Config(
                  Optional.empty(), schemaSource.quickstart, schemaSource.schemaPath, 10, format));

      var meta = producer.send(pg.record(topicName)).get();
      out.println("Record sent. " + meta);
    }
    return 0;
  }
}
