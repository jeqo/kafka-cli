package kafka.cli.producer.datagen.command;

import static java.lang.System.out;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import kafka.cli.producer.datagen.Cli;
import kafka.cli.producer.datagen.PayloadGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@CommandLine.Command(name = "once", description = "produce once")
public class ProduceOnceCommand implements Callable<Integer> {

  @CommandLine.Option(names = { "-t", "--topic" }, description = "target Kafka topic name", required = true)
  String topicName;

  @CommandLine.ArgGroup(multiplicity = "1", exclusive = false)
  Cli.PropertiesOption propertiesOption;

  @CommandLine.ArgGroup(exclusive = false)
  Cli.SchemaOptions schemaOpts;

  @CommandLine.Option(names = { "-p", "--prop" }, description = "Additional client properties")
  Map<String, String> additionalProperties = new HashMap<>();

  @Override
  public Integer call() throws Exception {
    var producerConfig = propertiesOption.load();
    if (producerConfig == null) return 1;
    producerConfig.putAll(additionalProperties);
    var keySerializer = new StringSerializer();
    Serializer<Object> valueSerializer = PayloadGenerator.valueSerializer(schemaOpts.format(), producerConfig);

    try (var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer)) {
      var pg = new PayloadGenerator(schemaOpts.config());

      out.println("Avro Schema used to generate records:");
      out.println(pg.schema());
      out.printf("With field [%s] as key%n", pg.keyFieldName());

      var meta = producer.send(pg.record(topicName)).get();
      out.println("Record sent. " + meta);
    }
    return 0;
  }
}
