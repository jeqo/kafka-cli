package kafka.cli.producer.datagen;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PayloadGeneratorTest {

  private static final String TOPIC_NAME = "test-topic";
  private PayloadGenerator subject;
  private Deserializer<Object> deserializer;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
      new Object[][] {
        { PayloadGenerator.Format.JSON, new KafkaJsonDeserializer() },
        { PayloadGenerator.Format.JSON_SR, new KafkaJsonSchemaDeserializer() },
        { PayloadGenerator.Format.AVRO, new KafkaAvroDeserializer() },
      }
    );
  }

  public PayloadGeneratorTest(PayloadGenerator.Format format, Deserializer<Object> deserializer) {
    final PayloadGenerator.Config config = new PayloadGenerator.Config(
      Optional.of(1L),
      Optional.of(PayloadGenerator.Quickstart.ORDERS),
      Optional.empty(),
      1L,
      format,
      null
    );
    this.deserializer = deserializer;
    subject = new PayloadGenerator(config);
  }

  @Test
  public void serializationRoundTripByFormat() {
    final GenericRecord payload = subject.get();
    final Properties props = new Properties();
    props.put("schema.registry.url", "mock://dummy");
    final Serializer<Object> serializer = PayloadGenerator.valueSerializer(subject.format, props);
    final var value =
      switch (subject.format) {
        case AVRO -> payload;
        case JSON_SR -> subject.toJsonSr(payload);
        default -> subject.toJson(payload);
      };
    deserializer.configure(props.keySet().stream().collect(Collectors.toMap(String::valueOf, props::get)), false);
    final byte[] output = serializer.serialize(TOPIC_NAME, value);
    final var input = deserializer.deserialize(TOPIC_NAME, output);
    Assert.assertNotNull(input);
  }
}
