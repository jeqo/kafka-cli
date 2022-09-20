package kafka.cli.producer.datagen;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

/** Datagen side */
public class PayloadGenerator {

  final Format format;
  final Random random;
  final Generator generator;
  final Converter converter;
  final AvroData avroData;
  final Schema avroSchema;
  final org.apache.kafka.connect.data.Schema connectSchema;
  final String keyFieldName;

  public PayloadGenerator(Config config, Properties producerConfig) {
    this.format = config.format();
    this.random = new Random();
    config
      .randomSeed()
      .ifPresent(r -> {
        random.setSeed(r);
        random.setSeed(random.nextLong());
      });

    this.generator = new Generator.Builder().random(random).generation(config.count()).schema(config.schema()).build();
    this.keyFieldName = config.keyFieldName();
    this.avroData = new AvroData(1);
    this.avroSchema = config.schema();
    this.connectSchema = avroData.toConnectSchema(config.schema());
    this.converter = switch (this.format) {
      case JSON -> {
        var jsonConverter = new JsonConverter();
        var schemasEnabled = producerConfig.getProperty("schemas.enabled", "false");
        jsonConverter.configure(Map.of("schemas.enable", schemasEnabled, "converter.type", "value"));
        yield jsonConverter;
      }
      case AVRO -> {
        var avroConverter = new AvroConverter();
        avroConverter.configure(
                producerConfig.keySet().stream().collect(Collectors.toMap(String::valueOf, producerConfig::get)),
                false);
        yield avroConverter;
      }
      case PROTOBUF -> {
        var avroConverter = new ProtobufConverter();
        avroConverter.configure(
                producerConfig.keySet().stream().collect(Collectors.toMap(String::valueOf, producerConfig::get)),
                false);
        yield avroConverter;
      }
      case JSON_SCHEMA -> {
        var avroConverter = new JsonSchemaConverter();
        avroConverter.configure(
                producerConfig.keySet().stream().collect(Collectors.toMap(String::valueOf, producerConfig::get)),
                false);
        yield avroConverter;
      }
    };
  }

  public GenericRecord get() {
    final Object generatedObject = generator.generate();
    if (!(generatedObject instanceof GenericRecord)) {
      throw new RuntimeException(
        String.format(
          "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
          generatedObject.getClass().getName()
        )
      );
    }
    return (GenericRecord) generatedObject;
  }

  public ProducerRecord<String, byte[]> record(String topicName) {
    final var record = get();
    final var key = key(record);
    final var value = value(topicName, record);
    return new ProducerRecord<>(topicName, key, value);
  }

  String toJson(GenericRecord record) {
    try {
      final var outputStream = new ByteArrayOutputStream();
      final var schema = record.getSchema();
      final var datumWriter = new GenericDatumWriter<GenericRecord>(schema);
      final var encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);
      datumWriter.write(record, encoder);
      encoder.flush();
      return outputStream.toString();
    } catch (IOException e) {
      throw new RuntimeException("Error converting to json", e);
    }
  }

  public byte[] sample() {
    if (format.equals(Format.AVRO)) {
      return toBytes(get());
    } else {
      return toJson(get()).getBytes(StandardCharsets.UTF_8);
    }
  }

  private byte[] toBytes(GenericRecord record) {
    try {
      final var outputStream = new ByteArrayOutputStream();
      final var schema = record.getSchema();
      final var datumWriter = new GenericDatumWriter<GenericRecord>(schema);
      final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      datumWriter.write(record, encoder);
      encoder.flush();
      return outputStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Error converting to json", e);
    }
  }

  public String key(GenericRecord payload) {
    return String.valueOf(payload.get(keyFieldName));
  }

  public String schema() {
    return generator.schema().toString();
  }

  public String keyFieldName() {
    return keyFieldName;
  }

  public byte[] value(String topicName, GenericRecord payload) {
    final var schemaAndValue = avroData.toConnectData(avroSchema, payload);
    return converter.fromConnectData(topicName, schemaAndValue.schema(), schemaAndValue.value());
  }

  public record Config(
    Optional<Long> randomSeed,
    Optional<Quickstart> quickstart,
    Optional<Path> schemaPath,
    long count,
    Format format,
    String keyFieldName
  ) {
    Schema schema() {
      return quickstart
        .map(Quickstart::getSchemaFilename)
        .map(Config::getSchemaFromSchemaFileName)
        .orElse(
          schemaPath
            .map(s -> {
              Schema schemaFromSchemaFileName = null;
              try {
                schemaFromSchemaFileName = getSchemaFromSchemaFileName(Files.newInputStream(schemaPath.get()));
              } catch (IOException e) {
                e.printStackTrace();
              }
              return schemaFromSchemaFileName;
            })
            .orElse(null)
        );
    }

    public static Schema getSchemaFromSchemaFileName(InputStream stream) {
      Schema.Parser schemaParser = new Parser();
      Schema schema;
      try {
        schema = schemaParser.parse(stream);
      } catch (SchemaParseException | IOException e) {
        throw new ConfigException("Unable to parse the provided schema", e);
      } finally {
        try {
          stream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return schema;
    }

    @Override
    public String keyFieldName() {
      if (keyFieldName == null) return quickstart
        .map(Quickstart::getSchemaKeyField)
        .orElse(null); else return keyFieldName;
    }
  }

  public enum Format {
    JSON,
    AVRO,
    JSON_SCHEMA,
    PROTOBUF
  }

  @SuppressWarnings("unchecked")
  public static Serializer<Object> valueSerializer(Format format, Properties producerConfig) {
    Serializer<Object> valueSerializer;
    if (format.equals(Format.AVRO)) {
      valueSerializer = new KafkaAvroSerializer();
      valueSerializer.configure(
        producerConfig.keySet().stream().collect(Collectors.toMap(String::valueOf, producerConfig::get)),
        false
      );
    } else {
      valueSerializer = (Serializer) new StringSerializer();
    }
    return valueSerializer;
  }

  public enum Quickstart {
    CLICKSTREAM_CODES("clickstream_codes_schema.avro", "code"),
    CLICKSTREAM("clickstream_schema.avro", "ip"),
    CLICKSTREAM_USERS("clickstream_users_schema.avro", "user_id"),
    ORDERS("orders_schema.avro", "orderid"),
    RATINGS("ratings_schema.avro", "rating_id"),
    USERS("users_schema.avro", "userid"),
    USERS_("users_array_map_schema.avro", "userid"),
    PAGEVIEWS("pageviews_schema.avro", "viewtime"),
    STOCK_TRADES("stock_trades_schema.avro", "symbol"),
    INVENTORY("inventory.avro", "id"),
    PRODUCT("product.avro", "id"),
    PURCHASES("purchase.avro", "id"),
    TRANSACTIONS("transactions.avro", "transaction_id"),
    STORES("stores.avro", "store_id"),
    CREDIT_CARDS("credit_cards.avro", "card_id");

    static final Set<String> configValues = new HashSet<>();

    static {
      for (Quickstart q : Quickstart.values()) {
        configValues.add(q.name().toLowerCase());
      }
    }

    private final String schemaFilename;
    private final String keyName;

    Quickstart(String schemaFilename, String keyName) {
      this.schemaFilename = schemaFilename;
      this.keyName = keyName;
    }

    public InputStream getSchemaFilename() {
      try {
        return Quickstart.class.getClassLoader().getResourceAsStream(schemaFilename);
      } catch (SchemaParseException i) {
        // log.error("Unable to parse the provided schema", i);
        throw new ConfigException("Unable to parse the provided schema");
      }
    }

    public String getSchemaKeyField() {
      return keyName;
    }
  }
}
