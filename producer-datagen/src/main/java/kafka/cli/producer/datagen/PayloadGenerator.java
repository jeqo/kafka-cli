package kafka.cli.producer.datagen;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.avro.random.generator.Generator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.config.ConfigException;

/** Datagen side */
public class PayloadGenerator implements Supplier<GenericRecord> {

  final Config config;
  final Random random;
  final Generator generator;
  final String keyFieldName;

  public PayloadGenerator(Config config) {
    this.config = config;

    this.random = new Random();
    config
        .randomSeed()
        .ifPresent(
            r -> {
              random.setSeed(r);
              random.setSeed(random.nextLong());
            });

    generator =
        new Generator.Builder()
            .random(random)
            .generation(config.count())
            .schema(config.schema())
            .build();
    keyFieldName = config.keyFieldName();
  }

  @Override
  public GenericRecord get() {
    final Object generatedObject = generator.generate();
    if (!(generatedObject instanceof GenericRecord)) {
      throw new RuntimeException(
          String.format(
              "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
              generatedObject.getClass().getName()));
    }
    return (GenericRecord) generatedObject;
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

  byte[] sample() {
    if (config.format().equals(Format.AVRO)) {
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
    return String.valueOf(payload.get(config.keyFieldName()));
  }

  record Config(
      Optional<Long> randomSeed,
      Optional<Quickstart> quickstart,
      Optional<Path> schemaPath,
      Optional<String> schemaString,
      long count,
      Format format) {

    Schema schema() {
      return quickstart
          .map(Quickstart::getSchemaFilename)
          .map(Config::getSchemaFromSchemaFileName)
          .orElse(
              schemaString
                  .map(Config::getSchemaFromSchemaString)
                  .orElse(
                      schemaPath
                          .map(
                              s -> {
                                Schema schemaFromSchemaFileName = null;
                                try {
                                  schemaFromSchemaFileName =
                                      getSchemaFromSchemaFileName(
                                          Files.newInputStream(schemaPath.get()));
                                } catch (IOException e) {
                                  e.printStackTrace();
                                }
                                return schemaFromSchemaFileName;
                              })
                          .orElse(null)));
    }

    public static Schema getSchemaFromSchemaString(String schemaString) {
      Schema.Parser schemaParser = new Parser();
      Schema schema;
      try {
        schema = schemaParser.parse(schemaString);
      } catch (SchemaParseException e) {
        throw new ConfigException("Unable to parse the provided schema");
      }
      return schema;
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

    public String keyFieldName() {
      return quickstart.map(Quickstart::getSchemaKeyField).orElse(null);
    }
  }

  enum Format {
    JSON,
    AVRO
  }

  public static void main(String[] args) throws IOException {
    var pg =
        new PayloadGenerator(
            new PayloadGenerator.Config(
                Optional.empty(),
                Optional.empty(),
                Optional.of(Path.of("cli/producer-datagen/src/main/resources/inventory.avro")),
                Optional.empty(),
                1000000,
                Format.JSON));
    var bytes = pg.sample();
    System.out.println(
        new ObjectMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(new ObjectMapper().readTree(new String(bytes))));
  }
}
