package kafka.cli.producer.datagen.command;

import static java.lang.System.out;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import kafka.cli.producer.datagen.Cli;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;

@CommandLine.Command(name = "topics", description = "List topics and subjectSchemas available in a cluster")
public class ListTopicsCommand implements Callable<Integer> {

  @CommandLine.ArgGroup(multiplicity = "1")
  Cli.PropertiesOption propertiesOption;

  @CommandLine.Option(names = { "--pretty" }, defaultValue = "false", description = "Print pretty/formatted JSON")
  boolean pretty;

  @CommandLine.Option(names = { "-p", "--prop" }, description = "Additional client properties")
  Map<String, String> additionalProperties = new HashMap<>();

  @CommandLine.Option(names = { "--prefix" }, description = "Topic name prefix")
  Optional<String> prefix;

  final ObjectMapper json = new ObjectMapper();

  @Override
  public Integer call() throws Exception {
    var props = propertiesOption.load();
    if (props == null) return 1;
    props.putAll(additionalProperties);
    try (final var kafkaAdminClient = AdminClient.create(props)) {
      final var topics = kafkaAdminClient
              .listTopics()
              .names()
              .get()
              .stream()
              .filter(t -> prefix.map(t::startsWith).orElse(true))
              .toList();

      final var schemaRegistryUrl = props.getProperty("schema.registry.url");
      final Optional<SchemaRegistryClient> schemaRegistryClient;
      if (schemaRegistryUrl != null && !schemaRegistryUrl.isBlank()) {
        schemaRegistryClient =
                Optional.of(
                        new CachedSchemaRegistryClient(
                                schemaRegistryUrl,
                                10,
                                props.keySet().stream().collect(Collectors.toMap(Object::toString, k -> props.getProperty(k.toString())))
                        )
                );
      } else {
        schemaRegistryClient = Optional.empty();
      }
      final var result = new ArrayList<TopicAndSchema>(topics.size());
      for (final var topic : topics) {
        var subject = schemaRegistryClient
                .map(c -> {
                  try {
                    final var allSubjectsByPrefix = c.getAllSubjectsByPrefix(topic);
                    final var subjects = new HashMap<String, List<ParsedSchema>>();
                    for (final var s : allSubjectsByPrefix) {
                      try {
                        final var schemas = c.getSchemas(s, false, true);
                        subjects.put(s, schemas);
                      } catch (IOException | RestClientException e) {
                        throw new RuntimeException(e);
                      }
                    }
                    return subjects;
                  } catch (IOException | RestClientException e) {
                    throw new RuntimeException(e);
                  }
                })
                .map(parsedSchemas -> parsedSchemas.entrySet().stream().map(TopicAndSchema.SubjectSchemas::from).toList())
                .orElse(List.of());
        result.add(new TopicAndSchema(topic, subject));
      }
      final var array = json.createArrayNode();
      result.forEach(t -> array.add(t.toJson()));
      if (pretty) {
        out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(array));
      } else {
        out.println(json.writeValueAsString(array));
      }
      return 0;
    }
  }

  record TopicAndSchema(String topicName, List<SubjectSchemas> subjects) {
    static final ObjectMapper objectMapper = new ObjectMapper();

    JsonNode toJson() {
      final var json = objectMapper.createObjectNode().put("topicName", this.topicName);
      final var subjects = objectMapper.createArrayNode();
      this.subjects.forEach(s -> subjects.add(s.toJson()));
      json.set("subjects", subjects);
      return json;
    }

    record SubjectSchemas(String name, List<Schema> schemas) {
      static SubjectSchemas from(Map.Entry<String, List<ParsedSchema>> entry) {
        return new SubjectSchemas(entry.getKey(), entry.getValue().stream().map(TopicAndSchema.Schema::from).toList());
      }

      JsonNode toJson() {
        var json = objectMapper.createObjectNode();
        json.put("name", name);
        var schemasArray = objectMapper.createArrayNode();
        schemas.forEach(s -> schemasArray.add(s.toJson()));
        json.set("schemas", schemasArray);
        return json;
      }
    }

    record Schema(String name, String canonicalString) {
      static Schema from(ParsedSchema parsedSchema) {
        return new Schema(parsedSchema.name(), parsedSchema.canonicalString());
      }

      JsonNode toJson() {
        var json = objectMapper.createObjectNode();
        json.put("name", name).put("canonicalString", canonicalString);
        return json;
      }
    }
  }
}
