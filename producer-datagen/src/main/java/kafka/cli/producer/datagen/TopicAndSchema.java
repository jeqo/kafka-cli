package kafka.cli.producer.datagen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.util.List;
import java.util.Map;

public record TopicAndSchema(String topicName, List<SubjectSchemas> subjects) {

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
      return new SubjectSchemas(
          entry.getKey(), entry.getValue().stream().map(Schema::from).toList());
    }

    public JsonNode toJson() {
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

    public JsonNode toJson() {
      var json = objectMapper.createObjectNode();
      json.put("name", name).put("canonicalString", canonicalString);
      return json;
    }
  }
}
