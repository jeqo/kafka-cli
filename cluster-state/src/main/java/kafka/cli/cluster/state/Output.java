package kafka.cli.cluster.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSynonym;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

public record Output(
  KafkaCluster kafkaCluster,
  Map<String, Topic> topics,
  SchemaRegistry schemaRegistry
) {
  static ObjectMapper json = new ObjectMapper()
    .registerModule(new Jdk8Module())
    .registerModule(new JavaTimeModule());

  public static Builder newBuilder(List<String> topicNames) {
    return new Builder(topicNames);
  }

  public String toJson(boolean pretty) throws JsonProcessingException {
    var node = json.createObjectNode();

    var topicsNode = json.createObjectNode();
    topics.forEach((s, topic) -> topicsNode.set(s, topic.jsonNode()));

    node.set("cluster", kafkaCluster.jsonNode());
    node.set("topics", topicsNode);

    if (schemaRegistry != null) {
      node.set("schemaRegistry", schemaRegistry.jsonNode());
    }

    if (pretty) {
      return json.writerWithDefaultPrettyPrinter().writeValueAsString(node);
    } else {
      return json.writeValueAsString(node);
    }
  }

  static class Builder {

    String clusterId;
    Collection<org.apache.kafka.common.Node> brokers;

    final List<String> names;

    Map<String, TopicDescription> descriptions;
    Map<String, Config> topicConfigs;
    Map<TopicPartition, ListOffsetsResultInfo> startOffsets;
    Map<TopicPartition, ListOffsetsResultInfo> endOffsets;

    Map<String, Subject> srSubjects;

    Builder(List<String> names) {
      this.names = names;
    }

    List<String> names() {
      return Collections.unmodifiableList(names);
    }

    List<ConfigResource> configResources() {
      return names
        .stream()
        .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
        .toList();
    }

    Output build() {
      final var topics = new HashMap<String, Topic>();
      for (final var name : names) {
        final var description = descriptions.get(name);
        var partitions = description
          .partitions()
          .stream()
          .map(tpi -> {
            final var tp = new TopicPartition(name, tpi.partition());
            return Partition.from(tpi, startOffsets.get(tp), endOffsets.get(tp));
          })
          .toList();
        final var config = topicConfigs.get(name);
        final var topic = new Topic(
          name,
          description.topicId().toString(),
          partitions.size(),
          partitions.get(0).replicas().size(),
          description.isInternal(),
          partitions,
          config
        );
        topics.put(name, topic);
      }
      return new Output(
        new KafkaCluster(clusterId, brokers.stream().map(Node::from).toList()),
        topics,
        srSubjects == null ? null : new SchemaRegistry(srSubjects)
      );
    }

    public Builder withClusterId(String id) {
      this.clusterId = id;
      return this;
    }

    public Builder withBrokers(Collection<org.apache.kafka.common.Node> nodes) {
      this.brokers = nodes;
      return this;
    }

    public Builder withConfigs(
      Map<ConfigResource, org.apache.kafka.clients.admin.Config> configs
    ) {
      final var map = new HashMap<String, Config>(configs.size());
      for (var configResource : configResources()) {
        var config = configs.get(configResource);
        map.put(configResource.name(), Config.from(config));
      }
      this.topicConfigs = map;
      return this;
    }

    public Builder withStartOffsets(
      Map<TopicPartition, ListOffsetsResultInfo> startOffsets
    ) {
      this.startOffsets = startOffsets;
      return this;
    }

    public Builder withEndOffsets(Map<TopicPartition, ListOffsetsResultInfo> endOffsets) {
      this.endOffsets = endOffsets;
      return this;
    }

    public Builder withTopicDescriptions(Map<String, TopicDescription> descriptions) {
      this.descriptions = descriptions;
      return this;
    }

    public Builder withSchemaRegistrySubjects(Map<String, SchemaMetadata> srm) {
      this.srSubjects = new HashMap<>(srm.size());
      srm.forEach((subject, schemaMetadata) -> {
        Subject s = new Subject(
          schemaMetadata.getId(),
          schemaMetadata.getSchemaType(),
          schemaMetadata.getVersion()
        );
        srSubjects.put(subject, s);
      });
      return this;
    }
  }

  public record KafkaCluster(String id, List<Node> nodes) {
    public JsonNode jsonNode() {
      final var node = json.createObjectNode();
      node.put("id", id);
      final var brokers = node.putArray("brokers");
      nodes.forEach(node1 -> brokers.add(node1.jsonNode()));
      return node;
    }
  }

  public record SchemaRegistry(Map<String, Subject> subjects) {
    public JsonNode jsonNode() {
      var jsonNode = json.createObjectNode();

      var subjectsNode = json.createObjectNode();
      subjects.forEach((s, subject) -> subjectsNode.set(s, subject.jsonNode()));

      jsonNode.set("subjects", subjectsNode);
      return jsonNode;
    }
  }

  public record Subject(int id, String type, int currentVersion) {
    public JsonNode jsonNode() {
      var node = json.createObjectNode();
      node.put("id", id).put("schemaType", type).put("currentVersion", currentVersion);
      return node;
    }
  }

  public record Topic(
    String name,
    String id,
    int partitionCount,
    int replicationFactor,
    boolean isInternal,
    List<Partition> partitions,
    Config config
  ) {
    public JsonNode jsonNode() {
      var node = json.createObjectNode();
      node
        .put("name", name)
        .put("id", id)
        .put("isInternal", isInternal)
        .put("partitionCount", partitionCount)
        .put("replicationFactor", replicationFactor);
      var ps = node.putArray("partitions");
      partitions.forEach(p -> ps.add(p.jsonNode()));
      node.set("config", config.jsonNode());
      return node;
    }
  }

  public record Partition(
    int id,
    Integer leader,
    List<Integer> replicas,
    List<Integer> isr,
    Offset startOffset,
    Offset endOffset
  ) {
    public static Partition from(
      TopicPartitionInfo topicPartitionInfo,
      ListOffsetsResultInfo startOffset,
      ListOffsetsResultInfo endOffset
    ) {
      return new Partition(
        topicPartitionInfo.partition(),
        topicPartitionInfo.leader().id(),
        topicPartitionInfo.replicas().stream().map(Node::from).map(Node::id).toList(),
        topicPartitionInfo.isr().stream().map(Node::from).map(Node::id).toList(),
        Offset.from(startOffset),
        Offset.from(endOffset)
      );
    }

    public JsonNode jsonNode() {
      var node = json.createObjectNode();
      node.put("id", id);
      node.put("leader", leader);
      var rs = node.putArray("replicas");
      replicas.forEach(rs::add);
      var is = node.putArray("isr");
      isr.forEach(is::add);
      node.set("startOffset", startOffset.jsonNode());
      node.set("endOffset", endOffset.jsonNode());
      return node;
    }

    public record Offset(long offset, long timestamp, Optional<Integer> leaderEpoch) {
      static Offset from(ListOffsetsResultInfo resultInfo) {
        return new Offset(
          resultInfo.offset(),
          resultInfo.timestamp(),
          resultInfo.leaderEpoch()
        );
      }

      public JsonNode jsonNode() {
        var node = json.createObjectNode();
        node.put("offset", offset).put("timestamp", timestamp);
        leaderEpoch.ifPresent(l -> node.put("leaderEpoch", l));
        return node;
      }
    }
  }

  public record Node(int id, String host, int port, Optional<String> rack) {
    public static Node from(org.apache.kafka.common.Node node) {
      return new Node(
        node.id(),
        node.host(),
        node.port(),
        node.hasRack() ? Optional.of(node.rack()) : Optional.empty()
      );
    }

    public JsonNode jsonNode() {
      var node = json.createObjectNode();
      node.put("id", id).put("host", host).put("port", port);
      rack.ifPresent(r -> node.put("rack", r));
      return node;
    }
  }

  public record Config(Map<String, Entry> entries) {
    public static Config from(org.apache.kafka.clients.admin.Config config) {
      return new Config(
        config
          .entries()
          .stream()
          .collect(Collectors.toMap(ConfigEntry::name, Entry::from))
      );
    }

    public JsonNode jsonNode() {
      var node = json.createObjectNode();
      entries.forEach((s, entry) -> node.set(s, entry.jsonNode()));
      return node;
    }

    public record Entry(
      String name,
      String value,
      boolean isReadOnly,
      boolean isSensitive,
      boolean isDefault,
      String documentation,
      Map<String, String> synonyms
    ) {
      public static Entry from(ConfigEntry e) {
        return new Entry(
          e.name(),
          e.value(),
          e.isReadOnly(),
          e.isSensitive(),
          e.isDefault(),
          e.documentation(),
          e
            .synonyms()
            .stream()
            .collect(Collectors.toMap(ConfigSynonym::name, ConfigSynonym::value))
        );
      }

      public JsonNode jsonNode() {
        var node = json.createObjectNode();
        node
          .put("name", name)
          .put("value", isSensitive ? "*****" : value)
          .put("isReadOnly", isReadOnly)
          .put("isSensitive", isSensitive)
          .put("isDefault", isDefault)
          .put("documentation", documentation);
        var ss = node.putArray("synonyms");
        synonyms.forEach((k, v) -> ss.add(json.createObjectNode().put(k, v)));
        return node;
      }
    }
  }
}
