package kafka.cli.cluster.state;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import kafka.cli.cluster.state.Cli.Opts;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;

public final class Helper {

  final AdminClient adminClient;
  final Optional<SchemaRegistryClient> srClient;

  Helper(AdminClient adminClient) {
    this.adminClient = adminClient;
    this.srClient = Optional.empty();
  }

  Helper(AdminClient adminClient, SchemaRegistryClient srClient) {
    this.adminClient = adminClient;
    this.srClient = Optional.of(srClient);
  }

  List<String> filterTopics(Opts opts)
    throws InterruptedException, ExecutionException {
    final var all = adminClient
      .listTopics(new ListTopicsOptions())
      .listings()
      .get();
    final var names = all.stream().map(TopicListing::name).toList();
    var filtered = names.stream().filter(opts::match).toList();
    return filtered.isEmpty() ? names : filtered;
  }

  Output run(Opts opts) throws ExecutionException, InterruptedException {
    final var topics = filterTopics(opts);

    final var builder = Output.newBuilder(topics);
    final var describeClusterResult = adminClient.describeCluster();

    final var descriptions = adminClient
      .describeTopics(builder.names())
      .allTopicNames()
      .get();

    final var startOffsetRequest = new HashMap<TopicPartition, OffsetSpec>();
    final var endOffsetRequest = new HashMap<TopicPartition, OffsetSpec>();

    for (final var topic : builder.names) {
      final var description = descriptions.get(topic);
      final var tps = description
        .partitions()
        .stream()
        .map(tpi -> new TopicPartition(topic, tpi.partition()))
        .sorted(Comparator.comparingInt(TopicPartition::partition))
        .toList();
      for (final var tp : tps) {
        startOffsetRequest.put(tp, OffsetSpec.earliest());
        endOffsetRequest.put(tp, OffsetSpec.latest());
      }
    }

    final var startOffsets = adminClient
      .listOffsets(startOffsetRequest)
      .all()
      .get();
    final var endOffsets = adminClient
      .listOffsets(endOffsetRequest)
      .all()
      .get();

    final var configs = adminClient
      .describeConfigs(builder.configResources())
      .all()
      .get();

    final var srSubjects = srClient.map(sr -> {
      try {
        return opts
          .prefix()
          .map(p -> {
            try {
              return sr.getAllSubjectsByPrefix(p);
            } catch (IOException | RestClientException e) {
              throw new RuntimeException(e);
            }
          })
          .orElse(sr.getAllSubjects());
      } catch (IOException | RestClientException e) {
        throw new RuntimeException(e);
      }
    });

    final var srSubjectsMetadata = srClient.map(sr ->
      srSubjects
        .map(subjects ->
          subjects
            .stream()
            .map(s -> {
              try {
                return Map.entry(s, sr.getLatestSchemaMetadata(s));
              } catch (IOException | RestClientException e) {
                throw new RuntimeException(e);
              }
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        )
        .orElse(Map.of())
    );
    builder
      .withClusterId(describeClusterResult.clusterId().get())
      .withBrokers(describeClusterResult.nodes().get())
      .withTopicDescriptions(descriptions)
      .withStartOffsets(startOffsets)
      .withEndOffsets(endOffsets)
      .withConfigs(configs);

    srSubjectsMetadata.ifPresent(builder::withSchemaRegistrySubjects);

    return builder.build();
  }
}
