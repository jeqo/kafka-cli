package kafka.cli;


import static java.lang.System.out;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "topics", subcommands = {
        TopicCommand.CreateTopicCommand.class
})
public class TopicCommand implements Callable<Integer> {

    @Override
    public Integer call() {
        CommandLine.usage(this, out);
        return 0;
    }

    public static void main(String[] args) {
        final int exitCode = new CommandLine(new TopicCommand()).setCaseInsensitiveEnumValuesAllowed(true).execute(args);
        System.exit(exitCode);
    }

    static AdminClient adminClient(ConnectionOpts connectionOpts) {
        Properties properties = connectionOpts.properties();
        return AdminClient.create(properties);
    }

    @Command(name = "create")
    static class CreateTopicCommand implements Callable<Integer> {

        @ArgGroup(multiplicity = "1", exclusive = false)
        ConnectionOpts connectionOpts;

        @Option(names = {"--topic"}, required = true)
        Set<String> topics;
        @Option(names = {"--partitions"})
        Optional<Integer> partitions;
        @Option(names = {"--replication-factor"})
        Optional<Integer> replicationFactor;
        @Option(names = {"--config"})
        Map<String, String> configs;
        @Option(names = {"--replica-assignment"})
        Map<Integer, String > replicaAssignments;
        @Option(names = {"--if-not-exist"})
        boolean ifTopicsDoNotExist;


        @Override
        public Integer call() throws Exception {

            if (replicationFactor.map(rf -> rf > Short.MAX_VALUE || rf < 1).orElse(false))
                throw new IllegalArgumentException("The replication factor must be between 1 and " + Short.MAX_VALUE + " inclusive");
            if (partitions.map(partitions -> partitions < 1).orElse(false))
                throw new IllegalArgumentException("The partitions must be greater than 0");

            try {
                Set<NewTopic> newTopics = new HashSet<>();
                for (String name: topics) {
                    NewTopic newTopic = newTopic(name);
                    if (hasConfigs())
                        newTopic.configs(configs);
                    newTopics.add(newTopic);
                }

                try (AdminClient adminClient = adminClient(connectionOpts)) {
                    CreateTopicsResult createResult = adminClient.createTopics(
                            newTopics,
                            new CreateTopicsOptions().retryOnQuotaViolation(false)
                    );
                    createResult.all().get();
                    out.println("Created topic(s) " + topics + ".");
                    return 0;
                }
            } catch (ExecutionException e) {
                    if (e.getCause() == null)
                        throw e;
                    if (!(e.getCause() instanceof TopicExistsException && ifTopicsDoNotExist))
                        throw new RuntimeException(e.getCause());
                return -1;
            }
        }

        private NewTopic newTopic(String name) {
            if (hasReplicaAssignment()) {
                return new NewTopic(name, parseReplicaAssignment(replicaAssignments));
            } else {
                return new NewTopic(
                        name,
                        partitions,
                        replicationFactor.map(Integer::shortValue));
            }
        }

        private boolean hasConfigs() {
            return configs != null && !configs.isEmpty();
        }


        boolean hasReplicaAssignment() {
            return replicaAssignments != null && !replicaAssignments.isEmpty();
        }

        private Map<Integer, List<Integer>> parseReplicaAssignment(Map<Integer, String> replicaAssignments) {
            return new HashMap<>();
        }
    }

    static class ListTopicsCommand {

        @ArgGroup(multiplicity = "1")
        TopicOpts topicOpts;
    }

    static class AlterTopicCommand {

    }

    static class DescribeTopicsCommand {

        @ArgGroup(multiplicity = "1")
        TopicOpts topicOpts;
    }

    static class DeleteTopicCommand {

    }

    static class ReplicationReportsOpts {

    }

    static class TopicOpts {
        @Option(names = {"--topic"})
        List<String> topics;

        @Option(names = {"--topic-id"})
        List<String> topicIds;
    }
}
