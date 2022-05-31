package kafka.cli.quotas;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import kafka.cli.quotas.Cli.CreateCommand;
import kafka.cli.quotas.Cli.DeleteCommand;
import kafka.cli.quotas.Cli.QueryCommand;
import kafka.cli.quotas.Quotas.ClientEntity;
import kafka.cli.quotas.Quotas.ConnectionCreationRate;
import kafka.cli.quotas.Quotas.Constraint;
import kafka.cli.quotas.Quotas.KafkaClientEntity;
import kafka.cli.quotas.Quotas.NetworkBandwidth;
import kafka.cli.quotas.Quotas.Quota;
import kafka.cli.quotas.Quotas.RequestRate;
import kafka.context.KafkaContexts;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "kfk-quotas",
    versionProvider = Cli.VersionProviderWithConfigProvider.class,
    mixinStandardHelpOptions = true,
    description = "CLI to manage Manage Kafka Quotas",
    subcommands = {QueryCommand.class, CreateCommand.class, DeleteCommand.class})
public class Cli implements Callable<Integer> {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cli()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    CommandLine.usage(this, out);
    return 0;
  }

  @Command(
      name = "query",
      description =
          """
            Search for existing quotas or quotas applying to a certain client application
            """)
  static class QueryCommand implements Callable<Integer> {

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @Option(
        names = {"--all-users"},
        description = "Get all quotas related to users")
    boolean allUsers;

    @Option(
        names = {"--all-clients"},
        description = "Get all quotas related to clients")
    boolean allClients;

    @Option(
        names = {"--all-ips"},
        description = "Get all quotas related to IPs")
    boolean allIps;

    @Option(names = {"--user-clients"})
    Map<String, String> userClients = new HashMap<>();

    @Option(names = {"--user"})
    List<String> users = new ArrayList<>();

    @Option(names = {"--user-default"})
    boolean userDefault;

    @Option(names = {"--client"})
    List<String> clientIds = new ArrayList<>();

    @Option(names = {"--client-default"})
    boolean clientIdDefault;

    @Option(names = {"--ip"})
    List<String> ips = new ArrayList<>();

    @Option(names = {"--ip-default"})
    boolean ipDefault;

    @Option(
        names = {"--only", "-o"},
        description =
            """
            Look only for quotas matching User, Client IDs, or IPs.
            If set to false (default), returns quotas even if not explicitly matching filters, e.g. defaults.
            If set to true, will return only the quotas matching the filters.
            """)
    boolean onlyMatch;

    @Override
    public Integer call() throws Exception {
      final var props = propertiesOption.load();
      try (final var kafkaAdmin = AdminClient.create(props)) {
        final var quotaManager = new QuotaManager(kafkaAdmin);

        if (allUsers) {
          final var quotas = quotaManager.allByUsers();
          System.out.println(quotas.toJson());
        } else if (allClients) {
          final var quotas = quotaManager.allByClients();
          System.out.println(quotas.toJson());
        } else if (allIps) {
          final var quotas = quotaManager.allByIps();
          System.out.println(quotas.toJson());
        } else // end of all by *
        // start querying by params
        if (!userClients.isEmpty()) {
          final var userClientMap =
              userClients.keySet().stream()
                  .collect(Collectors.toMap(k -> k, k -> List.of(userClients.get(k).split(","))));
          final var quotas = quotaManager.byUsers(userClientMap, clientIdDefault, onlyMatch);
          System.out.println(quotas.toJson());
        } else if (userDefault || !users.isEmpty()) {
          final var quotas = quotaManager.byUsers(users, userDefault, onlyMatch);
          System.out.println(quotas.toJson());
        } else if (clientIdDefault || !clientIds.isEmpty()) {
          final var quotas = quotaManager.byClients(clientIds, clientIdDefault, onlyMatch);
          System.out.println(quotas.toJson());
        } else if (ipDefault || !ips.isEmpty()) {
          final var quotas = quotaManager.byIps(ips, ipDefault, onlyMatch);
          System.out.println(quotas.toJson());
        } else { // all (default)
          final var quotas = quotaManager.all();
          System.out.println(quotas.toJson());
        }
        return 0;
      }
    }
  }

  @Command(name = "create", description = "Register new Quotas")
  static class CreateCommand implements Callable<Integer> {

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @Option(
        names = {"--user-default"},
        description = "Default to all users")
    boolean userDefault;

    @Option(
        names = {"--user"},
        description = "Application's User Principal")
    Optional<String> user;

    @Option(
        names = {"--client-default"},
        description = "Default to all client IDs")
    boolean clientIdDefault;

    @Option(
        names = {"--client"},
        description = "Application's Client ID")
    Optional<String> clientId;

    @Option(
        names = {"--ip-default"},
        description = "Default to all IPs")
    boolean ipDefault;

    @Option(
        names = {"--ip"},
        description = "Application's IP")
    Optional<String> ip;

    @Option(
        names = {"--produce-rate"},
        description = "Write bandwidth")
    Optional<Double> writeBandwidth;

    @Option(
        names = {"--fetch-rate"},
        description = "Read bandwidth")
    Optional<Double> readBandwidth;

    @Option(
        names = {"--request-rate"},
        description = "Request rate")
    Optional<Double> requestRate;

    @Option(
        names = {"--connection-rate"},
        description = "Connection creation rate")
    Optional<Double> connectionRate;

    @Override
    public Integer call() throws Exception {
      final var props = propertiesOption.load();
      try (final var kafkaAdmin = AdminClient.create(props)) {
        final var quotaManager = new QuotaManager(kafkaAdmin);
        final var quota =
            new Quota(
                new ClientEntity(
                    new KafkaClientEntity(userDefault, user),
                    new KafkaClientEntity(clientIdDefault, clientId),
                    new KafkaClientEntity(ipDefault, ip)),
                new Constraint(
                    writeBandwidth.map(NetworkBandwidth::new),
                    readBandwidth.map(NetworkBandwidth::new),
                    requestRate.map(RequestRate::new),
                    connectionRate.map(ConnectionCreationRate::new)));
        quotaManager.create(quota);
        return 0;
      }
    }
  }

  @Command(name = "delete", description = "Remove an existing Quota")
  static class DeleteCommand implements Callable<Integer> {

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    @Option(
        names = {"--user-default"},
        description = "Default to all users")
    boolean userDefault;

    @Option(
        names = {"--user"},
        description = "Application's User Principal")
    Optional<String> user;

    @Option(
        names = {"--client-default"},
        description = "Default to all client IDs")
    boolean clientIdDefault;

    @Option(
        names = {"--client"},
        description = "Application's Client ID")
    Optional<String> clientId;

    @Option(
        names = {"--ip-default"},
        description = "Default to all IPs")
    boolean ipDefault;

    @Option(
        names = {"--ip"},
        description = "Application's IP")
    Optional<String> ip;

    @Option(
        names = {"--all"},
        description = "Use to remove all existing quotas for an application")
    boolean all;

    @Option(
        names = {"--produce-rate"},
        description = "Write bandwidth")
    boolean writeBandwidth;

    @Option(
        names = {"--fetch-rate"},
        description = "Read bandwidth")
    boolean readBandwidth;

    @Option(
        names = {"--request-rate"},
        description = "Request rate")
    boolean requestRate;

    @Option(
        names = {"--connection-rate"},
        description = "Connection creation rate")
    boolean connectionRate;

    @Override
    public Integer call() throws Exception {
      final var props = propertiesOption.load();
      try (final var kafkaAdmin = AdminClient.create(props)) {
        final var quotaManager = new QuotaManager(kafkaAdmin);
        if (all) {
          if (userDefault || user.isPresent()) {
            final var quotas =
                quotaManager.byUsers(user.map(List::of).orElse(List.of()), userDefault, true);
            System.out.println(quotas.toJson());
            quotaManager.delete(quotas);
          } else if (clientIdDefault || clientId.isPresent()) {
            final var quotas =
                quotaManager.byClients(
                    clientId.map(List::of).orElse(List.of()), clientIdDefault, true);
            System.out.println(quotas.toJson());
            quotaManager.delete(quotas);
          } else if (ipDefault || ip.isPresent()) {
            final var quotas =
                quotaManager.byIps(ip.map(List::of).orElse(List.of()), ipDefault, true);
            System.out.println(quotas.toJson());
            quotaManager.delete(quotas);
          }
        } else {
          final var quota =
              new Quota(
                  new ClientEntity(
                      new KafkaClientEntity(userDefault, user),
                      new KafkaClientEntity(clientIdDefault, clientId),
                      new KafkaClientEntity(ipDefault, ip)),
                  new Constraint(
                      writeBandwidth ? Optional.of(NetworkBandwidth.empty()) : Optional.empty(),
                      readBandwidth ? Optional.of(NetworkBandwidth.empty()) : Optional.empty(),
                      requestRate ? Optional.of(RequestRate.empty()) : Optional.empty(),
                      connectionRate
                          ? Optional.of(ConnectionCreationRate.empty())
                          : Optional.empty()));
          quotaManager.delete(quota);
        }
        return 0;
      }
    }
  }

  static class PropertiesOption {

    @CommandLine.Option(
        names = {"-c", "--config"},
        description =
            "Client configuration properties file."
                + "Must include connection to Kafka and Schema Registry")
    Optional<Path> configPath;

    @ArgGroup(exclusive = false)
    ContextOption contextOption;

    public Properties load() {
      return configPath
          .map(
              path -> {
                try {
                  final var p = new Properties();
                  p.load(Files.newInputStream(path));
                  return p;
                } catch (Exception e) {
                  throw new IllegalArgumentException(
                      "ERROR: properties file at %s is failing to load".formatted(path));
                }
              })
          .orElseGet(
              () -> {
                try {
                  return contextOption.load();
                } catch (IOException e) {
                  throw new IllegalArgumentException("ERROR: loading contexts");
                }
              });
    }
  }

  static class ContextOption {

    @Option(names = "--kafka", description = "Kafka context name", required = true)
    String kafkaContextName;

    public Properties load() throws IOException {
      final var kafkas = KafkaContexts.load();
      final var props = new Properties();
      if (kafkas.has(kafkaContextName)) {
        final var kafka = kafkas.get(kafkaContextName);
        final var kafkaProps = kafka.properties();
        props.putAll(kafkaProps);

        return props;
      } else {
        err.printf(
            "ERROR: Kafka context `%s` not found. Check that context already exist.%n",
            kafkaContextName);
        return null;
      }
    }
  }

  static class VersionProviderWithConfigProvider implements CommandLine.IVersionProvider {

    @Override
    public String[] getVersion() throws IOException {
      final var url =
          VersionProviderWithConfigProvider.class.getClassLoader().getResource("cli.properties");
      if (url == null) {
        return new String[] {"No cli.properties file found in the classpath."};
      }
      final var properties = new Properties();
      properties.load(url.openStream());
      return new String[] {
        properties.getProperty("appName") + " version " + properties.getProperty("appVersion") + "",
        "Built: " + properties.getProperty("appBuildTime"),
      };
    }
  }
}
