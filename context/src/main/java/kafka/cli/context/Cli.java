package kafka.cli.context;

import static java.lang.System.err;
import static java.lang.System.out;
import static kafka.cli.context.Cli.CreateCommand;
import static kafka.cli.context.Cli.DeleteCommand;
import static kafka.cli.context.Cli.EnvCommand;
import static kafka.cli.context.Cli.KCatCommand;
import static kafka.cli.context.Cli.KFK_CTX_CMD;
import static kafka.cli.context.Cli.PropertiesCommand;
import static kafka.cli.context.Cli.TestCommand;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.context.KafkaCluster;
import kafka.context.KafkaContext;
import kafka.context.KafkaContexts;
import kafka.context.auth.KafkaAuth;
import kafka.context.auth.KafkaNoAuth;
import kafka.context.auth.KafkaUsernamePasswordAuth;
import kafka.context.sr.SchemaRegistryContexts;
import kafka.context.sr.auth.HttpUsernamePasswordAuth;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@CommandLine.Command(
  name = KFK_CTX_CMD,
  versionProvider = Cli.VersionProviderWithConfigProvider.class,
  mixinStandardHelpOptions = true,
  subcommands = {
    CreateCommand.class,
    Cli.RenameCommand.class,
    DeleteCommand.class,
    TestCommand.class,
    PropertiesCommand.class,
    KCatCommand.class,
    EnvCommand.class,
    SchemaRegistryContextsCommand.class,
  },
  description = "Manage Kafka connection as contexts."
)
public class Cli implements Callable<Integer> {

  public static final String KFK_CTX_CMD = "kfk-ctx";

  @Option(names = { "-v", "--verbose" })
  boolean verbose;

  public static void main(String[] args) {
    final int exitCode = new CommandLine(new Cli()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    final var contexts = KafkaContexts.load();
    if (verbose) {
      out.println(contexts.printNamesAndAddresses());
    } else {
      out.println(contexts.names());
    }
    return 0;
  }

  @CommandLine.Command(name = "create", description = "Register a Kafka context. Destination: ~/.kafka/kafka.json")
  static class CreateCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name. e.g. `local`")
    String name;

    @CommandLine.Parameters(index = "1", description = "Bootstrap servers. e.g. `localhost:9092`")
    String bootstrapServers;

    @CommandLine.Option(
      names = "--auth",
      description = "Authentication method (default: ${DEFAULT-VALUE}). Valid values: ${COMPLETION-CANDIDATES}",
      required = true,
      defaultValue = "PLAINTEXT"
    )
    KafkaAuth.AuthType authType;

    @ArgGroup(exclusive = false)
    UsernamePasswordOptions usernamePasswordOptions;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();

      try {
        final KafkaAuth auth =
          switch (authType) {
            case SASL_PLAIN -> KafkaUsernamePasswordAuth.build(
              authType,
              usernamePasswordOptions.username,
              usernamePasswordOptions.password()
            );
            default -> new KafkaNoAuth();
          };
        final var ctx = new KafkaContext(name, new KafkaCluster(bootstrapServers, auth));

        contexts.add(ctx);
        contexts.save();

        out.printf(
          "Kafka context `%s` with bootstrap-servers [%s] is saved.",
          ctx.name(),
          ctx.cluster().bootstrapServers()
        );
        return 0;
      } catch (IllegalArgumentException e) {
        err.println("ERROR: " + e.getMessage());
        return 1;
      }
    }
  }

  @CommandLine.Command(name = "rename", description = "Rename context. Destination: ~/.kafka/kafka.json")
  static class RenameCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Existing Kafka context name. e.g. `local`")
    String oldName;

    @CommandLine.Parameters(index = "1", description = "Name Kafka context name. e.g. `local`")
    String newName;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();

      if (contexts.has(oldName)) {
        final var ctx = contexts.get(oldName);
        contexts.rename(oldName, newName);
        contexts.save();

        out.printf(
          "Kafka context `%s` with bootstrap servers: [%s] is renamed to `%s`.%n",
          oldName,
          ctx.cluster().bootstrapServers(),
          newName
        );
        return 0;
      } else {
        out.printf("Kafka context `%s` is not registered.%n", oldName);
        return 1;
      }
    }
  }

  @CommandLine.Command(name = "delete", description = "Removes context. Destination: ~/.kafka/kafka.json")
  static class DeleteCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name. e.g. `local`")
    String name;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();

      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        contexts.remove(name);
        contexts.save();

        out.printf(
          "Kafka context `%s` with bootstrap servers: [%s] is deleted.%n",
          ctx.name(),
          ctx.cluster().bootstrapServers()
        );
        return 0;
      } else {
        out.printf("Kafka context `%s` is not registered.%n", name);
        return 1;
      }
    }
  }

  @CommandLine.Command(name = "properties", description = "Get properties configurations for contexts")
  static class PropertiesCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name")
    String name;

    @Option(names = { "--schema-registry", "-sr" }, description = "Schema Registry context name")
    Optional<String> schemeRegistryContext;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();
      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        final var props = ctx.properties();
        props.store(out, "Kafka client properties generated by " + KFK_CTX_CMD);

        if (schemeRegistryContext.isPresent()) {
          final var srContexts = SchemaRegistryContexts.load();
          if (srContexts.has(schemeRegistryContext.get())) {
            final var srCtx = srContexts.get(schemeRegistryContext.get());
            final var srProps = srCtx.properties();

            srProps.store(out, "Schema Registry client properties generated by " + KFK_CTX_CMD);
          } else {
            System.err.printf(
              "WARN: Schema Registry context %s does not exist. " +
              "Schema Registry connection properties will not be included",
              schemeRegistryContext.get()
            );
          }
        }
      } else {
        err.printf("Kafka context `%s` is not found%n", name);
        return 1;
      }
      return 0;
    }
  }

  @CommandLine.Command(name = "test", description = "Test cluster contexts")
  static class TestCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name")
    String name;

    @Option(names = { "--schema-registry", "-sr" }, description = "Schema Registry context name")
    Optional<String> schemeRegistryContext;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();
      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        final var props = ctx.properties();

        final var bootstrapServers = props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        try (final var admin = AdminClient.create(props)) {
          final var clusterId = admin.describeCluster(new DescribeClusterOptions().timeoutMs(10_000)).clusterId().get();
          out.printf("Connection to Kafka `%s` [%s] (id=%s) succeed%n", name, bootstrapServers, clusterId);
          admin.describeCluster().nodes().get().forEach(node -> System.err.println("Node: " + node));
        } catch (Exception e) {
          out.printf("Connection to Kafka `%s` [%s] failed%n", name, bootstrapServers);
          e.printStackTrace();
          return 1;
        }

        if (schemeRegistryContext.isPresent()) {
          final var srContexts = SchemaRegistryContexts.load();
          final var sr = schemeRegistryContext.get();
          if (srContexts.has(sr)) {
            final var srCtx = srContexts.get(sr);

            final var auth = srCtx.cluster().auth();
            final var httpClient =
              switch (auth.type()) {
                case BASIC_AUTH -> HttpClient
                  .newBuilder()
                  .authenticator(
                    new Authenticator() {
                      @Override
                      protected PasswordAuthentication getPasswordAuthentication() {
                        final var basicAuth = (HttpUsernamePasswordAuth) auth;
                        return basicAuth.passwordAuth();
                      }
                    }
                  )
                  .build();
                case NO_AUTH -> HttpClient.newHttpClient();
              };
            final var urls = srCtx.cluster().urls();
            final var response = httpClient.send(
              HttpRequest.newBuilder().uri(URI.create(urls)).GET().build(),
              BodyHandlers.discarding()
            );
            if (response.statusCode() == 200) {
              out.printf("Connection to Schema Registry `%s` [%s] succeed%n", sr, urls);
            } else {
              out.printf("Connection to Schema Registry `%s` URL(s): [%s] failed%n", sr, urls);
              return 1;
            }
          } else {
            out.printf(
              "WARN: Schema Registry context %s does not exist. " +
              "Schema Registry connection properties will not be tested",
              sr
            );
          }
        }
      } else {
        err.printf("Kafka context `%s` is not found%n", name);
        return 1;
      }
      return 0;
    }
  }

  @CommandLine.Command(name = "env", description = "env command with properties from context")
  static class EnvCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Context name")
    String name;

    @Option(names = { "--schema-registry", "-sr" }, description = "Schema Registry context name")
    Optional<String> schemeRegistryContext;

    @Option(names = { "--auth" }, description = "Include auth env variables")
    boolean includeAuth;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();
      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        final var env = ctx.env(includeAuth);

        if (schemeRegistryContext.isPresent()) {
          final var srContexts = SchemaRegistryContexts.load();
          if (srContexts.has(schemeRegistryContext.get())) {
            final var srCtx = srContexts.get(schemeRegistryContext.get());
            final var srProps = srCtx.env(includeAuth);

            out.println(env + "\n" + srProps);
          } else {
            System.err.printf(
              "WARN: Schema Registry context %s does not exist. Schema Registry connection properties will not be included",
              schemeRegistryContext.get()
            );
          }
        } else {
          out.println(env);
        }
      } else {
        err.printf("Kafka context `%s` is not found%n", name);
        return 1;
      }
      return 0;
    }
  }

  @CommandLine.Command(name = "kcat", description = "kcat command with properties from context")
  static class KCatCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Context name")
    String name;

    @Option(names = { "--schema-registry", "-sr" }, description = "Schema Registry context name")
    Optional<String> schemeRegistryContext;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();
      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        final var kcat = ctx.kcat();

        if (schemeRegistryContext.isPresent()) {
          final var srContexts = SchemaRegistryContexts.load();
          if (srContexts.has(schemeRegistryContext.get())) {
            final var srCtx = srContexts.get(schemeRegistryContext.get());
            final var srProps = srCtx.kcat();

            out.println(kcat + srProps);
          } else {
            System.err.printf(
              "WARN: Schema Registry context %s does not exist. Schema Registry connection properties will not be included",
              schemeRegistryContext.get()
            );
          }
        } else {
          out.println(kcat);
        }
      } else {
        err.printf("Kafka context `%s` is not found%n", name);
        return 1;
      }
      return 0;
    }
  }

  static class UsernamePasswordOptions {

    @CommandLine.Option(names = { "--username", "-u" }, description = "Username authentication")
    String username;

    @CommandLine.Option(
      names = { "--password", "-p" },
      description = "Password authentication",
      arity = "0..1",
      interactive = true
    )
    String password;

    public String password() {
      if (password == null || password.isBlank()) {
        throw new IllegalArgumentException("Password is empty");
      }
      return password;
    }
  }

  static class VersionProviderWithConfigProvider implements CommandLine.IVersionProvider {

    @Override
    public String[] getVersion() throws IOException {
      final var url = VersionProviderWithConfigProvider.class.getClassLoader().getResource("cli.properties");
      if (url == null) {
        return new String[] { "No cli.properties file found in the classpath." };
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
