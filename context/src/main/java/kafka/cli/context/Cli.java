package kafka.cli.context;

import static java.lang.System.err;
import static java.lang.System.out;
import static kafka.cli.context.Cli.CreateCommand;
import static kafka.cli.context.Cli.DeleteCommand;
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
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.context.KafkaAuth;
import kafka.context.KafkaCertificateAuth;
import kafka.context.KafkaCluster;
import kafka.context.KafkaContext;
import kafka.context.KafkaContexts;
import kafka.context.KafkaKeystoreAuth;
import kafka.context.KafkaNoAuth;
import kafka.context.KafkaTlsNoAuth;
import kafka.context.KafkaUsernamePasswordAuth;
import kafka.context.Keystore;
import kafka.context.Truststore;
import kafka.context.sr.HttpUsernamePasswordAuth;
import kafka.context.sr.SchemaRegistryContexts;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
  name = KFK_CTX_CMD,
  versionProvider = Cli.VersionProviderWithConfigProvider.class,
  mixinStandardHelpOptions = true,
  subcommands = {
    CreateCommand.class,
    Cli.RenameCommand.class,
    DeleteCommand.class,
    TestCommand.class,
    PropertiesCommand.class,
    SchemaRegistryCli.class,
  },
  description = "Manage Kafka connection as contexts."
)
public class Cli implements Callable<Integer> {

  public static final String KFK_CTX_CMD = "kfk-ctx";

  @Option(names = { "-v", "--verbose" })
  boolean verbose;

  public static void main(String[] args) {
    final int exitCode = new CommandLine(new Cli()).setCaseInsensitiveEnumValuesAllowed(true).execute(args);
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

  @Command(name = "create", description = "Register a Kafka context. Destination: ~/.config/kfk-ctx/kafka.json")
  static class CreateCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Kafka context name. e.g. `local`")
    String name;

    @Parameters(index = "1", description = "Bootstrap servers. e.g. `localhost:9092`")
    String bootstrapServers;

    @Option(
      names = "--auth",
      description = "Authentication method (default: ${DEFAULT-VALUE}). Valid values: ${COMPLETION-CANDIDATES}",
      required = true,
      defaultValue = "PLAINTEXT"
    )
    KafkaAuth.AuthType authType;

    @Option(
      names = { "--ssl" },
      description = "If the listener is encrypted with SSL/TLS or not",
      defaultValue = "true"
    )
    boolean isEncrypted;

    @ArgGroup
    TrustStoreOptions trustStoreOpts;

    @ArgGroup(exclusive = false)
    KeyStoreOptions keyStoreOpts;

    @ArgGroup(exclusive = false)
    CertificateOptions certOpts;

    @ArgGroup(exclusive = false)
    UsernamePasswordOptions usernamePasswordOptions;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.load();
      if (contexts.has(name)) err.println("WARN: Context name already exists. Will be overwritten");
      try {
        final KafkaAuth auth =
          switch (authType) {
            case SASL_PLAIN -> trustStoreOpts == null
              ? KafkaUsernamePasswordAuth.build(
                usernamePasswordOptions.username,
                usernamePasswordOptions.password(),
                isEncrypted
              )
              : KafkaUsernamePasswordAuth.build(
                usernamePasswordOptions.username,
                usernamePasswordOptions.password(),
                trustStoreOpts.trustStore(),
                isEncrypted
              );
            case MTLS_KEYSTORE -> KafkaKeystoreAuth.build(
              keyStoreOpts.keystore(),
              keyStoreOpts.keyPassword,
              trustStoreOpts.trustStore()
            );
            case MTLS_CERTIFICATE -> trustStoreOpts == null
              ? certOpts.password == null
                ? KafkaCertificateAuth.build(certOpts.privateKey, certOpts.publicCertificate)
                : KafkaCertificateAuth.build(certOpts.privateKey, certOpts.password, certOpts.publicCertificate)
              : certOpts.password == null
                ? KafkaCertificateAuth.build(
                  certOpts.privateKey,
                  certOpts.publicCertificate,
                  trustStoreOpts.trustStore()
                )
                : KafkaCertificateAuth.build(
                  certOpts.privateKey,
                  certOpts.password,
                  certOpts.publicCertificate,
                  trustStoreOpts.trustStore()
                );
            case TLS -> KafkaTlsNoAuth.build(trustStoreOpts.trustStore());
            default -> KafkaNoAuth.build();
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

  @Command(name = "rename", description = "Rename context. Destination: ~/.config/kfk-ctx/kafka.json")
  static class RenameCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Existing Kafka context name. e.g. `local`")
    String oldName;

    @Parameters(index = "1", description = "Name Kafka context name. e.g. `local`")
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

  @Command(name = "delete", description = "Removes context. Destination: ~/.config/kfk-ctx/kafka.json")
  static class DeleteCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Kafka context name. e.g. `local`")
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

  @Command(name = "properties", description = "Get properties configurations for contexts")
  static class PropertiesCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Kafka context name")
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

  @Command(name = "test", description = "Test cluster contexts")
  static class TestCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Kafka context name")
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

  static class CertificateOptions {

    @Option(names = { "--private-key-location" }, description = "Private Key (PEM) location")
    Path privateKey;

    @Option(
      names = { "--private-key-password" },
      description = "Private Key password",
      arity = "0..1",
      interactive = true
    )
    String password;

    @Option(names = { "--public-certificate-location" }, description = "Public certificate (PEM) location")
    Path publicCertificate;
  }

  static class KeyStoreOptions {

    @Option(names = { "--keystore-type" }, description = "Keystore type. Valid values: ${COMPLETION-CANDIDATES}")
    Optional<KeystoreType> type;

    @Option(names = { "--keystore-location" }, description = "Keystore location")
    Path location;

    @Option(names = { "--keystore-password" }, description = "Keystore password", arity = "0..1", interactive = true)
    String password;

    @Option(names = { "--key-password" }, description = "Keystore password", arity = "0..1", interactive = true)
    String keyPassword;

    public Keystore keystore() {
      return type.map(t -> Keystore.build(t.name(), location, password)).orElse(Keystore.build(location, password));
    }
  }

  static class TrustStoreOptions {

    @ArgGroup
    TrustStoreCertificateOptions certificateOptions;

    @ArgGroup(exclusive = false)
    TrustStoreKeystoreOptions keystoreOptions;

    public Truststore trustStore() {
      if (certificateOptions != null) return Truststore.build(certificateOptions.location);
      if (keystoreOptions != null) {
        var keystore = keystoreOptions.type
          .map(keystoreType -> Keystore.build(keystoreType.name(), keystoreOptions.location, keystoreOptions.password))
          .orElseGet(() -> Keystore.build(keystoreOptions.location, keystoreOptions.password));
        return Truststore.build(keystore);
      } else throw new IllegalStateException("No truststore defined.");
    }
  }

  static class TrustStoreCertificateOptions {

    @Option(names = { "--truststore-certificates" }, description = "Truststore certificates location (PEM file)")
    Path location;
  }

  static class TrustStoreKeystoreOptions {

    @Option(names = { "--truststore-type" }, description = "Truststore type. Valid values: ${COMPLETION-CANDIDATES}")
    Optional<KeystoreType> type;

    @Option(names = { "--truststore-location" }, description = "Truststore location")
    Path location;

    @Option(
      names = { "--truststore-password" },
      description = "Truststore password",
      arity = "0..1",
      interactive = true
    )
    String password;
  }

  enum KeystoreType {
    PKCS12,
    JKS,
    PEM,
  }

  static class UsernamePasswordOptions {

    @Option(names = { "--username", "-u" }, description = "Username authentication")
    String username;

    @Option(names = { "--password", "-p" }, description = "Password authentication", arity = "0..1", interactive = true)
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
