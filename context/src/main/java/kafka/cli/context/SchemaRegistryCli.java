package kafka.cli.context;

import static java.lang.System.err;
import static java.lang.System.out;

import java.util.concurrent.Callable;
import kafka.context.sr.HttpNoAuth;
import kafka.context.sr.HttpUsernamePasswordAuth;
import kafka.context.sr.SchemaRegistryAuth;
import kafka.context.sr.SchemaRegistryCluster;
import kafka.context.sr.SchemaRegistryContext;
import kafka.context.sr.SchemaRegistryContexts;
import picocli.CommandLine;

@CommandLine.Command(
  name = "sr",
  subcommands = {
    SchemaRegistryCli.CreateCommand.class, SchemaRegistryCli.RenameCommand.class, SchemaRegistryCli.DeleteCommand.class,
  },
  description = "Manage Schema Registry connection properties as contexts."
)
class SchemaRegistryCli implements Callable<Integer> {

  @CommandLine.Option(names = { "-v", "--verbose" })
  boolean verbose;

  @Override
  public Integer call() throws Exception {
    var contexts = SchemaRegistryContexts.load();
    if (verbose) {
      out.println(contexts.printNamesAndAddresses());
    } else {
      out.println(contexts.names());
    }
    return 0;
  }

  @CommandLine.Command(
    name = "create",
    description = "Register context. Destination: ~/.config/kfk-ctx/schema-registry.json"
  )
  static class CreateCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Context name. e.g. `local`")
    String name;

    @CommandLine.Parameters(index = "1", description = "Schema Registry URLs. e.g. `http://localhost:8081`")
    String urls;

    @CommandLine.Option(
      names = "--auth",
      description = "Authentication type (default: ${DEFAULT-VALUE}). Valid values: ${COMPLETION-CANDIDATES}",
      required = true,
      defaultValue = "NO_AUTH"
    )
    SchemaRegistryAuth.AuthType authType;

    @CommandLine.ArgGroup(exclusive = false)
    Cli.UsernamePasswordOptions usernamePasswordOptions;

    @Override
    public Integer call() throws Exception {
      var contexts = SchemaRegistryContexts.load();
      if (contexts.has(name)) err.println("WARN: Context name already exists. Will be overwritten");
      try {
        final SchemaRegistryAuth auth =
          switch (authType) {
            case BASIC_AUTH -> HttpUsernamePasswordAuth.build(
              authType,
              usernamePasswordOptions.username,
              usernamePasswordOptions.password()
            );
            default -> new HttpNoAuth();
          };
        final var ctx = new SchemaRegistryContext(name, new SchemaRegistryCluster(urls, auth));

        contexts.add(ctx);
        contexts.save();

        out.printf("Schema Registry context `%s` with URL(s): [%s] is saved.", ctx.name(), ctx.cluster().urls());
        return 0;
      } catch (IllegalArgumentException e) {
        err.println("ERROR: " + e.getMessage());
        return 1;
      }
    }
  }

  @CommandLine.Command(
    name = "rename",
    description = "Rename context. Destination: ~/.config/kfk-ctx/schema-registry.json"
  )
  static class RenameCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Existing Schema Registry context name. e.g. `local`")
    String oldName;

    @CommandLine.Parameters(index = "1", description = "Name Schema Registry context name. e.g. `local`")
    String newName;

    @Override
    public Integer call() throws Exception {
      final var contexts = SchemaRegistryContexts.load();

      if (contexts.has(oldName)) {
        final var ctx = contexts.get(oldName);
        contexts.rename(oldName, newName);
        contexts.save();

        out.printf(
          "Schema Registry context `%s` with URL(s): [%s] is renamed to `%s`.%n",
          ctx.name(),
          ctx.cluster().urls(),
          newName
        );
        return 0;
      } else {
        out.printf("SchemaRegistry context `%s` is not registered.%n", oldName);
        return 1;
      }
    }
  }

  @CommandLine.Command(
    name = "delete",
    description = "Removes context. Destination: ~/.config/kfk-ctx/schema-registry.json"
  )
  static class DeleteCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Context name. e.g. `local`")
    String name;

    @Override
    public Integer call() throws Exception {
      final var contexts = SchemaRegistryContexts.load();

      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        contexts.remove(name);
        contexts.save();

        out.printf("Schema Registry context `%s` with URL(s): [%s] is deleted.%n", ctx.name(), ctx.cluster().urls());
        return 0;
      } else {
        out.printf("Schema Registry Context %s is not registered.%n", name);
        return 1;
      }
    }
  }
}
