package kafka.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import picocli.CommandLine.Option;

public class ConnectionOpts {
    @Option(names = {"--bootstrap-server"}, required = true, description = "server to connect to")
    String bootstrapServer;

    @Option(names = {"--command-config"})
    Optional<Path> commandConfig;

    public Properties properties() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        commandConfig.ifPresent(path -> {
            try {
                props.load(Files.newInputStream(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return props;
    }
}
