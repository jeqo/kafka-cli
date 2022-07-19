package kafka.zip;

import java.nio.file.Path;

public class SqliteArchiveLoader implements ArchiveLoader {

    final Path archivePath;

    public SqliteArchiveLoader(Path archivePath) {
        this.archivePath = archivePath;
    }

    @Override
    public EmulatorArchive open() {

        return null;
    }

    @Override
    public void save() {

    }
}
