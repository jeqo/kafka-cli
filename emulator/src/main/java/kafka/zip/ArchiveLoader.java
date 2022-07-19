package kafka.zip;

public interface ArchiveLoader {
    EmulatorArchive open();
    void save();
}
