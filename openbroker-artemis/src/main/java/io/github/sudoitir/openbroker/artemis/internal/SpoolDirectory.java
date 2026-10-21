package io.github.sudoitir.openbroker.artemis.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/** Owner-only spool files for one receiver or sender, removed with it. */
final class SpoolDirectory implements CoreBodies.Spool, AutoCloseable {

    private final Path dir;

    SpoolDirectory(Path spoolDirectory) {
        this.dir = spoolDirectory.resolve("openbroker-" + UUID.randomUUID());
    }

    @Override
    public Path file(long size) throws IOException {
        boolean posix = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
        if (!Files.exists(dir)) {
            if (posix) {
                Files.createDirectories(
                        dir, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            } else {
                Files.createDirectories(dir);
            }
        }
        long usable = Files.getFileStore(dir).getUsableSpace();
        if (usable < size) {
            throw new IOException("A large message of " + size + " bytes needs that much free disk under " + dir
                    + ", and only " + usable + " bytes are free.");
        }
        FileAttribute<?>[] attributes = posix
                ? new FileAttribute<?>[] {
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
                }
                : new FileAttribute<?>[0];
        return Files.createTempFile(dir, "message-", ".body", attributes);
    }

    void delete(@Nullable Path file) {
        if (file != null) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not delete the spool file " + file, e);
            }
        }
    }

    @Override
    public void close() {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    // One undeletable file must not keep the rest.
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Could not remove the spool directory " + dir, e);
        }
    }
}
