package com.Worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

final class WorkerIdentity {
    private WorkerIdentity() {}

    static Identity load(Path identityFile) throws IOException {
        Path file = identityFile.toAbsolutePath();
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path incarnationFile = file.resolveSibling(file.getFileName() + ".incarnation");

        try (FileChannel channel = FileChannel.open(
                file, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            String workerUuid = read(channel);
            if (workerUuid.isBlank()) {
                workerUuid = UUID.randomUUID().toString();
                write(channel, workerUuid);
            } else {
                try {
                    UUID.fromString(workerUuid);
                } catch (IllegalArgumentException e) {
                    throw new IOException("Worker identity file contains an invalid UUID", e);
                }
            }

            long previousIncarnation = 0;
            if (Files.exists(incarnationFile)) {
                String savedIncarnation = Files.readString(incarnationFile).trim();
                try {
                    previousIncarnation = Long.parseLong(savedIncarnation);
                } catch (NumberFormatException e) {
                    throw new IOException("Worker incarnation file is invalid", e);
                }
                if (previousIncarnation < 0) {
                    throw new IOException("Worker incarnation cannot be negative");
                }
            }
            long nextIncarnation;
            try {
                nextIncarnation = Math.addExact(previousIncarnation, 1);
            } catch (ArithmeticException e) {
                throw new IOException("Worker incarnation counter is exhausted", e);
            }
            Files.writeString(incarnationFile, Long.toString(nextIncarnation),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            return new Identity(workerUuid, Long.toString(nextIncarnation));
        }
    }

    private static String read(FileChannel channel) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(Math.toIntExact(channel.size()));
        channel.position(0);
        while (data.hasRemaining() && channel.read(data) >= 0) {
            // Read the complete small identity file.
        }
        return StandardCharsets.UTF_8.decode(data.flip()).toString().trim();
    }

    private static void write(FileChannel channel, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        channel.truncate(0);
        channel.position(0);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        while (data.hasRemaining()) {
            channel.write(data);
        }
        channel.force(true);
    }

    record Identity(String workerUuid, String incarnationId) {}
}
