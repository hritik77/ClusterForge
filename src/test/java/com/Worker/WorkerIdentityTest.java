package com.Worker;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class WorkerIdentityTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void preservesUuidAndAdvancesIncarnationAcrossProcessStarts() throws Exception {
        Path identityFile = temporaryFolder.getRoot().toPath().resolve("worker.uuid");

        WorkerIdentity.Identity first = WorkerIdentity.load(identityFile);
        WorkerIdentity.Identity second = WorkerIdentity.load(identityFile);

        assertEquals(first.workerUuid(), second.workerUuid());
        assertNotEquals(first.incarnationId(), second.incarnationId());
        assertEquals(Long.parseLong(first.incarnationId()) + 1,
                Long.parseLong(second.incarnationId()));
    }
}
