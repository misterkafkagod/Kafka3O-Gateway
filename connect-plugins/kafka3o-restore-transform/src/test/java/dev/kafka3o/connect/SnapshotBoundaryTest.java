package dev.kafka3o.connect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class SnapshotBoundaryTest {
    @Test
    void includesOnlyObjectsUpToEachPartitionsBoundary() {
        SnapshotBoundary boundary = new SnapshotBoundary("0:20, 1:35");
        assertTrue(boundary.includes(new BackupObjectKey(0, 0)));
        assertTrue(boundary.includes(new BackupObjectKey(0, 20)));
        assertFalse(boundary.includes(new BackupObjectKey(0, 21)));
        assertTrue(boundary.includes(new BackupObjectKey(1, 35)));
        assertFalse(boundary.includes(new BackupObjectKey(2, 0)));
    }

    @Test
    void rejectsInvalidSpecifications() {
        for (String specification : new String[] {null, "", "0", "0:x", "a:1", "0:1,0:2"}) {
            assertThrows(ConfigException.class, () -> new SnapshotBoundary(specification), String.valueOf(specification));
        }
    }
}
