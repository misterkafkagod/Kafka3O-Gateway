package dev.kafka3o.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class BackupObjectKeyTest {
    @Test
    void parsesPartitionAndStartOffset() {
        assertEquals(Optional.of(new BackupObjectKey(0, 20)),
            BackupObjectKey.parse("kafka-a/kafka3o-m10-verify/kafka3o-m10-verify-0-00000000000000000020.gz"));
        assertEquals(Optional.of(new BackupObjectKey(12, 0)), BackupObjectKey.parse("orders-v2-12-00000000000000000000"));
    }

    @Test
    void rejectsOtherNames() {
        for (String name : new String[] {null, "", "notes.txt", "orders-x-1.gz", "orders-1-99999999999999999999.gz", "-1-2.gz"}) {
            assertTrue(BackupObjectKey.parse(name).isEmpty(), String.valueOf(name));
        }
    }
}
