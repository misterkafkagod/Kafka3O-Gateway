package dev.kafka3o.connect;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class UnwrapBackupRecordTest {
    private static String base64(String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    private static SourceRecord line(String objectKey, Map<String, Object> line) {
        return new SourceRecord(Map.of("bucket", "kafka3o-backups", "objectKey", objectKey), Map.of("recordCount", 1L),
            "kafka3o-m10-verify", null, null, objectKey.getBytes(StandardCharsets.UTF_8), null, line, 1L);
    }

    private static UnwrapBackupRecord<SourceRecord> transform() {
        UnwrapBackupRecord<SourceRecord> transform = new UnwrapBackupRecord<>();
        transform.configure(Map.of("restore.id", "r-1", "snapshot.boundary", "0:20,3:0"));
        return transform;
    }

    @Test
    void restoresKeyValueHeadersTimestampAndPartitionWithTheMarker() {
        Map<String, Object> backup = new HashMap<>();
        backup.put("key", base64("k-1"));
        backup.put("value", base64("backup test one"));
        backup.put("timestamp", "2026-11-10T07:33:15.940Z");
        backup.put("headers", List.of(Map.of("key", "source", "value", base64("m13")), new HashMap<>(Map.of("key", "empty"))));
        SourceRecord restored = transform().apply(line("kafka-a/kafka3o-m10-verify/kafka3o-m10-verify-3-00000000000000000000.gz", backup));

        assertEquals("kafka3o-m10-verify", restored.topic());
        assertEquals(3, restored.kafkaPartition());
        assertArrayEquals("k-1".getBytes(StandardCharsets.UTF_8), (byte[]) restored.key());
        assertArrayEquals("backup test one".getBytes(StandardCharsets.UTF_8), (byte[]) restored.value());
        assertEquals(Instant.parse("2026-11-10T07:33:15.940Z").toEpochMilli(), restored.timestamp());
        List<Header> headers = new java.util.ArrayList<>();
        restored.headers().forEach(headers::add);
        assertEquals(List.of("source", "empty", "kafka3o-restore"), headers.stream().map(Header::key).toList());
        assertArrayEquals("m13".getBytes(StandardCharsets.UTF_8), (byte[]) headers.get(0).value());
        assertNull(headers.get(1).value());
        assertArrayEquals("r-1".getBytes(StandardCharsets.UTF_8), (byte[]) headers.get(2).value());
    }

    @Test
    void keepsNullKeysAndValues() {
        Map<String, Object> backup = new HashMap<>();
        backup.put("key", null);
        backup.put("value", null);
        backup.put("timestamp", "2026-11-08T23:43:18.540Z");
        backup.put("headers", List.of());
        SourceRecord restored = transform().apply(line("p/t-0-00000000000000000000.gz", backup));
        assertNull(restored.key());
        assertNull(restored.value());
    }

    @Test
    void dropsRecordsFromObjectsOutsideTheSnapshot() {
        Map<String, Object> backup = Map.of("value", base64("later"), "timestamp", "2026-11-10T08:00:00Z", "headers", List.of());
        assertNull(transform().apply(line("p/t-0-00000000000000000021.gz", backup)));
        assertNull(transform().apply(line("p/t-1-00000000000000000000.gz", backup)));
        assertNull(transform().apply(line("p/notes.txt", backup)));
    }

    @Test
    void failsOnMalformedLinesNamingTheObject() {
        for (Object value : new Object[] {"not a map", Map.of("timestamp", "yesterday", "headers", List.of()), Map.of("value", "%%%", "timestamp", "2026-11-10T08:00:00Z")}) {
            DataException failure = assertThrows(DataException.class, () -> transform().apply(new SourceRecord(
                Map.of("objectKey", "p/t-0-00000000000000000000.gz"), Map.of(), "t", null, null, null, null, value, 1L)));
            assertEquals(true, failure.getMessage().contains("p/t-0-00000000000000000000.gz"), failure.getMessage());
        }
    }

    @Test
    void requiresTheRestoreIdAndBoundary() {
        assertThrows(ConfigException.class, () -> new UnwrapBackupRecord<SourceRecord>().configure(Map.of("snapshot.boundary", "0:1")));
        assertThrows(ConfigException.class, () -> new UnwrapBackupRecord<SourceRecord>().configure(Map.of("restore.id", "r-1")));
    }
}
