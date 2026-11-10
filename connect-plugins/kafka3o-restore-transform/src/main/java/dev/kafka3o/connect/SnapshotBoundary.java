package dev.kafka3o.connect;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

/**
 * The newest backup object per partition that existed when a restore started, written as {@code partition:offset,...}.
 * Objects outside it were written later and must not be restored.
 */
public final class SnapshotBoundary {
    private final Map<Integer, Long> highestStartOffsets = new HashMap<>();

    public SnapshotBoundary(String specification) {
        if (specification == null || specification.isBlank()) {
            throw new ConfigException("snapshot.boundary must list partition:offset pairs");
        }
        for (String pair : specification.split(",")) {
            String[] parts = pair.trim().split(":");
            try {
                if (parts.length != 2 || highestStartOffsets.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1])) != null) {
                    throw new ConfigException("snapshot.boundary has an invalid or repeated entry: " + pair);
                }
            } catch (NumberFormatException invalid) {
                throw new ConfigException("snapshot.boundary has an invalid entry: " + pair);
            }
        }
    }

    public boolean includes(BackupObjectKey key) {
        Long highest = highestStartOffsets.get(key.partition());
        return highest != null && key.startOffset() <= highest;
    }
}
