package dev.kafka3o.connect;

import org.apache.kafka.common.config.ConfigException;

/**
 * The record timestamps a restore keeps, {@code [from, to)} in epoch milliseconds; a missing bound is open.
 * Records outside it are dropped after unwrapping.
 */
public final class TimeWindow {
    private final long from;
    private final long to;

    public TimeWindow(String fromSpecification, String toSpecification) {
        long[] bounds = {Long.MIN_VALUE, Long.MAX_VALUE};
        String[] specifications = {fromSpecification, toSpecification};
        for (int i = 0; i < bounds.length; i++) {
            if (specifications[i] != null && !specifications[i].isBlank()) {
                try {
                    bounds[i] = Long.parseLong(specifications[i].trim());
                } catch (NumberFormatException invalid) {
                    throw new ConfigException((i == 0 ? "time.from" : "time.to") + " must be epoch milliseconds");
                }
            }
        }
        from = bounds[0];
        to = bounds[1];
        if (from >= to) {
            throw new ConfigException("time.from must be before time.to");
        }
    }

    public boolean includes(long timestamp) {
        return timestamp >= from && timestamp < to;
    }
}
