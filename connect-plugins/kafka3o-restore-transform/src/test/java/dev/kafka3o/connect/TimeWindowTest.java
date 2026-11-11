package dev.kafka3o.connect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class TimeWindowTest {
    @Test
    void keepsTheStartAndDropsTheEnd() {
        TimeWindow window = new TimeWindow("1000", "2000");
        assertFalse(window.includes(999));
        assertTrue(window.includes(1000));
        assertTrue(window.includes(1999));
        assertFalse(window.includes(2000));
    }

    @Test
    void missingBoundsAreOpen() {
        assertTrue(new TimeWindow(null, null).includes(Long.MIN_VALUE));
        assertTrue(new TimeWindow("", " ").includes(Long.MAX_VALUE - 1));
        assertFalse(new TimeWindow("1000", null).includes(999));
        assertTrue(new TimeWindow("1000", null).includes(Long.MAX_VALUE - 1));
        assertTrue(new TimeWindow(null, "1000").includes(999));
        assertFalse(new TimeWindow(null, "1000").includes(1000));
    }

    @Test
    void refusesInvalidWindows() {
        assertThrows(ConfigException.class, () -> new TimeWindow("2000", "1000"));
        assertThrows(ConfigException.class, () -> new TimeWindow("1000", "1000"));
        assertThrows(ConfigException.class, () -> new TimeWindow("yesterday", null));
        assertThrows(ConfigException.class, () -> new TimeWindow(null, "2026-11-11T00:00:00Z"));
    }
}
