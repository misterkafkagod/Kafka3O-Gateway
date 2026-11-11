package dev.kafka3o.connect;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The partition and first offset encoded in a backup object name: {@code <prefix><topic>-<partition>-<start offset>}
 * plus the compression's extension ({@code .gz}, {@code .zst}, {@code .snappy}, or none).
 */
public record BackupObjectKey(int partition, long startOffset) {
    private static final Pattern NAME = Pattern.compile("^.+-(\\d{1,10})-(\\d{1,20})(\\.gz|\\.zst|\\.snappy)?$");

    public static Optional<BackupObjectKey> parse(String objectKey) {
        if (objectKey == null) {
            return Optional.empty();
        }
        String name = objectKey.substring(objectKey.lastIndexOf('/') + 1);
        Matcher matcher = NAME.matcher(name);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        try {
            return Optional.of(new BackupObjectKey(Integer.parseInt(matcher.group(1)), Long.parseLong(matcher.group(2))));
        } catch (NumberFormatException tooLarge) {
            return Optional.empty();
        }
    }
}
