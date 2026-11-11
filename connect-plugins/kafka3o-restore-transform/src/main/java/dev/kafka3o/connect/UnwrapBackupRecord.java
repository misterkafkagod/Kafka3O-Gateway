package dev.kafka3o.connect;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Restores one Kafka3O backup line (Aiven S3 sink jsonl with base64 key, value and headers) as the original record,
 * marks it with the restore header so backups skip it, and drops records from objects outside the restore's snapshot
 * or with timestamps outside its optional time window.
 * The lifecycle methods are required by Kafka Connect's {@link Transformation} interface.
 */
public class UnwrapBackupRecord<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String MARKER_HEADER = "kafka3o-restore";
    private static final ConfigDef CONFIG = new ConfigDef()
        .define("restore.id", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Value of the kafka3o-restore header")
        .define("snapshot.boundary", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            "Highest backup object start offset per partition, as partition:offset,...")
        .define("resume.backup", ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Whether the Gateway resumes the backup afterwards")
        .define("time.from", ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Earliest record timestamp kept, epoch ms (inclusive)")
        .define("time.to", ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Record timestamps kept are before this, epoch ms (exclusive)");

    private byte[] restoreId;
    private SnapshotBoundary boundary;
    private TimeWindow window;

    @Override
    public void configure(Map<String, ?> configs) {
        Object id = configs.get("restore.id");
        if (id == null || id.toString().isBlank()) {
            throw new ConfigException("restore.id is required");
        }
        restoreId = id.toString().getBytes(StandardCharsets.UTF_8);
        Object specification = configs.get("snapshot.boundary");
        boundary = new SnapshotBoundary(specification == null ? null : specification.toString());
        Object from = configs.get("time.from");
        Object to = configs.get("time.to");
        window = new TimeWindow(from == null ? null : from.toString(), to == null ? null : to.toString());
    }

    @Override
    public R apply(R record) {
        String objectKey = record instanceof SourceRecord source && source.sourcePartition() != null
            ? String.valueOf(source.sourcePartition().get("objectKey")) : null;
        Optional<BackupObjectKey> key = BackupObjectKey.parse(objectKey);
        if (key.isEmpty() || !boundary.includes(key.get())) {
            return null;
        }
        if (!(record.value() instanceof Map<?, ?> line)) {
            throw new DataException("backup object " + objectKey + " holds a line that is not a Kafka3O backup record");
        }
        try {
            Base64.Decoder base64 = Base64.getDecoder();
            byte[] originalKey = line.get("key") == null ? null : base64.decode((String) line.get("key"));
            byte[] originalValue = line.get("value") == null ? null : base64.decode((String) line.get("value"));
            long timestamp = Instant.parse((String) line.get("timestamp")).toEpochMilli();
            if (!window.includes(timestamp)) {
                return null;
            }
            ConnectHeaders headers = new ConnectHeaders();
            if (line.get("headers") instanceof List<?> list) {
                for (Object entry : list) {
                    Map<?, ?> header = (Map<?, ?>) entry;
                    Object headerValue = header.get("value");
                    headers.addBytes((String) header.get("key"), headerValue == null ? null : base64.decode((String) headerValue));
                }
            }
            headers.addBytes(MARKER_HEADER, restoreId.clone());
            return record.newRecord(record.topic(), key.get().partition(), Schema.OPTIONAL_BYTES_SCHEMA, originalKey,
                Schema.OPTIONAL_BYTES_SCHEMA, originalValue, timestamp, headers);
        } catch (ClassCastException | IllegalArgumentException | NullPointerException | DateTimeParseException malformed) {
            throw new DataException("backup object " + objectKey + " holds a malformed backup record", malformed);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }

    @Override
    public void close() {
    }
}
