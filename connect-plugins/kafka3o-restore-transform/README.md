# kafka3o-restore-transform

A Kafka Connect transformation that restores Kafka3O topic backups losslessly. It is used with Aiven's S3 source
connector (3.4.3) on backups written by Aiven's S3 sink in the format the Gateway configures (gzip jsonl with base64
key, value and headers). See the UI repository's
[s3-backup-connectors discovery](../../../Kafka3O-UI/spec/discovery/s3-backup-connectors.md).

- `dev.kafka3o.connect.UnwrapBackupRecord`: turns each backup line back into the original record (key, value,
  headers, timestamp, partition), adds the header `kafka3o-restore: <restore.id>` so the topic's backup skips it, and
  drops records from objects newer than `snapshot.boundary` (`partition:offset,...`, the highest object start offset
  per partition when the restore started).

Build with Java 21 (Temurin 21.0.13) and Maven 3.9.16:

```bash
mvn -B verify          # tests and target/kafka3o-restore-transform-1.0.0.jar
```

Install by copying the jar into its own directory on the Connect worker's `plugin.path` and restarting the worker.
