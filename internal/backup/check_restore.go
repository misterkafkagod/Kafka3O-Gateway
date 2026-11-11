package backup

import (
	"context"
	"fmt"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

// CheckRestore reports a restore's progress and, once every snapshot object is fully restored, finishes it: the
// restore connector is deleted and the backup it paused is resumed. With a time window an object is done when its last
// matching line is committed (records the transform drops commit no offset).
type CheckRestore interface {
	Execute(ctx context.Context, topic string, connector connect.Connector) RestoreStatus
}

type checkRestore struct {
	settings Settings
	config   connect.FetchConnectorConfig
	objects  s3store.ListObjects
	scan     ScanObjectRecords
	offsets  ReadRestoreOffsets
	manage   connect.ManageConnector
}

func NewCheckRestore(settings Settings, config connect.FetchConnectorConfig, objects s3store.ListObjects, scan ScanObjectRecords,
	offsets ReadRestoreOffsets, manage connect.ManageConnector) CheckRestore {
	return checkRestore{settings: settings, config: config, objects: objects, scan: scan, offsets: offsets, manage: manage}
}

func (c checkRestore) Execute(ctx context.Context, topic string, connector connect.Connector) RestoreStatus {
	status := RestoreStatus{State: connector.State, Error: connector.Error}
	for _, task := range connector.Tasks {
		if task.State == "FAILED" {
			status.State = "FAILED"
			if status.Error == "" {
				status.Error = task.Error
			}
		}
	}
	config, err := c.config.Execute(ctx, connector.Name)
	if err != nil {
		status.Error = firstNonEmpty(status.Error, "restore progress is unavailable: "+err.Error())
		return status
	}
	status.RestoreID = config["transforms.unwrap.restore.id"]
	boundary, ok := ParseSnapshotBoundary(config["transforms.unwrap.snapshot.boundary"])
	if !ok {
		status.Error = firstNonEmpty(status.Error, "the restore has no valid snapshot boundary")
		return status
	}
	objects, err := c.objects.Execute(ctx, c.settings.Prefix+topic+"/")
	if err != nil {
		status.Error = firstNonEmpty(status.Error, "restore progress is unavailable: "+err.Error())
		return status
	}
	committed, err := c.offsets.Execute(ctx, connector.Name)
	if err != nil {
		status.Error = firstNonEmpty(status.Error, "restore progress is unavailable: "+err.Error())
		return status
	}
	compression := firstNonEmpty(config["file.compression.type"], "gzip")
	window := AllRecords
	if from, err := strconv.ParseInt(config["transforms.unwrap.time.from"], 10, 64); err == nil {
		window.From = from
	}
	if to, err := strconv.ParseInt(config["transforms.unwrap.time.to"], 10, 64); err == nil {
		window.To = to
	}
	for _, object := range objects {
		name, partition, offset, ok := ParseBackupObjectName(object.Key)
		highest, inSnapshot := boundary[partition]
		if !ok || name != topic || !inSnapshot || offset > highest || BackupObjectCompression(object.Key) != compression {
			continue
		}
		scan, err := c.scan.Execute(ctx, object, window)
		if err != nil {
			status.Error = firstNonEmpty(status.Error, "restore progress is unavailable: "+err.Error())
			return status
		}
		status.Total += scan.Matched
		switch {
		case window == AllRecords:
			status.Restored += min(committed[object.Key], scan.Matched)
		case scan.LastMatch > 0 && committed[object.Key] >= scan.LastMatch:
			status.Restored += scan.Matched
		}
	}
	if status.State != "RUNNING" || status.Error != "" || status.Restored < status.Total {
		return status
	}
	if err := c.manage.Execute(ctx, connect.Action{Kind: connect.ActionDelete, Connector: connector.Name}); err != nil {
		status.Error = "the restore is complete but could not be removed: " + err.Error()
		return status
	}
	if resume, _ := strconv.ParseBool(config["transforms.unwrap.resume.backup"]); resume {
		if err := c.manage.Execute(ctx, connect.Action{Kind: connect.ActionResume, Connector: BackupConnectorName(topic)}); err != nil {
			status.Error = fmt.Sprintf("the restore finished but the backup could not be resumed: %v", err)
		}
	}
	status.Finished = true
	return status
}
