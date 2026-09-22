package cluster

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/service/cluster"
)

// commandIDExtension is the x-command-id key every operation carries
// (TECH-SPEC O5).
const commandIDExtension = "x-command-id"

func commandExtension(id string) map[string]any {
	return map[string]any{commandIDExtension: id}
}

// Register wires the cluster commands onto humaAPI (FUNC-SPEC §8.7 C1, C2,
// C4; TECH-SPEC §6.2).
func Register(humaAPI huma.API, svc *cluster.Service) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-describe",
		Method:      http.MethodGet,
		Path:        "/v1/cluster",
		Summary:     "Describe the cluster",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C1"),
	}, describeCluster(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-broker-config",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/brokers/{brokerId}/config",
		Summary:     "Describe a broker's configuration",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C2"),
	}, describeBrokerConfig(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-health",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/health",
		Summary:     "Cluster health summary",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C4"),
	}, clusterHealth(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-quorum",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/quorum",
		Summary:     "KRaft quorum status",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C6"),
	}, quorum(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-reassignments-list",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/reassignments",
		Summary:     "List in-progress partition reassignments",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C7"),
	}, listReassignments(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-log-dirs",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/log-dirs",
		Summary:     "Broker log directory usage",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C8"),
	}, logDirs(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-throughput",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/throughput",
		Summary:     "Sample messages-per-second for one or every topic",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C10"),
	}, throughput(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-export",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/export",
		Summary:     "Export topic definitions",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C11"),
	}, export(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-alter-broker-config",
		Method:      http.MethodPatch,
		Path:        "/v1/cluster/brokers/{brokerId}/config",
		Summary:     "Alter a broker's configuration",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C5"),
	}, alterBrokerConfig(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-reassign",
		Method:      http.MethodPost,
		Path:        "/v1/cluster/reassignments",
		Summary:     "Reassign partition replicas",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C9"),
	}, reassign(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-cancel-reassignments",
		Method:      http.MethodPost,
		Path:        "/v1/cluster/reassignments/cancel",
		Summary:     "Cancel in-progress partition reassignments",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C9"),
	}, cancelReassignments(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-elections",
		Method:      http.MethodPost,
		Path:        "/v1/cluster/elections",
		Summary:     "Trigger a leader election",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C9"),
	}, elections(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topics-apply",
		Method:      http.MethodPost,
		Path:        "/v1/batch/topics/apply",
		Summary:     "Reconcile topic definitions against the cluster",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C12"),
	}, applyTopics(svc))
}

func quorum(svc *cluster.Service) func(context.Context, *QuorumInput) (*QuorumOutput, error) {
	return func(ctx context.Context, _ *QuorumInput) (*QuorumOutput, error) {
		status, err := svc.Quorum(ctx)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &QuorumOutput{Body: toQuorumBody(status)}, nil
	}
}

func listReassignments(svc *cluster.Service) func(context.Context, *ListReassignmentsInput) (*ListReassignmentsOutput, error) {
	return func(ctx context.Context, _ *ListReassignmentsInput) (*ListReassignmentsOutput, error) {
		reassignments, err := svc.Reassignments(ctx)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ListReassignmentsOutput{Body: ListReassignmentsBody{Items: toReassignmentDTOs(reassignments)}}, nil
	}
}

func logDirs(svc *cluster.Service) func(context.Context, *LogDirsInput) (*LogDirsOutput, error) {
	return func(ctx context.Context, in *LogDirsInput) (*LogDirsOutput, error) {
		dirs, err := svc.LogDirs(ctx, in.BrokerID)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &LogDirsOutput{Body: LogDirsBody{Items: toBrokerLogDirDTOs(dirs)}}, nil
	}
}

func throughput(svc *cluster.Service) func(context.Context, *ThroughputInput) (*ThroughputOutput, error) {
	return func(ctx context.Context, in *ThroughputInput) (*ThroughputOutput, error) {
		items, seconds, err := svc.Throughput(ctx, in.Topic, in.Seconds)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ThroughputOutput{Body: toThroughputBody(items, seconds)}, nil
	}
}

func export(svc *cluster.Service) func(context.Context, *ExportInput) (*ExportOutput, error) {
	return func(ctx context.Context, in *ExportInput) (*ExportOutput, error) {
		e, err := svc.Export(ctx, in.Pattern)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ExportOutput{Body: toExportBody(e)}, nil
	}
}

func alterBrokerConfig(svc *cluster.Service) func(context.Context, *AlterBrokerConfigInput) (*AlterBrokerConfigOutput, error) {
	return func(ctx context.Context, in *AlterBrokerConfigInput) (*AlterBrokerConfigOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.AlterBrokerConfig(ctx, caller, in.BrokerID, in.Body.Set, in.Body.Reset, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p cluster.AlterBrokerConfigPlan) any { return toAlterBrokerConfigPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &AlterBrokerConfigOutput{Body: AlterBrokerConfigBody{DryRun: true, Plan: toAlterBrokerConfigPlanDTO(result.Plan)}}, nil
		}
		return &AlterBrokerConfigOutput{Body: AlterBrokerConfigBody{
			BrokerID: result.Value.BrokerID, Configs: toBrokerConfigEntryDTOs(result.Value.Configs),
		}}, nil
	}
}

func reassign(svc *cluster.Service) func(context.Context, *ReassignInput) (*ReassignOutput, error) {
	return func(ctx context.Context, in *ReassignInput) (*ReassignOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Reassign(ctx, caller, toReassignMoves(in.Body.Reassignments), in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p cluster.ReassignPlan) any { return toReassignPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &ReassignOutput{Status: http.StatusOK, Body: ReassignBody{DryRun: true, Plan: toReassignPlanDTO(result.Plan)}}, nil
		}
		return &ReassignOutput{
			Status: bulkStatus(result.Value.Summary.Failed),
			Body:   ReassignBody{Items: toBulkItemResultDTOs(result.Value.Items), Summary: toBulkSummaryDTO(result.Value.Summary)},
		}, nil
	}
}

func cancelReassignments(svc *cluster.Service) func(context.Context, *CancelReassignmentsInput) (*ReassignOutput, error) {
	return func(ctx context.Context, in *CancelReassignmentsInput) (*ReassignOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.CancelReassignments(ctx, caller, toCancelTargets(in.Body.Cancel), in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p cluster.ReassignPlan) any { return toReassignPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &ReassignOutput{Status: http.StatusOK, Body: ReassignBody{DryRun: true, Plan: toReassignPlanDTO(result.Plan)}}, nil
		}
		return &ReassignOutput{
			Status: bulkStatus(result.Value.Summary.Failed),
			Body:   ReassignBody{Items: toBulkItemResultDTOs(result.Value.Items), Summary: toBulkSummaryDTO(result.Value.Summary)},
		}, nil
	}
}

func elections(svc *cluster.Service) func(context.Context, *ElectionsInput) (*ElectionsOutput, error) {
	return func(ctx context.Context, in *ElectionsInput) (*ElectionsOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		preferred := in.Body.Elect.Type != "UNCLEAN"
		result, err := svc.Elect(ctx, caller, preferred, toElectTargets(in.Body.Elect.Partitions), in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p cluster.ElectPlan) any { return toElectPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &ElectionsOutput{Status: http.StatusOK, Body: ElectionsBody{DryRun: true, Plan: toElectPlanDTO(result.Plan)}}, nil
		}
		return &ElectionsOutput{
			Status: bulkStatus(result.Value.Summary.Failed),
			Body:   ElectionsBody{Items: toBulkItemResultDTOs(result.Value.Items), Summary: toBulkSummaryDTO(result.Value.Summary)},
		}, nil
	}
}

func applyTopics(svc *cluster.Service) func(context.Context, *ApplyTopicsInput) (*ApplyTopicsOutput, error) {
	return func(ctx context.Context, in *ApplyTopicsInput) (*ApplyTopicsOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Import(ctx, caller, toImportTopics(in.Body.Topics), in.Body.AllowDelete, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p cluster.ImportPlan) any { return toImportPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &ApplyTopicsOutput{Status: http.StatusOK, Body: ApplyTopicsBody{DryRun: true, Plan: toImportPlanDTO(result.Plan)}}, nil
		}
		body, status := toImportResultBody(result.Value)
		return &ApplyTopicsOutput{Status: status, Body: body}, nil
	}
}

func describeCluster(svc *cluster.Service) func(context.Context, *DescribeClusterInput) (*DescribeClusterOutput, error) {
	return func(ctx context.Context, _ *DescribeClusterInput) (*DescribeClusterOutput, error) {
		info, err := svc.DescribeCluster(ctx)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &DescribeClusterOutput{Body: DescribeClusterBody{
			ClusterID:    info.ClusterID,
			ControllerID: info.ControllerID,
			Brokers:      toBrokerDTOs(info.Brokers),
		}}, nil
	}
}

func describeBrokerConfig(svc *cluster.Service) func(context.Context, *DescribeBrokerConfigInput) (*DescribeBrokerConfigOutput, error) {
	return func(ctx context.Context, in *DescribeBrokerConfigInput) (*DescribeBrokerConfigOutput, error) {
		configs, err := svc.DescribeBrokerConfig(ctx, in.BrokerID)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &DescribeBrokerConfigOutput{Body: DescribeBrokerConfigBody{
			BrokerID: in.BrokerID,
			Configs:  toBrokerConfigEntryDTOs(configs),
		}}, nil
	}
}

func clusterHealth(svc *cluster.Service) func(context.Context, *ClusterHealthInput) (*ClusterHealthOutput, error) {
	return func(ctx context.Context, _ *ClusterHealthInput) (*ClusterHealthOutput, error) {
		summary, err := svc.HealthSummary(ctx)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ClusterHealthOutput{Body: toHealthSummaryBody(summary)}, nil
	}
}
