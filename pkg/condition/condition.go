package condition

import (
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	tasksv1 "github.com/amimof/voiyd/api/services/tasks/v1"
	typesv1 "github.com/amimof/voiyd/api/types/v1"
)

const (
	RuntimeReady  Type = "RuntimeReady"
	ImageReady    Type = "ImageReady"
	VolumeReady   Type = "VolumeReady"
	NetworkReady  Type = "NetworkReady"
	TaskReady     Type = "TaskReady"
	TaskScheduled Type = "TaskScheduled"
	NodeReady     Type = "NodeReady"

	ReasonPulling    Reason = "Pulling"
	ReasonPulled     Reason = "Pulled"
	ReasonPullFailed Reason = "PullFailed"

	ReasonAttaching    Reason = "Attaching"
	ReasonAttached     Reason = "Attached"
	ReasonAttachFailed Reason = "AttachFailed"

	ReasonDetaching    Reason = "Detaching"
	ReasonDetached     Reason = "Detached"
	ReasonDetachFailed Reason = "DetachFailed"

	ReasonCreating     Reason = "Creating"
	ReasonCreated      Reason = "Created"
	ReasonCreateFailed Reason = "CreateFailed"

	ReasonStarting    Reason = "Starting"
	ReasonStarted     Reason = "Started"
	ReasonStartFailed Reason = "StartFailed"

	ReasonRunning   Reason = "Running"
	ReasonRunFailed Reason = "RunFailed"

	ReasonStopping   Reason = "Stopping"
	ReasonStopped    Reason = "Stopped"
	ReasonStopFailed Reason = "StopFailed"

	ReasonDeleting     Reason = "Deleting"
	ReasonDeleted      Reason = "Deleted"
	ReasonDeleteFailed Reason = "FailedFailed"

	ReasonScheduling       Reason = "Scheduling"
	ReasonScheduled        Reason = "Scheduled"
	ReasonSchedulingFailed Reason = "SchedulingFailed"

	ReasonConnecting       Reason = "Connecting"
	ReasonConnected        Reason = "Connected"
	ReasonConnectionFailed Reason = "ConnectionFailed"

	ReasonDisconnecting  Reason = "Disconnecting"
	ReasonDisconnected   Reason = "Disconnected"
	ReasonDisconntFailed Reason = "DisconnectFailed"

	ReasonUpgrading     Reason = "Upgrading"
	ReasonUpgraded      Reason = "Upgraded"
	ReasonUpgradeFailed Reason = "UpgradeFailed"

	ReasonUnhealthy Reason = "Unhealthy"
	ReasonReady     Reason = "Ready"
)

type (
	Type   string
	Reason string
)

type Report struct {
	report *typesv1.ConditionReport
}

type Resource interface {
	GetMeta() *typesv1.Meta
}

func NewReport(resourceID string, gen uint64) *Report {
	return &Report{
		report: &typesv1.ConditionReport{
			ResourceId:              resourceID,
			ObservedResourceVersion: gen,
		},
	}
}

func NewReportFor(task *tasksv1.Task) *Report {
	return NewReport(task.GetMeta().GetName(), task.GetMeta().GetGeneration())
}

func NewForResource(res Resource) *Report {
	return &Report{
		report: &typesv1.ConditionReport{
			ResourceId:              res.GetMeta().GetName(),
			ObservedResourceVersion: res.GetMeta().GetResourceVersion(),
			ObservedAt:              timestamppb.Now(),
		},
	}
}

func (r *Report) As(name string) *Report {
	r.report.Reporter = name
	return r
}

func (r *Report) Report() *typesv1.ConditionReport {
	return r.report
}

func (r *Report) Type(t Type) *Report {
	r.report.Type = string(t)
	r.report.ObservedAt = timestamppb.Now()
	return r
}

func (r *Report) WithMetadata(m map[string]string) *Report {
	valMap := make(map[string]*wrapperspb.StringValue)
	for k, v := range m {
		valMap[k] = wrapperspb.String(v)
	}
	r.report.Metadata = valMap
	return r
}

func (r *Report) True(reason Reason, msg ...string) *typesv1.ConditionReport {
	r.report.Status = typesv1.ConditionStatus_CONDITION_STATUS_TRUE
	r.report.Reason = string(reason)
	if len(msg) > 0 {
		r.report.Msg = string(msg[0])
	}
	return r.report
}

func (r *Report) False(reason Reason, msg ...string) *typesv1.ConditionReport {
	r.report.Status = typesv1.ConditionStatus_CONDITION_STATUS_FALSE
	r.report.Reason = string(reason)
	if len(msg) > 0 {
		r.report.Msg = string(msg[0])
	}
	return r.report
}

func (r *Report) InProgress(reason Reason, msg string) *typesv1.ConditionReport {
	r.report.Status = typesv1.ConditionStatus_CONDITION_STATUS_UNKNOWN
	r.report.Reason = string(reason)
	r.report.Msg = string(msg)
	return r.report
}
