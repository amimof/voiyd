package conditioncontroller

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/amimof/voiyd/pkg/client"
	"github.com/amimof/voiyd/pkg/condition"
	"github.com/amimof/voiyd/pkg/consts"
	errs "github.com/amimof/voiyd/pkg/errors"
	"github.com/amimof/voiyd/pkg/events"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/services/node"
	"github.com/amimof/voiyd/services/task"

	nodesv1 "github.com/amimof/voiyd/api/services/nodes/v1"
	tasksv1 "github.com/amimof/voiyd/api/services/tasks/v1"
	typesv1 "github.com/amimof/voiyd/api/types/v1"
)

type NewOption func(c *Controller)

func WithLogger(l logger.Logger) NewOption {
	return func(c *Controller) {
		c.logger = l
	}
}

func WithExchange(e *events.Exchange) NewOption {
	return func(c *Controller) {
		c.exchange = e
	}
}

type Controller struct {
	clientset *client.ClientSet
	logger    logger.Logger
	exchange  *events.Exchange
}

func (c *Controller) Run(ctx context.Context) {
	// Subscribe to events
	ctx = metadata.AppendToOutgoingContext(ctx, "voiyd_controller_name", "scheduler")
	_, err := c.clientset.EventV1().Subscribe(ctx, events.ConditionReported)

	// Setup Handlers
	c.clientset.EventV1().On(events.ConditionReported, events.HandleErrors(c.logger, events.HandleConditionReport(c.onConditionReported)))

	// Handle errors
	for e := range err {
		c.logger.Error("received error on channel", "error", e)
	}
}

func (c *Controller) validateGeneration(observedGen, currentGen uint64) bool {
	if observedGen != 0 && observedGen < currentGen {
		return false
	}
	return true
}

func (c *Controller) validateLease(ctx context.Context, task *tasksv1.Task, reportedHolder string) bool {
	// Skip validation of no reporter. This might be dangerous!
	// TODO: Look this over, feels sketchy
	if reportedHolder == "" {
		return true
	}

	taskID := task.GetMeta().GetName()
	lease, err := c.clientset.LeaseV1().Get(ctx, taskID)
	if err != nil {
		c.logger.Warn("lease validation failed, error getting lease for task", "error", err, "task", taskID)
		return false
	}

	expectedHolder := lease.GetConfig().GetNodeId()

	if expectedHolder != reportedHolder {
		c.logger.Warn("report is invalid, reporter is not the lease holder", "holder", expectedHolder, "reporter", reportedHolder)
		return false
	}

	return true
}

func (c *Controller) onNodeCondition(ctx context.Context, report *typesv1.ConditionReport, n *nodesv1.Node) error {
	resourceID := report.GetResourceId()
	observedGen := report.GetObservedGeneration()
	expectedGen := n.GetMeta().GetGeneration()

	// Validate generation (prevent stale updates)
	if !c.validateGeneration(uint64(observedGen), expectedGen) {
		c.logger.Warn("skipping stale node condition report",
			"reason", report.GetReason(),
			"status", report.GetStatus(),
			"node", resourceID,
			"observedGen", observedGen,
			"currentGen", expectedGen)
		return nil
	}

	// Convert ConditionReport to Condition
	newCondition := reportToCondition(report, n.GetStatus().GetConditions())

	// Merge with existing conditions
	updatedConditions := mergeCondition(n.GetStatus().GetConditions(), newCondition)

	// Derive fields from metadata
	hostname := getMetadataString(report, condition.NodeReady, "hostname")       // hostname
	runtime := getMetadataString(report, condition.NodeReady, "runtime_version") // runtime_version
	nodever := getMetadataString(report, condition.NodeReady, "node_version")    // node_version / upgraded_to

	// Derive phase from conditions
	phase := getPhaseFromConditions(updatedConditions, condition.ReasonReady)

	// Update Node status
	return c.clientset.NodeV1().Status().Update(
		ctx,
		resourceID,
		&nodesv1.Status{
			Conditions: updatedConditions,
			Phase:      wrapperspb.String(phase),
			Hostname:   hostname,
			Runtime:    runtime,
			Version:    nodever,
		},
		"conditions", "phase", "hostname", "runtime", "version",
	)
}

func (c *Controller) onTaskCondition(ctx context.Context, report *typesv1.ConditionReport, t *tasksv1.Task) error {
	resourceID := report.GetResourceId()
	observedGen := report.GetObservedGeneration()
	expectedGen := t.GetMeta().GetGeneration()
	reporterID := report.GetReporter()

	// Validate generation
	if !c.validateGeneration(uint64(observedGen), expectedGen) {
		c.logger.Warn("skipping stale node condition report",
			"reason", report.GetReason(),
			"status", report.GetStatus(),
			"node", resourceID,
			"observedGen", observedGen,
			"currentGen", expectedGen)
		return nil
	}

	// Validate lease ownership
	if !c.validateLease(ctx, t, reporterID) {
		c.logger.Warn("skipping unowned node condition report",
			"reason", report.GetReason(),
			"status", report.GetStatus(),
			"node", resourceID,
			"observedVer", observedGen,
			"currentVer", expectedGen)
		return nil
	}

	// Convert ConditionReport to Condition
	newCondition := reportToCondition(report, t.GetStatus().GetConditions())

	// Merge with existing conditions
	updatedConditions := mergeCondition(t.GetStatus().GetConditions(), newCondition)

	// Derive fields from metadata
	nodeID := getMetadataString(report, condition.TaskScheduled, "node")
	pid := getMetadataUInt32(report, condition.TaskReady, "pid")
	id := getMetadataString(report, condition.TaskReady, "id")
	ipaddr := getMetadataString(report, condition.NetworkReady, "ip_address") // node_version / upgraded_to

	// Derive phase from conditions
	phase := getPhaseFromConditions(updatedConditions, condition.ReasonRunning)

	// Update Task status
	return c.clientset.TaskV1().Status().Update(
		ctx,
		resourceID,
		&tasksv1.Status{
			Conditions: updatedConditions,
			Phase:      wrapperspb.String(phase),
			Node:       nodeID,
			Id:         id,
			Pid:        pid,
			Ip:         ipaddr,
			// Gw:         gateway,
		},
		"conditions", "phase", "node", "id", "pid", "ip",
	)
}

func (c *Controller) onConditionReported(ctx context.Context, report *typesv1.ConditionReport, resourceVersion string) error {
	c.logger.Debug("condition controller received a report", "resource_version", resourceVersion, "resource", report.GetResourceId(), "observedGeneration", report.GetObservedGeneration())

	resourceID := report.GetResourceId()

	switch resourceVersion {
	case node.Version:
		node, err := c.clientset.NodeV1().Get(ctx, resourceID)
		if err != nil {
			if errs.IsNotFound(err) {
				c.logger.Warn("condition report for non-existent node", "node", resourceID)
				return nil
			}
			return err
		}
		return c.onNodeCondition(ctx, report, node)
	case task.Version:
		task, err := c.clientset.TaskV1().Get(ctx, resourceID)
		if err != nil {
			if errs.IsNotFound(err) {
				c.logger.Warn("condition report for non-existent task", "task", resourceID)
				return nil
			}
			return err
		}
		return c.onTaskCondition(ctx, report, task)
	}

	return nil
}

func getMetadataString(report *typesv1.ConditionReport, t condition.Type, key string) *wrapperspb.StringValue {
	if condition.Type(report.GetType()) == condition.Type(t) {
		if v, ok := report.GetMetadata()[key]; ok {
			return v
		}
	}
	return nil
}

func getMetadataUInt32(report *typesv1.ConditionReport, t condition.Type, key string) *wrapperspb.UInt32Value {
	if condition.Type(report.GetType()) == condition.Type(t) {
		if v, ok := report.GetMetadata()[key]; ok {
			i, err := strconv.Atoi(v.GetValue())
			if err != nil {
				return wrapperspb.UInt32(0)
			}
			return wrapperspb.UInt32(uint32(i))
		}
	}
	return nil
}

func getPhaseFromConditions(conds []*typesv1.Condition, ready condition.Reason) string {
	if len(conds) == 0 {
		return consts.PHASEUNKNOWN
	}

	// Check if there are un-ready conditions (status==false)
	var hasfailed bool
	for _, c := range conds {
		if !c.GetStatus().GetValue() {
			hasfailed = true
			break
		}
	}

	// Condition is "Ready" when all statuses are true
	if !hasfailed {
		return string(ready)
	}

	// Derive phase from most recent condition
	var mostRecent *typesv1.Condition
	for _, c := range conds {
		if mostRecent == nil ||
			c.GetLastTransitionTime().AsTime().After(mostRecent.GetLastTransitionTime().AsTime()) {
			mostRecent = c
		}
	}

	return string(condition.Reason(mostRecent.GetReason().GetValue()))
}

func reportToCondition(report *typesv1.ConditionReport, conditions []*typesv1.Condition) *typesv1.Condition {
	// Find existing condition with same type to preserve last_transition_time if needed
	var existingCondition *typesv1.Condition
	for _, cond := range conditions {
		if cond.GetType().GetValue() == report.GetType() {
			existingCondition = cond
			break
		}
	}

	// Determine if status changed
	statusChanged := existingCondition == nil ||
		existingCondition.GetStatus() != wrapperspb.Bool(conditionStatusToBoolValue(report.GetStatus()))

	// Use existing timestamp if status hasn't changed, otherwise use now
	transitionTime := report.GetObservedAt()
	if !statusChanged && existingCondition != nil {
		transitionTime = existingCondition.GetLastTransitionTime()
	}

	return &typesv1.Condition{
		Type:               wrapperspb.String(report.GetType()),
		Status:             wrapperspb.Bool(conditionStatusToBoolValue(report.GetStatus())),
		Reason:             wrapperspb.String(report.GetReason()),
		Msg:                wrapperspb.String(report.GetMsg()),
		LastTransitionTime: transitionTime,
	}
}

func conditionStatusToBoolValue(status typesv1.ConditionStatus) bool {
	switch status {
	case typesv1.ConditionStatus_CONDITION_STATUS_TRUE:
		return true
	default:
		return false
	}
}

func mergeCondition(existing []*typesv1.Condition, new *typesv1.Condition) []*typesv1.Condition {
	// Clone existing to avoid mutations
	result := make([]*typesv1.Condition, 0, len(existing)+1)

	found := false
	for _, cond := range existing {
		if cond.GetType().GetValue() == new.GetType().GetValue() {
			// Replace with new condition
			result = append(result, new)
			found = true
		} else {
			// Keep other conditions unchanged
			result = append(result, cond)
		}
	}

	// If condition type doesn't exist, append it
	if !found {
		result = append(result, new)
	}

	return result
}

func New(cs *client.ClientSet, opts ...NewOption) *Controller {
	c := &Controller{
		clientset: cs,
		logger:    logger.ConsoleLogger{},
	}
	for _, opt := range opts {
		opt(c)
	}

	return c
}
