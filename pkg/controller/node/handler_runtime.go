package nodecontroller

import (
	"context"
	"fmt"
	"strconv"

	eventsv1 "github.com/amimof/voiyd/api/services/events/v1"
	"github.com/amimof/voiyd/pkg/condition"
	errs "github.com/amimof/voiyd/pkg/errors"
	cevents "github.com/containerd/containerd/api/events"
)

func (c *Controller) onRuntimeTaskStart(ctx context.Context, obj *eventsv1.Event) error {
	var e cevents.TaskStart
	err := obj.GetObject().UnmarshalTo(&e)
	if err != nil {
		return err
	}

	tname, err := c.runtime.Name(ctx, e.GetContainerID())
	if err != nil {
		return err
	}

	lease, err := c.clientset.LeaseV1().Get(ctx, tname)
	if errs.IgnoreNotFound(err) != nil {
		return err
	}

	task, err := c.clientset.TaskV1().Get(ctx, tname)
	if err != nil {
		return err
	}

	// Only proceed if task is owned by us
	if lease.GetConfig().GetNodeId() == c.node.GetMeta().GetName() {
		c.logger.Info("received task start event from runtime", "task", e.GetContainerID(), "pid", e.GetPid())

		report := condition.NewForResource(task).As(c.node.GetMeta().GetName())

		md := map[string]string{
			"pid":  strconv.Itoa(int(e.GetPid())),
			"id":   e.GetContainerID(),
			"node": lease.GetConfig().GetNodeId(),
		}

		report.
			Type(condition.TaskScheduled).
			WithMetadata(md).
			True(condition.ReasonScheduled)

		_ = c.clientset.TaskV1().Condition(ctx, report.Report())

		report.
			Type(condition.TaskReady).
			WithMetadata(md).
			True(condition.ReasonRunning)

		return c.clientset.TaskV1().Condition(ctx, report.Report())
	}

	return nil
}

func (c *Controller) onRuntimeTaskExit(ctx context.Context, obj *eventsv1.Event) error {
	var e cevents.TaskExit
	err := obj.GetObject().UnmarshalTo(&e)
	if err != nil {
		return err
	}

	tname, err := c.runtime.Name(ctx, e.GetContainerID())
	if err != nil {
		return err
	}

	lease, err := c.clientset.LeaseV1().Get(ctx, tname)
	if errs.IgnoreNotFound(err) != nil {
		return err
	}

	task, err := c.clientset.TaskV1().Get(ctx, tname)
	if err != nil {
		return err
	}

	// Only proceed if task is owned by us
	if lease.GetConfig().GetNodeId() == c.node.GetMeta().GetName() {
		c.logger.Info("received task exit event from runtime", "exitCode", e.GetExitStatus(), "pid", e.GetPid(), "exitedAt", e.GetExitedAt())

		report := condition.NewForResource(task).As(c.node.GetMeta().GetName())

		exitStatus := ""
		if e.GetExitStatus() > 0 {
			exitStatus = fmt.Sprintf("exit status %d", e.GetExitStatus())
		}

		md := map[string]string{"exit_status": exitStatus}

		taskReport := report.
			Type(condition.TaskReady).
			WithMetadata(md).
			False(condition.ReasonStopped, exitStatus)

		return c.clientset.TaskV1().Condition(ctx, taskReport)
	}
	return nil
}

func (c *Controller) onRuntimeTaskDelete(ctx context.Context, obj *eventsv1.Event) error {
	var e cevents.TaskDelete
	err := obj.GetObject().UnmarshalTo(&e)
	if err != nil {
		return err
	}

	tname, err := c.runtime.Name(ctx, e.GetContainerID())
	if err != nil {
		return err
	}

	lease, err := c.clientset.LeaseV1().Get(ctx, tname)
	if errs.IgnoreNotFound(err) != nil {
		return err
	}

	task, err := c.clientset.TaskV1().Get(ctx, tname)
	if err != nil {
		return err
	}

	// Only proceed if task is owned by us
	if lease.GetConfig().GetNodeId() == c.node.GetMeta().GetName() {
		c.logger.Info("received task delete event from runtime", "task", e.GetContainerID(), "pid", e.GetPid())

		report := condition.NewForResource(task).As(c.node.GetMeta().GetName())
		md := map[string]string{
			"id":  "",
			"pid": "",
		}
		taskReport := report.
			Type(condition.TaskReady).
			WithMetadata(md).
			False(condition.ReasonStopped)

		return c.clientset.TaskV1().Condition(ctx, taskReport)
	}
	return nil
}
