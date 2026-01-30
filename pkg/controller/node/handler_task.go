package nodecontroller

import (
	"context"
	"errors"

	"github.com/containerd/errdefs"
	gocni "github.com/containerd/go-cni"
	"google.golang.org/protobuf/proto"

	"github.com/amimof/voiyd/pkg/condition"
	errs "github.com/amimof/voiyd/pkg/errors"
	"github.com/amimof/voiyd/pkg/labels"
	"github.com/amimof/voiyd/pkg/networking"

	nodesv1 "github.com/amimof/voiyd/api/services/nodes/v1"
	tasksv1 "github.com/amimof/voiyd/api/services/tasks/v1"
)

func (c *Controller) isNodeSelected(ctx context.Context, task *tasksv1.Task) bool {
	node, err := c.clientset.NodeV1().Get(ctx, c.node.GetMeta().GetName())
	if err != nil {
		return false
	}
	return labels.NewCompositeSelectorFromMap(task.GetConfig().GetNodeSelector()).Matches(node.GetMeta().GetLabels())
}

func (c *Controller) killTask(ctx context.Context, task *tasksv1.Task) error {
	ctx, span := c.tracer.Start(ctx, "controller.node.OnTaskKill")
	defer span.End()

	taskID := task.GetMeta().GetName()
	nodeID := c.node.GetMeta().GetName()

	// Release lease
	defer func() {
		err := c.clientset.LeaseV1().Release(ctx, taskID, nodeID)
		if err != nil {
			c.logger.Warn("unable to release lease", "error", err, "task", taskID, "nodeID", nodeID)
		}
	}()

	report := condition.NewForResource(task).As(c.node.GetMeta().GetName())

	// Detach network
	err := c.detachNetwork(ctx, task, report)
	if err != nil {
		return err
	}

	// Remove any previous tasks
	err = c.runtime.Kill(ctx, task)
	if err != nil {
		return err
	}

	err = c.detachMounts(ctx, task, report)
	if !errdefs.IsNotFound(err) {
		return err
	}

	// Detach volumes
	return c.deleteTask(ctx, task, report)
}

func (c *Controller) stopTask(ctx context.Context, task *tasksv1.Task) error {
	ctx, span := c.tracer.Start(ctx, "controller.node.OnTaskStop")
	defer span.End()

	taskID := task.GetMeta().GetName()
	nodeID := c.node.GetMeta().GetName()

	// Release lease
	defer func() {
		err := c.clientset.LeaseV1().Release(ctx, taskID, nodeID)
		if err != nil {
			c.logger.Warn("unable to release lease", "error", err, "task", taskID, "nodeID", nodeID)
		}
	}()

	report := condition.NewForResource(task).As(c.node.GetMeta().GetName())

	// Detach volumes
	err := c.detachMounts(ctx, task, report)
	if err != nil {
		return err
	}

	// Detach network
	err = c.detachNetwork(ctx, task, report)
	if err != nil {
		return err
	}

	// Stop the task
	err = c.runtime.Stop(ctx, task)
	if errs.IgnoreNotFound(err) != nil {
		return err
	}

	// Remove any previous tasks
	return c.deleteTask(ctx, task, report)
}

func (c *Controller) acquireLease(ctx context.Context, task *tasksv1.Task) error {
	taskID := task.GetMeta().GetName()
	nodeID := c.node.GetMeta().GetName()

	ttl, expired, err := c.clientset.LeaseV1().Acquire(ctx, taskID, nodeID)
	if err != nil {
		c.logger.Error("failed to acquire lease", "error", err, "task", taskID, "nodeID", nodeID)
		return err
	}

	// Release if task can't be provisioned
	defer func() {
		if err != nil {
			err = c.clientset.LeaseV1().Release(ctx, taskID, nodeID)
			if err != nil {
				c.logger.Warn("unable to release lease", "task", taskID, "node", nodeID)
			}
		}
	}()

	if !expired {
		c.logger.Warn("lease held by another node", "task", taskID)
		return errors.New("lease held by another another")
	}

	c.logger.Info("acquired lease for task", "task", taskID, "node", nodeID, "ttl", ttl)
	return nil
}

func (c *Controller) deleteTask(ctx context.Context, task *tasksv1.Task, report *condition.Report) error {
	ctx, span := c.tracer.Start(ctx, "controller.node.OnTaskDelete")
	defer span.End()

	taskID := task.GetMeta().GetName()

	// Run cleanup early while netns still exists.
	// This will allow the CNI plugin to remove networks without leaking.
	_ = c.runtime.Cleanup(ctx, taskID)

	// Remove any previous tasks ignoring any errors

	report.
		Type(condition.TaskReady).
		False(condition.ReasonDeleting)

	_ = c.clientset.TaskV1().Condition(ctx, report.Report())
	err := c.runtime.Delete(ctx, task)
	if err != nil {
		report.
			Type(condition.TaskReady).
			False(condition.ReasonDeleteFailed, err.Error())
		_ = c.clientset.TaskV1().Condition(ctx, report.Report())
		return err
	}

	report.
		Type(condition.TaskScheduled).
		WithMetadata(map[string]string{"node": ""}).
		False(condition.ReasonStopped)

	_ = c.clientset.TaskV1().Condition(ctx, report.Report())

	report.
		Type(condition.TaskReady).
		WithMetadata(map[string]string{"pid": "", "id": ""}).
		False(condition.ReasonStopped)

	return c.clientset.TaskV1().Condition(ctx, report.Report())
}

func (c *Controller) attachMounts(ctx context.Context, task *tasksv1.Task, report *condition.Report) error {
	// Prepare volumes/mounts
	report.
		Type(condition.VolumeReady).
		False(condition.ReasonAttaching)
	_ = c.clientset.TaskV1().Condition(ctx, report.Report())

	if err := c.attacher.PrepareMounts(ctx, c.node, task); err != nil {
		report.
			Type(condition.VolumeReady).
			False(condition.ReasonAttachFailed, err.Error())
		_ = c.clientset.TaskV1().Condition(ctx, report.Report())
		return err
	}

	report.
		Type(condition.VolumeReady).
		True(condition.ReasonAttached)

	return c.clientset.TaskV1().Condition(ctx, report.Report())
}

func (c *Controller) detachMounts(ctx context.Context, task *tasksv1.Task, report *condition.Report) error {
	// Prepare volumes/mounts
	report.
		Type(condition.VolumeReady).
		False(condition.ReasonDetaching)
	_ = c.clientset.TaskV1().Condition(ctx, report.Report())

	if err := c.attacher.Detach(ctx, c.node, task); err != nil {
		report.
			Type(condition.ImageReady).
			False(condition.ReasonPullFailed)
		_ = c.clientset.TaskV1().Condition(ctx, report.Report())
		return err
	}

	report.
		Type(condition.VolumeReady).
		False(condition.ReasonDetached)
	return c.clientset.TaskV1().Condition(ctx, report.Report())
}

func (c *Controller) pullImage(ctx context.Context, task *tasksv1.Task, report *condition.Report) error {
	// Pull image
	report.
		Type(condition.ImageReady).
		False(condition.ReasonPulling)

	_ = c.clientset.TaskV1().Condition(ctx, report.Report())

	err := c.runtime.Pull(ctx, task)
	if err != nil {
		report.
			Type(condition.ImageReady).
			False(condition.ReasonPullFailed, err.Error())
		_ = c.clientset.TaskV1().Condition(ctx, report.Report())
		return err
	}

	report.
		Type(condition.ImageReady).
		True(condition.ReasonPulled)

	return c.clientset.TaskV1().Condition(ctx, report.Report())
}

func (c *Controller) onSchedule(ctx context.Context, task *tasksv1.Task, _ *nodesv1.Node) error {
	if !c.isNodeSelected(ctx, task) {
		c.logger.Debug("declining schedule request due to selector mismatch", "selector", task.GetConfig().GetNodeSelector())
		return nil
	}
	return c.startTask(ctx, task)
}

// Returns a version of task that is comparable by stripping fields that
// should be omitted when comparing with proto.Equal for example
func comparable(task *tasksv1.Task) *tasksv1.Task {
	t := proto.Clone(task).(*tasksv1.Task)
	t.Status = nil
	t.GetMeta().ResourceVersion = 0
	return t
}

func (c *Controller) startTask(ctx context.Context, task *tasksv1.Task) error {
	ctx, span := c.tracer.Start(ctx, "controller.node.OnTaskStart")
	defer span.End()

	err := c.acquireLease(ctx, task)
	if err != nil {
		return err
	}

	t, err := c.runtime.Get(ctx, task.GetMeta().GetName())
	if errs.IgnoreNotFound(err) != nil {
		return err
	}

	// Skip task provision if no changes are made. Ignore status-fields when comparing
	if t != nil {
		if proto.Equal(comparable(t), comparable(task)) {
			c.logger.Debug("task does not require re-provisioning", "task", task.GetMeta().GetName())
			return nil
		}
	}

	report := condition.NewForResource(task).As(c.node.GetMeta().GetName())

	err = c.deleteTask(ctx, task, report)
	if err != nil {
		return err
	}

	err = c.attachMounts(ctx, task, report)
	if err != nil {
		return err
	}

	err = c.pullImage(ctx, task, report)
	if err != nil {
		return err
	}

	err = c.runtime.Run(ctx, task)
	if err != nil {
		return err
	}

	return c.attachNetwork(ctx, task, report)
}

func (c *Controller) detachNetwork(ctx context.Context, task *tasksv1.Task, report *condition.Report) error {
	_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonDetaching))

	id, err := c.runtime.ID(ctx, task.GetMeta().GetName())
	if err != nil {
		_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonDetachFailed, err.Error()))
		return err
	}

	pid, err := c.runtime.Pid(ctx, id)
	if err != nil {
		_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonDetachFailed, err.Error()))
		return err
	}

	pm := networking.ParseCNIPortMappings(task.GetConfig().PortMappings...)
	attachOpts := []gocni.NamespaceOpts{gocni.WithCapabilityPortMap(pm), gocni.WithArgs("IgnoreUnknown", "true")}

	// Delete CNI Network
	err = c.netmanager.Detach(ctx, id, pid, attachOpts...)
	if err != nil {
		_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonDetachFailed, err.Error()))
		return err
	}

	md := map[string]string{
		"ip_address": "",
		"gateway":    "",
	}

	return c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).WithMetadata(md).True(condition.ReasonDetached))
}

func (c *Controller) attachNetwork(ctx context.Context, task *tasksv1.Task, report *condition.Report) error {
	_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonAttaching))

	id, err := c.runtime.ID(ctx, task.GetMeta().GetName())
	if err != nil {
		_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonAttachFailed, err.Error()))
		return err
	}

	pid, err := c.runtime.Pid(ctx, id)
	if err != nil {
		_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonAttachFailed, err.Error()))
		return err
	}

	pm := networking.ParseCNIPortMappings(task.GetConfig().PortMappings...)
	attachOpts := []gocni.NamespaceOpts{gocni.WithCapabilityPortMap(pm), gocni.WithArgs("IgnoreUnknown", "true")}

	res, err := c.netmanager.Attach(ctx, id, pid, attachOpts...)
	if err != nil {
		_ = c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).False(condition.ReasonAttachFailed, err.Error()))
		return err
	}

	var ipaddr, gw string

	for i, inter := range res.Interfaces {
		for _, ipcfg := range inter.IPConfigs {
			if i == "eth1" {
				ipaddr = ipcfg.IP.String()
				gw = ipcfg.Gateway.String()
				break
			}
		}
	}

	md := map[string]string{
		"ip_address": ipaddr,
		"gateway":    gw,
	}

	return c.clientset.TaskV1().Condition(ctx, report.Type(condition.NetworkReady).WithMetadata(md).True(condition.ReasonAttached))
}
