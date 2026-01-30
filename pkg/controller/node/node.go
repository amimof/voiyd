// Package nodecontroller implemenets controller and provides logic for multiplexing node management
package nodecontroller

import (
	"context"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/amimof/voiyd/pkg/client"
	"github.com/amimof/voiyd/pkg/condition"
	errs "github.com/amimof/voiyd/pkg/errors"
	"github.com/amimof/voiyd/pkg/events"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/pkg/networking"
	"github.com/amimof/voiyd/pkg/runtime"
	"github.com/amimof/voiyd/pkg/volume"

	eventsv1 "github.com/amimof/voiyd/api/services/events/v1"
	logsv1 "github.com/amimof/voiyd/api/services/logs/v1"
	nodesv1 "github.com/amimof/voiyd/api/services/nodes/v1"
	tasksv1 "github.com/amimof/voiyd/api/services/tasks/v1"
)

type Controller struct {
	runtime          runtime.Runtime
	logger           logger.Logger
	clientset        *client.ClientSet
	tracer           trace.Tracer
	logChan          chan *logsv1.LogEntry
	activeLogStreams map[events.LogKey]context.CancelFunc
	logStreamsMu     sync.Mutex
	node             *nodesv1.Node
	attacher         volume.Attacher
	exchange         *events.Exchange
	renewInterval    time.Duration
	netmanager       networking.Manager
}

type NewOption func(c *Controller)

func WithLeaseRenewalInterval(d time.Duration) NewOption {
	return func(c *Controller) {
		c.renewInterval = d
	}
}

func WithVolumeAttacher(a volume.Attacher) NewOption {
	return func(c *Controller) {
		c.attacher = a
	}
}

func WithConfig(n *nodesv1.Node) NewOption {
	return func(c *Controller) {
		c.node = n
	}
}

func WithLogger(l logger.Logger) NewOption {
	return func(c *Controller) {
		c.logger = l
	}
}

func WithName(s string) NewOption {
	return func(c *Controller) {
		c.node.GetMeta().Name = s
	}
}

func WithExchange(e *events.Exchange) NewOption {
	return func(c *Controller) {
		c.exchange = e
	}
}

func WithNetworkManager(m networking.Manager) NewOption {
	return func(c *Controller) {
		c.netmanager = m
	}
}

// Run implements controller
func (c *Controller) Run(ctx context.Context) {
	nodeName := c.node.GetMeta().GetName()

	topics := []eventsv1.EventType{
		events.NodeDelete,
		events.NodeConnect,
		events.Schedule,
		events.TaskDelete,
		events.TaskStop,
		events.TaskKill,
		events.TailLogsStart,
		events.TailLogsStop,
	}

	// Subscribe to events
	ctx = metadata.AppendToOutgoingContext(ctx, "voiyd_controller_name", "node")
	evt, errCh := c.clientset.EventV1().Subscribe(ctx, topics...)

	// Setup Node Handlers
	c.clientset.EventV1().On(events.NodeDelete, c.onNodeDelete)
	c.clientset.EventV1().On(events.NodeConnect, c.onNodeConnect)
	c.clientset.EventV1().On(events.Schedule, events.HandleErrors(c.logger, events.HandleScheduling(c.onSchedule)))
	c.clientset.EventV1().On(events.TaskDelete, events.HandleErrors(c.logger, events.HandleTask(c.stopTask)))
	c.clientset.EventV1().On(events.TaskStop, events.HandleErrors(c.logger, events.HandleTask(c.stopTask)))
	c.clientset.EventV1().On(events.TaskKill, events.HandleErrors(c.logger, events.HandleTask(c.killTask)))
	c.clientset.EventV1().On(events.TailLogsStart, events.HandleErrors(c.logger, c.onLogStart))
	c.clientset.EventV1().On(events.TailLogsStop, events.HandleErrors(c.logger, c.onLogStop))

	go func() {
		for e := range evt {
			c.logger.Info("node controller received event", "event", e.GetType().String(), "clientID", nodeName, "objectID", e.GetObjectId())
		}
	}()

	// Handle runtime events
	runtimeChan := c.exchange.Subscribe(ctx, events.RuntimeTaskExit, events.RuntimeTaskStart, events.RuntimeTaskDelete)
	c.exchange.On(events.RuntimeTaskExit, events.HandleErrors(c.logger, c.onRuntimeTaskExit))
	c.exchange.On(events.RuntimeTaskStart, events.HandleErrors(c.logger, c.onRuntimeTaskStart))
	c.exchange.On(events.RuntimeTaskDelete, events.HandleErrors(c.logger, c.onRuntimeTaskDelete))

	go func() {
		for e := range runtimeChan {
			c.logger.Info("node controller received runtime event", "event", e.GetType().String(), "objectID", e.GetObjectId())
		}
	}()

	// Connect with retry logic
	connErr := make(chan error, 1)
	go func() {
		err := c.clientset.NodeV1().Connect(ctx, nodeName, evt, connErr)
		if err != nil {
			c.logger.Error("error connecting to server", "error", err)
		}
	}()

	// Reconcile
	go func() {
		if err := c.Reconcile(ctx); err != nil {
			c.logger.Warn("error reconciling", "error", err, "node", nodeName)
		}
	}()

	// Get hostname from environment
	hostname, err := os.Hostname()
	if err != nil {
		c.logger.Error("error retrieving hostname from environment", "error", err)
	}

	// Get version from runtime
	runtimeVer, err := c.runtime.Version(ctx)
	if err != nil {
		c.logger.Error("error retrieving version from runtime", "error", err)
	}

	// Report node status with metadata
	if node, err := c.clientset.NodeV1().Get(ctx, nodeName); err == nil {
		reporter := condition.NewForResource(node)
		_ = c.clientset.NodeV1().Condition(ctx, reporter.Type(condition.NodeReady).WithMetadata(map[string]string{"hostname": hostname, "runtime_version": runtimeVer}).True(condition.ReasonConnected))
	}

	// Start lease loop
	go c.renewLeases(ctx)

	// Handle errors
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if e != nil {
				c.logger.Error("received error on channel", "error", e)
			}
		case e, ok := <-connErr:
			if !ok {
				connErr = nil
				continue
			}
			if e != nil {
				c.logger.Error("received error on channel", "error", e)
			}
		}
	}
}

// renewLeases continuously renews leases for all running tasks
func (c *Controller) renewLeases(ctx context.Context) {
	ticker := time.NewTicker(c.renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.renewAllLeases(ctx)
		}
	}
}

func (c *Controller) renewAllLeases(ctx context.Context) {
	nodeName := c.node.GetMeta().GetName()

	// Get all running tasks on this node from runtime
	tasks, err := c.runtime.List(ctx)
	if err != nil {
		c.logger.Error("failed to list runtime tasks", "error", err)
		return
	}

	for _, task := range tasks {
		taskName, err := c.runtime.Name(ctx, task.GetMeta().GetName())
		if err != nil {
			continue
		}

		// Stop task if lease doesn't exist for it
		if _, err := c.clientset.LeaseV1().Get(ctx, taskName); err != nil {
			if errs.IsNotFound(err) {
				if err := c.stopTask(ctx, task); err != nil {
					c.logger.Error("error stopping task", "error", err, "task", taskName)
					continue
				}
			}
		}

		// Renew lease
		renewed, err := c.clientset.LeaseV1().Renew(ctx, taskName, nodeName)
		if err != nil {
			c.logger.Debug("couldn't renew lease, stopping task", "task", taskName)
			if err := c.stopTask(ctx, task); err != nil {
				c.logger.Error("error stopping task", "error", err, "task", taskName)
				continue
			}
			continue
		}

		if !renewed {
			c.logger.Warn("failed to renew lease", "task", taskName, "error", err)
		}

		c.logger.Debug("renewed lease, reconciling", "task", taskName)
	}
}

// Reconcile ensures that desired tasks matches with tasks
// in the runtime environment. It removes any tasks that are not
// desired (missing from the server) and adds those missing from runtime.
// It is preferrably run early during startup of the controller.
func (c *Controller) Reconcile(ctx context.Context) error {
	nodeID := c.node.GetMeta().GetName()

	// Renew leases on boot
	c.renewAllLeases(ctx)

	// Get running tasks from runtime
	runtimeTasks, err := c.runtime.List(ctx)
	if err != nil {
		return err
	}

	// Verify that the containers in the runtime are supposed to run. If a lease
	// for a running container cannot be acquired, stop it. Otherwise let it run.
	for _, task := range runtimeTasks {
		taskID := task.GetMeta().GetName()

		// Only acquire if task is supposed to be running
		if task.GetStatus().GetPhase().GetValue() != string(condition.ReasonRunning) {
			c.logger.Debug("skip lease acquisition because task is not running", "task", taskID)
			continue
		}

		// Try to acquire lease for this task
		ttl, acquired, err := c.clientset.LeaseV1().Acquire(ctx, taskID, nodeID)
		if err != nil {
			c.logger.Error("error acquiring lease during reconcile", "task", taskID, "error", err)
			continue
		}

		// Successfully acquired lease - we can keep running this task
		if acquired {
			c.logger.Info("acquierd lease for task", "task", taskID, "node", nodeID, "ttl", ttl)

			// Update task status to reflect actual state
			if err = c.clientset.TaskV1().Status().Update(ctx, taskID, &tasksv1.Status{
				Node: wrapperspb.String(nodeID),
			}, "node"); err != nil {
				c.logger.Warn("unable to update status", "error", err, "task", taskID, "node", nodeID)
			}

			continue
		}

		// Lease held by another node, stop running tasks in runtime
		c.logger.Warn("lease held by another node", "task", taskID, "node", nodeID)

		// Run cleanup early while netns still exists.
		// This will allow the CNI plugin to remove networks without leaking.
		err = c.runtime.Cleanup(ctx, taskID)
		if err != nil {
			return err
		}

		// Stop the task
		err = c.runtime.Stop(ctx, task)
		if errs.IgnoreNotFound(err) != nil {
			return err
		}

		// Remove any previous tasks ignoring any errors
		err = c.runtime.Delete(ctx, task)
		if err != nil {
			return err
		}

		// Detach volumes
		return c.attacher.Detach(ctx, c.node, task)

	}

	return nil
}

func New(c *client.ClientSet, n *nodesv1.Node, rt runtime.Runtime, opts ...NewOption) (*Controller, error) {
	m := &Controller{
		clientset:        c,
		runtime:          rt,
		netmanager:       &networking.UnimplementedManager{},
		logger:           logger.ConsoleLogger{},
		tracer:           otel.Tracer("controller"),
		logChan:          make(chan *logsv1.LogEntry),
		activeLogStreams: make(map[events.LogKey]context.CancelFunc),
		node:             n,
		attacher:         volume.NewDefaultAttacher(c.VolumeV1()),
		renewInterval:    time.Second * 30,
	}
	for _, opt := range opts {
		opt(m)
	}

	return m, nil
}
