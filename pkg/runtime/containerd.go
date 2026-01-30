package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"

	// "github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/errdefs"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/filters"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	gocni "github.com/containerd/go-cni"
	"github.com/opencontainers/runtime-spec/specs-go"
	"go.opentelemetry.io/otel"

	errs "github.com/amimof/voiyd/pkg/errors"
	"github.com/amimof/voiyd/pkg/labels"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/pkg/networking"
	"github.com/amimof/voiyd/pkg/store"
	"github.com/amimof/voiyd/pkg/util"

	tasksv1 "github.com/amimof/voiyd/api/services/tasks/v1"
)

const (
	labelPrefix = "voiyd"
	logFileName = "stdout.log"
)

var tracer = otel.GetTracerProvider().Tracer("voiyd-node")

type ContainerdRuntime struct {
	client       *containerd.Client
	logger       logger.Logger
	ns           string
	mu           sync.Mutex
	containerIOs map[string]*TaskIO
	logDirFmt    string
	store        store.Store
}

type NewContainerdRuntimeOption func(c *ContainerdRuntime)

// WithLogDirFmt allows for setting the root directory for container log files (stdout).
// For example /var/lib/voiyd/containers/%s/log which happens to be the default location.
func WithLogDirFmt(rootPath string) NewContainerdRuntimeOption {
	return func(c *ContainerdRuntime) {
		c.logDirFmt = rootPath
	}
}

func WithLogger(l logger.Logger) NewContainerdRuntimeOption {
	return func(c *ContainerdRuntime) {
		c.logger = l
	}
}

func WithStore(s store.Store) NewContainerdRuntimeOption {
	return func(c *ContainerdRuntime) {
		c.store = s
	}
}

// withMounts creates an oci compatible list of volume mounts to be used by the runtime.
// Default mount type is read-write bind mount if not otherwise specified.
func withMounts(m []*tasksv1.Mount) oci.SpecOpts {
	var mounts []specs.Mount
	for _, mount := range m {
		mountType := mount.Type
		mountOptions := mount.Options
		if mountType == "" {
			mountType = "bind"
		}
		if len(mountOptions) == 0 {
			mountOptions = []string{"rbind", "rw"}
		}
		mounts = append(mounts, specs.Mount{
			Destination: mount.Destination,
			Type:        mountType,
			Source:      mount.Source,
			Options:     mountOptions,
		})
	}
	return oci.WithMounts(mounts)
}

func withEnvVars(envs []*tasksv1.EnvVar) oci.SpecOpts {
	envVars := make([]string, len(envs))
	for i, env := range envs {
		envVars[i] = fmt.Sprintf("%s=%s", env.GetName(), env.GetValue())
	}
	return oci.WithEnv(envVars)
}

func WithNamespace(ns string) NewContainerdRuntimeOption {
	return func(c *ContainerdRuntime) {
		c.ns = ns
	}
}

func withUser(user string) []oci.SpecOpts {
	var opts []oci.SpecOpts
	if user != "" {
		opts = append(opts, oci.WithUser(user), withResetAdditionalGIDs(), oci.WithAdditionalGIDs(user))
	}
	return opts
}

func withResetAdditionalGIDs() oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
		s.Process.User.AdditionalGids = nil
		return nil
	}
}

func withContainerLabels(l labels.Label, task *tasksv1.Task) containerd.NewContainerOpts {
	pm := task.GetConfig().GetPortMappings()

	// Convert to CNI type once here
	cniPorts := networking.ParseCNIPortMappings(pm...)

	// Store CNI mappings, not API mappings
	b, _ := json.Marshal(&cniPorts)

	// Fill label set with values
	l.Set("voiyd/revision", util.Uint64ToString(task.GetMeta().GetGeneration()))
	l.Set("voiyd/created", task.GetMeta().GetCreated().String())
	l.Set("voiyd/updated", task.GetMeta().GetUpdated().String())
	l.Set("voiyd/name", task.GetMeta().GetName())
	l.Set("voiyd/namespace", "voiyd")
	l.Set("voiyd/ports", string(b))

	return containerd.WithContainerLabels(l)
}

// Namespace returns the namespace used for runnning workload. If supported by the runtime
func (c *ContainerdRuntime) Namespace() string {
	return c.ns
}

// Version returns the version of the runtime. Should be printent in the form "runtime_name/version"
func (c *ContainerdRuntime) Version(ctx context.Context) (string, error) {
	// c.client.
	ver, err := c.client.Version(ctx)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("containerd/%s", ver.Version), nil
}

// Cleanup performs any tasks necessary to clean up the environment from danglig configuration.
// Such as tearing down the network.
func (c *ContainerdRuntime) Cleanup(ctx context.Context, id string) error {
	ctx, span := tracer.Start(ctx, "runtime.containerd.Cleanup")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)
	ctr, err := c.get(ctx, id)
	if err != nil {
		return err
	}

	t, err := ctr.Task(ctx, nil)
	if err != nil {
		return err
	}

	// Get metadata from labels
	ctrLabels, err := ctr.Labels(ctx)
	if err != nil {
		return err
	}

	// Unmarshal ports label
	b := ctrLabels["voiyd/ports"]
	cniports := []gocni.PortMapping{}
	err = json.Unmarshal([]byte(b), &cniports)
	if err != nil {
		return err
	}

	logFile := path.Join(fmt.Sprintf(c.logDirFmt, id), logFileName)
	c.logger.Debug("cleaning log file for container", "id", ctr.ID(), "name", id, "pid", t.Pid(), "logFile", logFile)
	return os.WriteFile(logFile, []byte{}, 0o666)
}

func (c *ContainerdRuntime) List(ctx context.Context, filter ...string) ([]*tasksv1.Task, error) {
	ctx, span := tracer.Start(ctx, "runtime.containerd.List")
	defer span.End()

	// We're only interested in containers with the voiyd.io/name label
	filters := []string{
		`labels."voiyd.io/name"`,
	}
	filters = append(filters, filter...)

	ctx = namespaces.WithNamespace(ctx, c.ns)
	ctrs, err := c.client.Containers(ctx, filters...)
	if err != nil {
		return nil, err
	}

	result := make([]*tasksv1.Task, len(ctrs))
	for i, ctr := range ctrs {

		cl, err := ctr.Labels(ctx)
		if err != nil {
			return nil, err
		}

		taskName, ok := cl["voiyd.io/name"]
		if !ok {
			continue
		}

		var t tasksv1.Task
		err = c.store.Load(taskName, &t)
		if err != nil {
			return nil, err
		}

		result[i] = &t
	}

	return result, nil
}

// Get returns the first container from the runtime that matches the provided id
func (c *ContainerdRuntime) Get(ctx context.Context, taskName string) (*tasksv1.Task, error) {
	// Get from runtime first to verify that the task is provisioned
	_, err := c.get(ctx, taskName)
	if err != nil {
		return nil, err
	}

	var t tasksv1.Task
	err = c.store.Load(taskName, &t)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// gets a containerd-container using voiyd task names.
func (c *ContainerdRuntime) get(ctx context.Context, taskName string) (containerd.Container, error) {
	ctx, span := tracer.Start(ctx, "runtime.containerd.get")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)

	cfilters := []string{
		fmt.Sprintf(`labels."voiyd.io/name"=="%s"`, regexp.QuoteMeta(taskName)),
		fmt.Sprintf("id~=^%s.*$", regexp.QuoteMeta(taskName)),
	}

	_, err := filters.ParseAll(cfilters...)
	if err != nil {
		return nil, err
	}

	ctrs, err := c.client.Containers(ctx, cfilters...)
	if err != nil {
		return nil, err
	}

	// Not found in runtime, remove from store
	if len(ctrs) == 0 {
		err = c.store.Delete(taskName)
		if errs.IgnoreNotFound(err) != nil {
			return nil, err
		}
		return nil, errdefs.ErrNotFound
	}

	return ctrs[0], nil
}

func (c *ContainerdRuntime) Pull(ctx context.Context, task *tasksv1.Task) error {
	ctx, span := tracer.Start(ctx, "runtime.containerd.Pull")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)
	_, err := c.client.Pull(ctx, task.Config.Image, containerd.WithPullUnpack)
	if err != nil {
		return err
	}
	return nil
}

// Delete deletes the container and any tasks associated with it.
// Tasks will be forcefully stopped if running.
func (c *ContainerdRuntime) Delete(ctx context.Context, t *tasksv1.Task) error {
	ctx, span := tracer.Start(ctx, "runtime.containerd.Delete")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)

	// Get the container from runtime. If container isn't found, then assume that it's already been deleted
	container, err := c.get(ctx, t.GetMeta().GetName())
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}

	// Return any error that isn't NotFound
	task, err := container.Task(ctx, nil)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return err
		}
	}

	// Make sure to stop tasks before deleting container. Using Kill here which is a forceful operation
	if task != nil {
		err = c.Kill(ctx, t)
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		}
	}

	// Clean up IO streams
	c.mu.Lock()
	if io, exists := c.containerIOs[t.GetMeta().GetName()]; exists {
		if io.Stdout != nil {
			_ = io.Stdout.Close()
		}
		if io.Stderr != nil {
			_ = io.Stderr.Close()
		}
		delete(c.containerIOs, t.GetMeta().GetName())
	}
	c.mu.Unlock()

	// Delete the task
	_, err = task.Delete(ctx)
	if err != nil {
		return err
	}

	// Delete the container
	err = container.Delete(ctx, containerd.WithSnapshotCleanup)
	if err != nil {
		return fmt.Errorf("error deleting container and its tasks: %v", err)
	}

	// Delete from store
	return c.store.Delete(t.GetMeta().GetName())
}

// Stop stops containers associated with the name of provided container instance.
// Stop will attempt to gracefully stop tasks, but will eventually do it forcefully
// if timeout is reached. Stop does not perform any garbage colletion and it is
// up to the caller to call Cleanup() after calling Stop()
func (c *ContainerdRuntime) Stop(ctx context.Context, t *tasksv1.Task) error {
	ctx, span := tracer.Start(ctx, "runtime.containerd.Stop")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)
	cont, err := c.get(ctx, t.GetMeta().GetName())
	if err != nil {
		return err
	}

	task, err := cont.Task(ctx, cio.Load)
	if err != nil {
		return err
	}

	// Wait
	waitChan, err := task.Wait(ctx)
	if err != nil {
		return err
	}

	// Attempt gracefull shutdown
	if err := task.Kill(ctx, syscall.SIGTERM); err != nil {
		return fmt.Errorf("failed to send SIGTERM: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case exitStatus := <-waitChan:
		if err := exitStatus.Error(); err != nil {
			return fmt.Errorf("failed to get exit status from task: %w", err)
		}
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timed out waiting for task to stop")
	}
}

// Kill forcefully stops the container and tasks within by sending SIGKILL to the process.
// Like Stop(), Kill() does not perform garbage collection. Use Gleanup() for this.
func (c *ContainerdRuntime) Kill(ctx context.Context, t *tasksv1.Task) error {
	ctx, span := tracer.Start(ctx, "runtime.containerd.Kill")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)

	cont, err := c.get(ctx, t.GetMeta().GetName())
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}

	task, err := cont.Task(ctx, cio.Load)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}

	// Attempt to forcefully kill the task
	if err = task.Kill(ctx, syscall.SIGKILL); err != nil {
		if !errdefs.IsNotFound(err) {
			return err
		}
	}

	// Wait
	exitChan, err := task.Wait(ctx)
	if err != nil {
		return err
	}

	// Handle exit status
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case exitStatus := <-exitChan:
			if err := exitStatus.Error(); err != nil {
				return fmt.Errorf("failed to get exit status from task: %w", err)
			}
			return nil
		case <-time.After(10 * time.Second):
			c.logger.Info("deadline exceeded waiting for task to exit", "container", cont.ID(), "task", task.ID())
			return os.ErrDeadlineExceeded
		}
	}
}

func (c *ContainerdRuntime) Run(ctx context.Context, t *tasksv1.Task) error {
	ctx, span := tracer.Start(ctx, "runtime.containerd.Run")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)

	// Get the image. Assumes that image has been pulled beforehand
	image, err := c.client.GetImage(ctx, t.GetConfig().GetImage())
	if err != nil {
		return err
	}

	// Build OCI specification
	opts := []oci.SpecOpts{
		oci.WithDefaultSpec(),
		oci.WithDefaultUnixDevices,
		oci.WithImageConfig(image),
		oci.WithHostname(t.GetMeta().GetName()),
		oci.WithAddedCapabilities(t.GetConfig().GetCapabilities().GetAdd()),
		oci.WithDroppedCapabilities(t.GetConfig().GetCapabilities().GetDrop()),
		withEnvVars(t.GetConfig().GetEnvvars()),
		withMounts(t.GetConfig().GetMounts()),
	}

	// Add privileged flag
	if t.GetConfig().GetPrivileged() {
		opts = append(opts, oci.WithPrivileged)
	}

	// Add user otps
	opts = append(opts, withUser(t.GetConfig().GetUser())...)

	// Add args opts
	if len(t.GetConfig().GetArgs()) > 0 {
		opts = append(opts, oci.WithProcessArgs(t.GetConfig().GetArgs()...))
	}

	// Assemble some labels
	l := labels.New()
	l.Set("voiyd.io/name", t.GetMeta().GetName())

	// Generate ID for the container
	containerID := GenerateID()
	containerName := t.GetMeta().GetName()

	// Create container
	cont, err := c.client.NewContainer(
		ctx,
		containerID.String(),
		containerd.WithImage(image),
		containerd.WithNewSnapshot(fmt.Sprintf("%s-snapshot", containerName), image),
		containerd.WithNewSpec(opts...),
		withContainerLabels(l, t),
	)
	if err != nil {
		return err
	}

	// Pipe stdout and stderr to log file on disk
	logRoot := fmt.Sprintf(c.logDirFmt, containerName)
	stdOut := filepath.Join(logRoot, logFileName)
	ioCreator := cio.LogFile(stdOut)

	// Create task
	task, err := cont.NewTask(ctx, ioCreator)
	if err != nil {
		return err
	}

	// Create task in store
	err = c.store.Save(containerName, t)
	if err != nil {
		return err
	}

	// Start the  task
	err = task.Start(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *ContainerdRuntime) Labels(ctx context.Context, id string) (labels.Label, error) {
	ctx, span := tracer.Start(ctx, "runtime.containerd.Labels")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)

	ctr, err := c.get(ctx, id)
	if err != nil {
		return nil, err
	}

	cl, err := ctr.Labels(ctx)
	if err != nil {
		return nil, err
	}

	l := labels.New()
	l.AppendMap(cl)

	return l, err
}

func (c *ContainerdRuntime) IO(ctx context.Context, id string) (*TaskIO, error) {
	logRoot := fmt.Sprintf(c.logDirFmt, id)
	stdOut := filepath.Join(logRoot, logFileName)

	f, err := os.OpenFile(stdOut, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	return &TaskIO{Stdout: f}, nil
}

func (c *ContainerdRuntime) ID(ctx context.Context, id string) (string, error) {
	ctx, span := tracer.Start(ctx, "runtime.containerd.ID")
	defer span.End()

	ctx = namespaces.WithNamespace(ctx, c.ns)

	ctr, err := c.get(ctx, id)
	if err != nil {
		return "", err
	}

	return ctr.ID(), nil
}

func (c *ContainerdRuntime) Name(ctx context.Context, id string) (string, error) {
	ctx = namespaces.WithNamespace(ctx, c.Namespace())
	ctx, span := tracer.Start(ctx, "runtime.containerd.Name")
	defer span.End()

	ctr, err := c.get(ctx, id)
	if err != nil {
		return "", err
	}

	l, err := ctr.Labels(ctx)
	if err != nil {
		return "", err
	}

	cName, ok := l["voiyd.io/name"]
	if !ok {
		return "", errdefs.ErrNotFound
	}

	return cName, nil
}

func (c *ContainerdRuntime) Pid(ctx context.Context, id string) (uint32, error) {
	ctx = namespaces.WithNamespace(ctx, c.Namespace())
	ctx, span := tracer.Start(ctx, "runtime.containerd.Pid")
	defer span.End()

	ctr, err := c.get(ctx, id)
	if err != nil {
		return 0, err
	}

	t, err := ctr.Task(ctx, nil)
	if err != nil {
		return 0, err
	}

	return t.Pid(), nil
}

func NewContainerdRuntimeClient(client *containerd.Client, opts ...NewContainerdRuntimeOption) *ContainerdRuntime {
	runtime := &ContainerdRuntime{
		client:       client,
		logger:       logger.ConsoleLogger{},
		ns:           DefaultNamespace,
		containerIOs: map[string]*TaskIO{},
		logDirFmt:    "/var/lib/voiyd/containers/%s/log",
		store:        store.NewEphemeralStore(),
	}

	for _, opt := range opts {
		opt(runtime)
	}

	return runtime
}
