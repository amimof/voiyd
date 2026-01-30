package task

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/amimof/voiyd/pkg/events"
	"github.com/amimof/voiyd/pkg/labels"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/pkg/protoutils"
	"github.com/amimof/voiyd/pkg/repository"

	tasksv1 "github.com/amimof/voiyd/api/services/tasks/v1"
	"github.com/amimof/voiyd/api/types/v1"
)

type local struct {
	repo     repository.TaskRepository
	mu       sync.Mutex
	exchange *events.Exchange
	logger   logger.Logger
}

var (
	_      tasksv1.TaskServiceClient = &local{}
	tracer                           = otel.GetTracerProvider().Tracer("voiyd-server")
)

func (l *local) handleError(err error, msg string, keysAndValues ...any) error {
	def := []any{"error", err.Error()}
	def = append(def, keysAndValues...)
	l.logger.Error(msg, def...)
	if errors.Is(err, repository.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func applyMaskedUpdate(dst, src *tasksv1.Status, mask *fieldmaskpb.FieldMask) error {
	if mask == nil || len(mask.Paths) == 0 {
		return status.Error(codes.InvalidArgument, "update_mask is required")
	}

	for _, p := range mask.Paths {
		switch p {
		case "ip":
			if src.Ip == nil {
				continue
			}
			dst.Ip = src.Ip
		case "node":
			if src.Node == nil {
				continue
			}
			dst.Node = src.Node
		case "phase":
			if src.Phase == nil {
				continue
			}
			dst.Phase = src.Phase
		case "id":
			if src.Id == nil {
				continue
			}
			dst.Id = src.Id
		case "reason":
			if src.Reason == nil {
				continue
			}
			dst.Reason = src.Reason
		case "pid":
			if src.Pid == nil {
				continue
			}
			dst.Pid = src.Pid
		case "conditions":
			if src.Conditions == nil {
				continue
			}
			dst.Conditions = src.Conditions
		default:
			return fmt.Errorf("unknown mask path %q", p)
		}
	}

	return nil
}

// Merge lists strategically using merge keys
func merge(base, patch *tasksv1.Task) *tasksv1.Task {
	merged := protoutils.StrategicMerge(base, patch,
		func(b, p *tasksv1.Task) {
			if patch.Config == nil {
				return
			}
			b.Config.Envvars = protoutils.MergeSlices(b.Config.Envvars, p.Config.Envvars,
				func(e *tasksv1.EnvVar) string {
					return e.Name
				},
				func(b, p *tasksv1.EnvVar) *tasksv1.EnvVar {
					if p.Value != "" {
						b.Value = p.Value
					}
					return b
				},
			)
		},
		func(b, p *tasksv1.Task) {
			if patch.Config == nil {
				return
			}
			b.Config.PortMappings = protoutils.MergeSlices(b.Config.PortMappings, p.Config.PortMappings,
				func(e *tasksv1.PortMapping) string {
					return e.Name
				},
				func(b, p *tasksv1.PortMapping) *tasksv1.PortMapping {
					if p.TargetPort != 0 {
						b = p
					}
					return b
				},
			)
		},
		func(b, p *tasksv1.Task) {
			if patch.Config == nil {
				return
			}
			b.Config.Mounts = protoutils.MergeSlices(b.Config.Mounts, p.Config.Mounts,
				func(e *tasksv1.Mount) string {
					return e.Name
				},
				func(b, p *tasksv1.Mount) *tasksv1.Mount {
					return p
				},
			)
		},
		func(b, p *tasksv1.Task) {
			if patch.Config == nil {
				return
			}
			b.Config.Args = protoutils.MergeSlices(b.Config.Args, p.Config.Args,
				func(e string) string {
					return e
				},
				func(b, p string) string {
					if p != "" {
						b = p
					}
					return b
				},
			)
		},
	)
	return merged
}

func (l *local) Get(ctx context.Context, req *tasksv1.GetRequest, _ ...grpc.CallOption) (*tasksv1.GetResponse, error) {
	ctx, span := tracer.Start(ctx, "task.Get", trace.WithSpanKind(trace.SpanKindServer))
	span.SetAttributes(
		attribute.String("service", "Task"),
		attribute.String("task.id", req.GetId()),
	)
	defer span.End()

	// Get task from repo
	task, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		span.RecordError(err)
		return nil, l.handleError(err, "couldn't GET task from repo", "name", req.GetId())
	}

	span.SetAttributes(attribute.String("task.name", task.GetMeta().GetName()))

	return &tasksv1.GetResponse{
		Task: task,
	}, nil
}

func (l *local) List(ctx context.Context, req *tasksv1.ListRequest, _ ...grpc.CallOption) (*tasksv1.ListResponse, error) {
	ctx, span := tracer.Start(ctx, "task.List")
	defer span.End()

	// Get tasks from repo
	ctrs, err := l.Repo().List(ctx, req.GetSelector())
	if err != nil {
		return nil, l.handleError(err, "couldn't LIST tasks from repo")
	}
	return &tasksv1.ListResponse{
		Tasks: ctrs,
	}, nil
}

func (l *local) Create(ctx context.Context, req *tasksv1.CreateRequest, _ ...grpc.CallOption) (*tasksv1.CreateResponse, error) {
	ctx, span := tracer.Start(ctx, "task.Create")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	task := req.GetTask()
	taskID := task.GetMeta().GetName()

	// Check if task already exists
	if existing, _ := l.Get(ctx, &tasksv1.GetRequest{Id: taskID}); existing != nil {
		return nil, fmt.Errorf("task %s already exists", task.GetMeta().GetName())
	}

	task.GetMeta().Created = timestamppb.Now()
	task.GetMeta().Updated = timestamppb.Now()
	task.GetMeta().Generation = 1
	task.GetMeta().ResourceVersion = 1

	// Initialize status field if empty
	if task.GetStatus() == nil {
		task.Status = &tasksv1.Status{}
	}

	// Create task in repo
	err := l.Repo().Create(ctx, task)
	if err != nil {
		return nil, l.handleError(err, "couldn't CREATE task in repo", "name", taskID)
	}

	// Get the created task from repo
	task, err = l.Repo().Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), task.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), task.GetVersion())

	// Publish event that task is created
	err = l.exchange.Forward(ctx, events.NewEvent(events.TaskCreate, task, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing CREATE event", "name", task.GetMeta().GetName(), "event", "TaskCreate")
	}

	return &tasksv1.CreateResponse{
		Task: task,
	}, nil
}

// Delete publishes a delete request and the subscribers are responsible for deleting resources.
// Once they do, they will update there resource with the status Deleted
func (l *local) Delete(ctx context.Context, req *tasksv1.DeleteRequest, _ ...grpc.CallOption) (*tasksv1.DeleteResponse, error) {
	ctx, span := tracer.Start(ctx, "task.Delete")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	task, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, l.handleError(err, "couldn't GET task from repo", "id", req.GetId())
	}
	err = l.Repo().Delete(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), task.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), task.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(events.TaskDelete, task, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing DELETE event", "name", task.GetMeta().GetName(), "event", "TaskDelete")
	}
	return &tasksv1.DeleteResponse{
		Id: req.GetId(),
	}, nil
}

func (l *local) Kill(ctx context.Context, req *tasksv1.KillRequest, _ ...grpc.CallOption) (*tasksv1.KillResponse, error) {
	ctx, span := tracer.Start(ctx, "task.Kill")
	defer span.End()

	task, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	ev := events.TaskStop
	if req.ForceKill {
		ev = events.TaskKill
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), task.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), task.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(ev, task, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing STOP/KILL event", "name", req.GetId(), "event", ev.String())
	}
	return &tasksv1.KillResponse{
		Id: req.GetId(),
	}, nil
}

func (l *local) Start(ctx context.Context, req *tasksv1.StartRequest, _ ...grpc.CallOption) (*tasksv1.StartResponse, error) {
	ctx, span := tracer.Start(ctx, "task.Start")
	defer span.End()

	task, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), task.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), task.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(events.TaskStart, task, eventLabels))
	if err != nil {
		return nil, err
	}

	return &tasksv1.StartResponse{
		Id: req.GetId(),
	}, nil
}

func (l *local) Patch(ctx context.Context, req *tasksv1.PatchRequest, _ ...grpc.CallOption) (*tasksv1.PatchResponse, error) {
	ctx, span := tracer.Start(ctx, "task.Patch")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	updateTask := req.GetTask()

	// Get existing task from repo
	existing, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, l.handleError(err, "couldn't GET task from repo", "name", updateTask.GetMeta().GetName())
	}

	// Generate field mask
	genFieldMask, err := protoutils.GenerateFieldMask(existing, updateTask)
	if err != nil {
		return nil, err
	}

	// Handle partial update
	maskedUpdate, err := protoutils.ApplyFieldMaskToNewMessage(updateTask, genFieldMask)
	if err != nil {
		return nil, err
	}

	// TODO: Handle errors
	updated := maskedUpdate.(*tasksv1.Task)
	existing = merge(existing, updated)
	existing.GetMeta().Generation++
	existing.GetMeta().ResourceVersion++

	// Update the task
	err = l.Repo().Update(ctx, existing)
	if err != nil {
		return nil, l.handleError(err, "couldn't PATCH task in repo", "name", existing.GetMeta().GetName())
	}

	// Retreive the task again so that we can include it in an event
	task, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Only publish if spec is updated
	if !proto.Equal(updateTask.Config, task.Config) {

		// Decorate label with some labels
		eventLabels := labels.New()
		eventLabels.Set(labels.LabelPrefix("object-id").String(), task.GetMeta().GetName())
		eventLabels.Set(labels.LabelPrefix("object-version").String(), task.GetVersion())

		err = l.exchange.Forward(ctx, events.NewEvent(events.TaskPatch, task, eventLabels))
		if err != nil {
			return nil, l.handleError(err, "error publishing PATCH event", "name", existing.GetMeta().GetName(), "event", "TaskUpdate")
		}
	}

	return &tasksv1.PatchResponse{
		Task: existing,
	}, nil
}

// UpdateStatus implements tasks.TaskServiceClient.
func (l *local) UpdateStatus(ctx context.Context, req *tasksv1.UpdateStatusRequest, opts ...grpc.CallOption) (*tasksv1.UpdateStatusResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ctx, span := tracer.Start(ctx, "task.UpdateStatus")
	defer span.End()

	// Get the existing task before updating so we can compare specs
	existingTask, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Apply mask safely
	base := proto.Clone(existingTask.Status).(*tasksv1.Status)
	if err := applyMaskedUpdate(base, req.Status, req.UpdateMask); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad mask: %v", err)
	}

	existingTask.GetMeta().ResourceVersion++
	existingTask.Status = base

	if err := l.Repo().Update(ctx, existingTask); err != nil {
		return nil, err
	}

	return &tasksv1.UpdateStatusResponse{
		Id: existingTask.GetMeta().GetName(),
	}, nil
}

func (l *local) Update(ctx context.Context, req *tasksv1.UpdateRequest, _ ...grpc.CallOption) (*tasksv1.UpdateResponse, error) {
	ctx, span := tracer.Start(ctx, "task.Update")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	updateTask := req.GetTask()

	// Get the existing task before updating so we can compare specs
	existingTask, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Ignore fields
	updateTask.Status = existingTask.Status
	updateTask.GetMeta().ResourceVersion++
	updateTask.GetMeta().Updated = existingTask.Meta.Updated
	updateTask.GetMeta().Created = existingTask.Meta.Created

	updVal := protoreflect.ValueOfMessage(updateTask.GetConfig().ProtoReflect())
	newVal := protoreflect.ValueOfMessage(existingTask.GetConfig().ProtoReflect())

	// Only update metadata fields if spec is updated
	if !updVal.Equal(newVal) {
		updateTask.Meta.Generation++
		updateTask.Meta.Updated = timestamppb.Now()
	}

	// Update the task
	err = l.Repo().Update(ctx, updateTask)
	if err != nil {
		return nil, l.handleError(err, "couldn't UPDATE task in repo", "name", updateTask.GetMeta().GetName())
	}

	// Retreive the task again so that we can include it in an event
	task, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Only publish if spec is updated
	if !updVal.Equal(newVal) {

		// Decorate label with some labels
		eventLabels := labels.New()
		eventLabels.Set(labels.LabelPrefix("object-id").String(), task.GetMeta().GetName())
		eventLabels.Set(labels.LabelPrefix("object-version").String(), task.GetVersion())

		l.logger.Debug("task was updated, emitting event to listeners", "event", "TaskUpdate", "name", task.GetMeta().GetName(), "revision", updateTask.GetMeta().GetGeneration())
		err = l.exchange.Forward(ctx, events.NewEvent(events.TaskUpdate, task, eventLabels))
		if err != nil {
			return nil, l.handleError(err, "error publishing UPDATE event", "name", task.GetMeta().GetName(), "event", "TaskUpdate")
		}
	}

	return &tasksv1.UpdateResponse{
		Task: task,
	}, nil
}

// Condition implements [tasks.TaskServiceClient].
func (l *local) Condition(ctx context.Context, req *types.ConditionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	// l.mu.Lock()
	// defer l.mu.Unlock()
	//
	// currentTask, err := l.Get(ctx, &tasksv1.GetRequest{Id: req.GetReport().GetResourceId()})
	// if err != nil {
	// 	return nil, l.handleError(err, "error getting task")
	// }
	//
	// if currentTask.GetTask().GetMeta().GetResourceVersion() > uint64(req.GetReport().GetObservedResourceVersion())+1 {
	// 	return nil, status.Error(codes.FailedPrecondition, "observed resource version is too old")
	// }

	err := l.exchange.Publish(ctx, events.NewEvent(events.ConditionReported, req))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (l *local) Repo() repository.TaskRepository {
	if l.repo != nil {
		return l.repo
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return repository.NewTaskInMemRepo()
}
