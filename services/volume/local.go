package volume

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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/amimof/voiyd/api/services/volumes/v1"
	"github.com/amimof/voiyd/pkg/events"
	"github.com/amimof/voiyd/pkg/labels"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/pkg/repository"
)

var (
	_      volumes.VolumeServiceClient = &local{}
	tracer                             = otel.GetTracerProvider().Tracer("volume-service")
)

type local struct {
	repo     repository.VolumeRepository
	mu       sync.Mutex
	exchange *events.Exchange
	logger   logger.Logger
}

func (l *local) handleError(err error, msg string, keysAndValues ...any) error {
	def := []any{"error", err.Error()}
	def = append(def, keysAndValues...)
	l.logger.Error(msg, def...)
	if errors.Is(err, repository.ErrNotFound) {
		return status.Error(codes.NotFound, fmt.Sprintf("%s: %v", msg, err.Error()))
	}
	return status.Error(codes.Internal, fmt.Sprintf("%s: %v", msg, err.Error()))
}

func applyMaskedUpdate(dst, src *volumes.Status, mask *fieldmaskpb.FieldMask) error {
	if mask == nil || len(mask.Paths) == 0 {
		return status.Error(codes.InvalidArgument, "update_mask is required")
	}

	for _, p := range mask.Paths {
		switch p {
		case "controllers":
			if src.Controllers == nil {
				continue
			}
			dst.Controllers = src.Controllers
		default:
			return fmt.Errorf("unknown mask path %q", p)
		}
	}

	return nil
}

// Patch implements volumes.VolumeServiceClient.
func (l *local) Patch(ctx context.Context, in *volumes.PatchRequest, opts ...grpc.CallOption) (*volumes.PatchResponse, error) {
	panic("unimplemented")
}

// Create implements volumes.VolumeServiceClient.
func (l *local) Create(ctx context.Context, req *volumes.CreateRequest, opts ...grpc.CallOption) (*volumes.CreateResponse, error) {
	ctx, span := tracer.Start(ctx, "volume.Create")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	volume := req.GetVolume()
	volumeID := volume.GetMeta().GetName()

	// Check if volume already exists
	if existing, _ := l.Get(ctx, &volumes.GetRequest{Id: volumeID}); existing != nil {
		return nil, fmt.Errorf("volume %s already exists", volume.GetMeta().GetName())
	}

	volume.GetMeta().Created = timestamppb.Now()
	volume.GetMeta().Updated = timestamppb.Now()
	volume.GetMeta().ResourceVersion = 1
	volume.GetMeta().Generation = 1

	// Initialize status field if empty
	if volume.GetStatus() == nil {
		volume.Status = &volumes.Status{}
	}

	// Create volume in repo
	err := l.Repo().Create(ctx, volume)
	if err != nil {
		return nil, l.handleError(err, "couldn't CREATE volume in repo", "name", volumeID)
	}

	// Get the created volume from repo
	volume, err = l.Repo().Get(ctx, volumeID)
	if err != nil {
		return nil, err
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), volume.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), volume.GetVersion())

	// Publish event that volume is created
	err = l.exchange.Forward(ctx, events.NewEvent(events.VolumeCreate, volume, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing CREATE event", "name", volume.GetMeta().GetName(), "event", "VolumeCreate")
	}

	return &volumes.CreateResponse{
		Volume: volume,
	}, nil
}

// Delete implements volumes.VolumeServiceClient.
func (l *local) Delete(ctx context.Context, req *volumes.DeleteRequest, opts ...grpc.CallOption) (*volumes.DeleteResponse, error) {
	ctx, span := tracer.Start(ctx, "volume.Delete")
	defer span.End()

	volume, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, l.handleError(err, "couldn't GET volume from repo", "id", req.GetId())
	}
	err = l.Repo().Delete(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), volume.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), volume.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(events.VolumeDelete, volume, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing DELETE event", "name", volume.GetMeta().GetName(), "event", "VolumeDelete")
	}
	return &volumes.DeleteResponse{
		Id: req.GetId(),
	}, nil
}

// Get implements volumes.VolumeServiceClient.
func (l *local) Get(ctx context.Context, req *volumes.GetRequest, opts ...grpc.CallOption) (*volumes.GetResponse, error) {
	ctx, span := tracer.Start(ctx, "volume.Get", trace.WithSpanKind(trace.SpanKindServer))
	span.SetAttributes(
		attribute.String("service", "Volume"),
		attribute.String("volume.id", req.GetId()),
	)
	defer span.End()

	// Get volume from repo
	volume, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		span.RecordError(err)
		return nil, l.handleError(err, "couldn't GET volume from repo", "name", req.GetId())
	}

	span.SetAttributes(attribute.String("volume.name", volume.GetMeta().GetName()))

	return &volumes.GetResponse{
		Volume: volume,
	}, nil
}

// List implements volumes.VolumeServiceClient.
func (l *local) List(ctx context.Context, req *volumes.ListRequest, opts ...grpc.CallOption) (*volumes.ListResponse, error) {
	ctx, span := tracer.Start(ctx, "volume.List")
	defer span.End()

	// Validate request

	// Get volumes from repo
	ctrs, err := l.Repo().List(ctx, req.GetSelector())
	if err != nil {
		return nil, l.handleError(err, "couldn't LIST volumes from repo")
	}
	return &volumes.ListResponse{
		Volumes: ctrs,
	}, nil
}

// Update implements volumes.VolumeServiceClient.
func (l *local) UpdateStatus(ctx context.Context, req *volumes.UpdateStatusRequest, opts ...grpc.CallOption) (*volumes.UpdateStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "volume.UpdateStatus")
	defer span.End()

	// Get the existing container before updating so we can compare specs
	existingVolume, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Apply mask safely
	base := proto.Clone(existingVolume.GetStatus()).(*volumes.Status)
	if err := applyMaskedUpdate(base, req.Status, req.UpdateMask); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad mask: %v", err)
	}

	existingVolume.GetMeta().ResourceVersion++
	existingVolume.Status = base

	if err := l.Repo().Update(ctx, existingVolume); err != nil {
		return nil, err
	}

	return &volumes.UpdateStatusResponse{
		Id: existingVolume.GetMeta().GetName(),
	}, nil
}

// UpdateStatus implements volumes.VolumeServiceClient.
func (l *local) Update(ctx context.Context, req *volumes.UpdateRequest, opts ...grpc.CallOption) (*volumes.UpdateResponse, error) {
	ctx, span := tracer.Start(ctx, "volume.Update")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	// Validate request
	updateVolume := req.GetVolume()

	// Get the existing volume before updating so we can compare specs
	existingVolume, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Ignore fields
	updateVolume.GetMeta().ResourceVersion++
	updateVolume.Status = existingVolume.Status
	updateVolume.GetMeta().Updated = existingVolume.Meta.Updated
	updateVolume.GetMeta().Created = existingVolume.Meta.Created
	updateVolume.GetMeta().ResourceVersion = existingVolume.Meta.ResourceVersion

	updVal := protoreflect.ValueOfMessage(updateVolume.GetConfig().ProtoReflect())
	newVal := protoreflect.ValueOfMessage(existingVolume.GetConfig().ProtoReflect())

	// Only update metadata fields if spec is updated
	if !updVal.Equal(newVal) {
		updateVolume.Meta.Generation++
		updateVolume.Meta.Updated = timestamppb.Now()
	}

	// Update the volume
	err = l.Repo().Update(ctx, updateVolume)
	if err != nil {
		return nil, l.handleError(err, "couldn't UPDATE volume in repo", "name", updateVolume.GetMeta().GetName())
	}

	// Retreive the volume again so that we can include it in an event
	volume, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Only publish if spec is updated
	if !updVal.Equal(newVal) {

		// Decorate label with some labels
		eventLabels := labels.New()
		eventLabels.Set(labels.LabelPrefix("object-id").String(), volume.GetMeta().GetName())
		eventLabels.Set(labels.LabelPrefix("object-version").String(), volume.GetVersion())

		l.logger.Debug("volume was updated, emitting event to listeners", "event", "VolumeUpdate", "name", volume.GetMeta().GetName(), "revision", updateVolume.GetMeta().GetGeneration())
		err = l.exchange.Forward(ctx, events.NewEvent(events.VolumeUpdate, volume, eventLabels))
		if err != nil {
			return nil, l.handleError(err, "error publishing UPDATE event", "name", volume.GetMeta().GetName(), "event", "VolumeUpdate")
		}
	}

	return &volumes.UpdateResponse{
		Volume: volume,
	}, nil
}

func (l *local) Repo() repository.VolumeRepository {
	if l.repo != nil {
		return l.repo
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return repository.NewVolumeInMemRepo()
}
