package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/amimof/voiyd/pkg/events"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/pkg/repository"
	"github.com/google/uuid"

	leasesv1 "github.com/amimof/voiyd/api/services/leases/v1"
	"github.com/amimof/voiyd/api/types/v1"
)

type local struct {
	repo        repository.LeaseRepository
	mu          sync.Mutex
	exchange    *events.Exchange
	logger      logger.Logger
	leaseTTL    uint32
	gracePeriod time.Duration
}

var (
	_      leasesv1.LeaseServiceClient = &local{}
	tracer                             = otel.GetTracerProvider().Tracer("voiyd-server")
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

func (l *local) Get(ctx context.Context, req *leasesv1.GetRequest, _ ...grpc.CallOption) (*leasesv1.GetResponse, error) {
	ctx, span := tracer.Start(ctx, "lease.Get")
	defer span.End()

	lease, err := l.repo.Get(ctx, req.GetId())
	if err != nil {
		return nil, l.handleError(err, "couldn't GET lease from repo", "name", req.GetId())
	}
	return &leasesv1.GetResponse{
		Lease: lease,
	}, nil
}

func (l *local) List(ctx context.Context, req *leasesv1.ListRequest, _ ...grpc.CallOption) (*leasesv1.ListResponse, error) {
	ctx, span := tracer.Start(ctx, "lease.List")
	defer span.End()

	ctrs, err := l.repo.List(ctx)
	if err != nil {
		return nil, l.handleError(err, "couldn't LIST leases from repo")
	}
	return &leasesv1.ListResponse{
		Leases: ctrs,
	}, nil
}

func (l *local) Acquire(ctx context.Context, req *leasesv1.AcquireRequest, _ ...grpc.CallOption) (*leasesv1.AcquireResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if lease already exists
	existing, err := l.repo.Get(ctx, req.GetTaskId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {

			ttl := l.leaseTTL
			now := time.Now()
			expires := now.Add(time.Duration(ttl) * time.Second)

			// Create new lease
			lease := &leasesv1.Lease{
				Version: Version,
				Meta: &types.Meta{
					Name:            uuid.New().String(),
					Created:         timestamppb.Now(),
					Updated:         timestamppb.Now(),
					ResourceVersion: 1,
					Generation:      1,
				},
				Config: &leasesv1.LeaseConfig{
					TaskId:     req.TaskId,
					NodeId:     req.NodeId,
					AcquiredAt: timestamppb.New(now),
					RenewTime:  timestamppb.New(now),
					ExpiresAt:  timestamppb.New(expires),
					TtlSeconds: ttl,
				},
			}
			err = l.repo.Create(ctx, lease)
			if err != nil {
				return nil, l.handleError(err, "error creating lease", "name", req.GetTaskId())
			}

			err = l.exchange.Publish(ctx, events.NewEvent(events.LeaseAcquiered, lease))
			if err != nil {
				return nil, l.handleError(err, "error publishing lease acquire event", "leaseID", lease.GetMeta().GetName(), "task", lease.GetConfig().GetTaskId(), "node", lease.GetConfig().GetNodeId())
			}

			return &leasesv1.AcquireResponse{Acquired: true, Lease: lease}, nil
		}
		return nil, l.handleError(err, "error getting lease", "name", req.TaskId)
	}

	// Node is same as before
	if existing.GetConfig().GetNodeId() == req.GetNodeId() {
		lease, err := l.renew(ctx, existing.GetConfig().GetTaskId(), req.GetNodeId())
		if err != nil {
			return nil, err
		}
		return &leasesv1.AcquireResponse{
			Lease:    lease,
			Holder:   existing.GetConfig().GetNodeId(),
			Acquired: true,
		}, nil
	}

	// Different node - check if current lease expired + grace period
	if time.Now().After(existing.GetConfig().GetExpiresAt().AsTime().Add(l.gracePeriod)) {
		lease, err := l.renew(ctx, existing.GetConfig().GetTaskId(), req.GetNodeId())
		if err != nil {
			return nil, err
		}
		return &leasesv1.AcquireResponse{
			Lease:    lease,
			Holder:   req.GetNodeId(),
			Acquired: true,
		}, nil
	}

	return nil, nil
}

func (l *local) Release(ctx context.Context, req *leasesv1.ReleaseRequest, _ ...grpc.CallOption) (*leasesv1.ReleaseResponse, error) {
	lease, err := l.repo.Get(ctx, req.TaskId)
	if err != nil {
		return &leasesv1.ReleaseResponse{Released: false}, l.handleError(err, "error getting lease")
	}

	// Decline release request if nodeID does match current lease holder
	if lease.GetConfig().GetNodeId() != req.GetNodeId() {
		return nil, status.Error(codes.InvalidArgument, "cannot release lease on behalf of another lease holder")
	}

	err = l.repo.Delete(ctx, req.TaskId)
	if err != nil {
		return nil, l.handleError(err, "error releasing lease", "lease", lease.GetMeta().GetName())
	}

	err = l.exchange.Publish(ctx, events.NewEvent(events.LeaseReleased, lease))
	if err != nil {
		return nil, l.handleError(err, "error publishing lease released event", "error", err, "leaseID", lease.GetMeta().GetName(), "task", lease.GetConfig().GetTaskId(), "node", lease.GetConfig().GetNodeId())
	}

	return &leasesv1.ReleaseResponse{Released: true}, err
}

func (l *local) Renew(ctx context.Context, req *leasesv1.RenewRequest, _ ...grpc.CallOption) (*leasesv1.RenewResponse, error) {
	lease, err := l.renew(ctx, req.GetTaskId(), req.GetNodeId())
	if err != nil {
		return &leasesv1.RenewResponse{Renewed: false}, err
	}

	err = l.exchange.Publish(ctx, events.NewEvent(events.LeaseRenewed, lease))
	if err != nil {
		return nil, l.handleError(err, "error publishing lease released event", "error", err, "leaseID", lease.GetMeta().GetName(), "task", lease.GetConfig().GetTaskId(), "node", lease.GetConfig().GetNodeId())
	}

	return &leasesv1.RenewResponse{Renewed: true, Lease: lease}, nil
}

func (l *local) renew(ctx context.Context, taskID, nodeID string) (*leasesv1.Lease, error) {
	existing, err := l.repo.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	// Renew if lease has a holder which has already expired
	if existing.GetConfig().GetNodeId() != nodeID {
		if time.Now().Before(existing.GetConfig().GetExpiresAt().AsTime().Add(l.gracePeriod)) {
			return nil, fmt.Errorf("lease held by %s", existing.GetConfig().GetNodeId())
		}
	}

	// Update expiry
	existing.GetConfig().RenewTime = timestamppb.Now()
	existing.GetConfig().ExpiresAt = timestamppb.New(time.Now().Add(time.Duration(existing.GetConfig().GetTtlSeconds()) * time.Second))
	existing.GetConfig().NodeId = nodeID
	existing.GetMeta().ResourceVersion++
	existing.GetMeta().Generation++

	err = l.repo.Update(ctx, existing)
	if err != nil {
		return nil, err
	}

	return existing, nil
}
