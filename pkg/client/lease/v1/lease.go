package v1

import (
	"context"

	leasesv1 "github.com/amimof/voiyd/api/services/leases/v1"
	"github.com/amimof/voiyd/pkg/labels"
	"github.com/amimof/voiyd/pkg/util"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var _ ClientV1 = &clientV1{}

type CreateOption func(c *clientV1)

func WithEmitLabels(l labels.Label) CreateOption {
	return func(c *clientV1) {
		c.emitLabels = l
	}
}

func WithClient(client leasesv1.LeaseServiceClient) CreateOption {
	return func(c *clientV1) {
		c.Client = client
	}
}

type ClientV1 interface {
	Acquire(context.Context, string, string, ...CreateOption) (uint32, bool, error)
	Renew(context.Context, string, string) (bool, error)
	Release(context.Context, string, string) error
	Get(context.Context, string) (*leasesv1.Lease, error)
	List(context.Context, ...labels.Label) ([]*leasesv1.Lease, error)
}

type clientV1 struct {
	Client     leasesv1.LeaseServiceClient
	emitLabels labels.Label
	id         string
}

func (c *clientV1) Acquire(ctx context.Context, taskID, nodeID string, opts ...CreateOption) (uint32, bool, error) {
	tracer := otel.Tracer("client-v1")
	ctx, span := tracer.Start(ctx, "client.lease.Acquire")
	defer span.End()

	for _, opt := range opts {
		opt(c)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "voiyd_client_id", c.id)
	resp, err := c.Client.Acquire(ctx, &leasesv1.AcquireRequest{NodeId: nodeID, TaskId: taskID})
	if err != nil {
		return 0, false, err
	}
	if !resp.Acquired {
		return 0, false, err
	}
	return resp.GetLease().GetConfig().GetTtlSeconds(), true, nil
}

func (c *clientV1) Renew(ctx context.Context, taskID, nodeID string) (bool, error) {
	tracer := otel.Tracer("client-v1")
	ctx, span := tracer.Start(ctx, "client.lease.Renew")
	defer span.End()

	ctx = metadata.AppendToOutgoingContext(ctx, "voiyd_client_id", c.id)
	_, err := c.Client.Renew(ctx, &leasesv1.RenewRequest{TaskId: taskID, NodeId: nodeID})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *clientV1) Release(ctx context.Context, taskID, nodeID string) error {
	tracer := otel.Tracer("client-v1")
	ctx, span := tracer.Start(ctx, "client.lease.Release")
	defer span.End()

	ctx = metadata.AppendToOutgoingContext(ctx, "voiyd_client_id", c.id)
	_, err := c.Client.Release(ctx, &leasesv1.ReleaseRequest{TaskId: taskID, NodeId: nodeID})
	if err != nil {
		return err
	}
	return nil
}

func (c *clientV1) Get(ctx context.Context, id string) (*leasesv1.Lease, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "voiyd_client_id", c.id)

	tracer := otel.Tracer("client-v1")
	ctx, span := tracer.Start(ctx, "client.lease.Get")
	defer span.End()

	res, err := c.Client.Get(ctx, &leasesv1.GetRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return res.GetLease(), nil
}

func (c *clientV1) List(ctx context.Context, l ...labels.Label) ([]*leasesv1.Lease, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "voiyd_client_id", c.id)

	tracer := otel.Tracer("client-v1")
	ctx, span := tracer.Start(ctx, "client.lease.List")
	defer span.End()

	mergedLabels := util.MergeLabels(l...)
	res, err := c.Client.List(ctx, &leasesv1.ListRequest{Selector: mergedLabels})
	if err != nil {
		return nil, err
	}
	return res.Leases, nil
}

func NewClientV1(opts ...CreateOption) ClientV1 {
	c := &clientV1{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func NewClientV1WithConn(conn *grpc.ClientConn, clientID string, opts ...CreateOption) ClientV1 {
	c := &clientV1{
		Client: leasesv1.NewLeaseServiceClient(conn),
		id:     clientID,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}
