package node

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
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
	"github.com/google/uuid"

	nodesv1 "github.com/amimof/voiyd/api/services/nodes/v1"
	typesv1 "github.com/amimof/voiyd/api/types/v1"
)

type local struct {
	repo     repository.NodeRepository
	mu       sync.Mutex
	exchange *events.Exchange
	logger   logger.Logger
}

var (
	_      nodesv1.NodeServiceClient = &local{}
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

func merge(base, patch *nodesv1.Node) *nodesv1.Node {
	return protoutils.StrategicMerge(base, patch)
}

func applyMaskedUpdate(dst, src *nodesv1.Status, mask *fieldmaskpb.FieldMask) error {
	if mask == nil || len(mask.Paths) == 0 {
		return status.Error(codes.InvalidArgument, "update_mask is required")
	}
	if dst == nil || src == nil {
		return status.Error(codes.InvalidArgument, "src or dst cannot be empty")
	}

	for _, p := range mask.Paths {
		switch p {
		case "phase":
			if src.Phase == nil {
				continue
			}
			dst.Phase = src.Phase
		case "status":
			if src.Status == nil {
				continue
			}
			dst.Status = src.Status
		case "hostname":
			if src.Hostname == nil {
				continue
			}
			dst.Hostname = src.Hostname
		case "runtime":
			if src.Runtime == nil {
				continue
			}
			dst.Runtime = src.Runtime
		case "version":
			if src.Version == nil {
				continue
			}
			dst.Version = src.Version
		case "conditions":
			if src.Conditions == nil {
				continue
			}
			dst.Conditions = src.Conditions
		case "ip.dns":
			if src.Ip.Dns == nil {
				continue
			}
			dst.Ip.Dns = src.Ip.Dns
		case "ip.links":
			if src.Ip.Links == nil {
				continue
			}
			dst.Ip.Links = src.Ip.Links
		case "ip.addresses":
			if src.Ip.Addresses == nil {
				continue
			}
			dst.Ip.Addresses = src.Ip.Addresses
		default:
			return fmt.Errorf("unknown mask path %q", p)
		}
	}

	return nil
}

func (l *local) Get(ctx context.Context, req *nodesv1.GetRequest, _ ...grpc.CallOption) (*nodesv1.GetResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Get")
	defer span.End()

	node, err := l.Repo().Get(ctx, req.Id)
	if err != nil {
		return nil, l.handleError(err, "couldn't GET node from repo", "name", req.GetId())
	}
	return &nodesv1.GetResponse{
		Node: node,
	}, nil
}

func (l *local) Create(ctx context.Context, req *nodesv1.CreateRequest, _ ...grpc.CallOption) (*nodesv1.CreateResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Create")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	node := req.GetNode()
	nodeID := node.GetMeta().GetName()

	if existing, _ := l.Repo().Get(ctx, node.GetMeta().GetName()); existing != nil {
		return nil, status.Error(codes.AlreadyExists, "node already exists")
	}

	node.Meta.Created = timestamppb.Now()
	node.GetMeta().Updated = timestamppb.Now()
	node.GetMeta().ResourceVersion = 1
	node.GetMeta().Generation = 1
	node.GetMeta().Uid = uuid.New().String()

	// Initialize status field if empty
	if node.GetStatus() == nil {
		node.Status = &nodesv1.Status{}
	}

	err := l.Repo().Create(ctx, node)
	if err != nil {
		return nil, l.handleError(err, "couldn't CREATE node in repo", "name", nodeID)
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), node.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), node.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(events.NodeCreate, node, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing CREATE event", "name", nodeID, "event", "NodeCreate")
	}
	return &nodesv1.CreateResponse{
		Node: node,
	}, nil
}

func (l *local) Delete(ctx context.Context, req *nodesv1.DeleteRequest, _ ...grpc.CallOption) (*nodesv1.DeleteResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Delete")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	node, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	err = l.Repo().Delete(ctx, req.GetId())
	if err != nil {
		return nil, l.handleError(err, "couldn't GET node from repo", "id", req.GetId())
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), node.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), node.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(events.NodeDelete, node, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing DELETE event", "name", req.GetId(), "event", "nodeDelete")
	}
	return &nodesv1.DeleteResponse{
		Id: req.Id,
	}, nil
}

func (l *local) List(ctx context.Context, req *nodesv1.ListRequest, _ ...grpc.CallOption) (*nodesv1.ListResponse, error) {
	ctx, span := tracer.Start(ctx, "node.List")
	defer span.End()

	nodeList, err := l.Repo().List(ctx)
	if err != nil {
		return nil, err
	}
	return &nodesv1.ListResponse{
		Nodes: nodeList,
	}, nil
}

func (l *local) UpdateStatus(ctx context.Context, req *nodesv1.UpdateStatusRequest, opts ...grpc.CallOption) (*nodesv1.UpdateStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "node.UpdateStatus")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	// Get the existing node before updating so we can compare specs
	existingNode, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Apply mask safely
	base := proto.Clone(existingNode.Status).(*nodesv1.Status)
	if err := applyMaskedUpdate(base, req.Status, req.UpdateMask); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad mask: %v", err)
	}

	existingNode.GetMeta().ResourceVersion++
	existingNode.Status = base

	if err := l.Repo().Update(ctx, existingNode); err != nil {
		return nil, err
	}

	return &nodesv1.UpdateStatusResponse{
		Id: existingNode.GetMeta().GetName(),
	}, nil
}

// Patch implements nodes.NodeServiceClient.
func (l *local) Patch(ctx context.Context, req *nodesv1.PatchRequest, opts ...grpc.CallOption) (*nodesv1.PatchResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Patch")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	updateNode := req.GetNode()

	// Get existing node from repo
	existing, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, l.handleError(err, "couldn't GET node from repo", "name", updateNode.GetMeta().GetName())
	}

	// Generate field mask
	genFieldMask, err := protoutils.GenerateFieldMask(existing, updateNode)
	if err != nil {
		return nil, err
	}

	// Handle partial update
	maskedUpdate, err := protoutils.ApplyFieldMaskToNewMessage(updateNode, genFieldMask)
	if err != nil {
		return nil, err
	}

	// TODO: Handle errors
	updated := maskedUpdate.(*nodesv1.Node)
	existing = merge(existing, updated)

	// Update the node
	err = l.Repo().Update(ctx, existing)
	if err != nil {
		return nil, l.handleError(err, "couldn't PATCH node in repo", "name", existing.GetMeta().GetName())
	}

	// Retreive the node again so that we can include it in an event
	node, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	updateNode.GetMeta().ResourceVersion++
	updateNode.Status = node.Status
	updateNode.GetMeta().Updated = node.Meta.Updated
	updateNode.GetMeta().Created = node.Meta.Created

	// Only publish if spec is updated
	if !proto.Equal(updateNode, node) {
		l.logger.Debug("node was patched, emitting event to listeners", "event", "NodePatch", "name", node.GetMeta().GetName(), "revision", updateNode.GetMeta().GetGeneration())

		// Decorate label with some labels
		eventLabels := labels.New()
		eventLabels.Set(labels.LabelPrefix("object-id").String(), node.GetMeta().GetName())
		eventLabels.Set(labels.LabelPrefix("object-version").String(), node.GetVersion())

		err = l.exchange.Forward(ctx, events.NewEvent(events.NodePatch, node, eventLabels))
		if err != nil {
			return nil, l.handleError(err, "error publishing PATCH event", "name", existing.GetMeta().GetName(), "event", "NodePatch")
		}
	}

	return &nodesv1.PatchResponse{
		Node: existing,
	}, nil
}

func (l *local) Update(ctx context.Context, req *nodesv1.UpdateRequest, _ ...grpc.CallOption) (*nodesv1.UpdateResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Update")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	updateNode := req.GetNode()

	// Get the existing node before updating so we can compare specs
	existingNode, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, l.handleError(err, "couldn't GET node from repo", "name", updateNode.GetMeta().GetName())
	}

	// Ignore fields
	updateNode.GetMeta().ResourceVersion++
	updateNode.Status = existingNode.Status
	updateNode.GetMeta().Updated = existingNode.Meta.Updated
	updateNode.GetMeta().Created = existingNode.Meta.Created

	updVal := protoreflect.ValueOfMessage(updateNode.ProtoReflect())
	newVal := protoreflect.ValueOfMessage(existingNode.ProtoReflect())

	// Only update metadata fields if spec is updated
	if !updVal.Equal(newVal) {
		updateNode.Meta.Generation++
		updateNode.Meta.Updated = timestamppb.Now()
	}

	// Update the node
	err = l.Repo().Update(ctx, updateNode)
	if err != nil {
		return nil, l.handleError(err, "couldn't UPDATE node in repo", "name", updateNode.GetMeta().GetName())
	}

	// Retreive the node again so that we can include it in an event
	node, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Only publish if spec is updated
	if !updVal.Equal(newVal) {
		l.logger.Debug("node was updated, emitting event to listeners", "event", "NodeUpdate", "name", node.GetMeta().GetName(), "revision", updateNode.GetMeta().GetGeneration())

		// Decorate label with some labels
		eventLabels := labels.New()
		eventLabels.Set(labels.LabelPrefix("object-id").String(), node.GetMeta().GetName())
		eventLabels.Set(labels.LabelPrefix("object-version").String(), node.GetVersion())

		err = l.exchange.Forward(ctx, events.NewEvent(events.NodeUpdate, node, eventLabels))
		if err != nil {
			return nil, l.handleError(err, "error publishing UPDATE event", "name", node.GetMeta().GetName(), "event", "NodeUpdate")
		}
	}

	return &nodesv1.UpdateResponse{
		Node: node,
	}, nil
}

func (l *local) Join(ctx context.Context, req *nodesv1.JoinRequest, _ ...grpc.CallOption) (*nodesv1.JoinResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Join")
	defer span.End()

	nodeID := req.GetNode().GetMeta().GetName()

	node, err := l.Repo().Get(ctx, nodeID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			l.logger.Debug("creating node that joined", "nodeID", nodeID)
			if _, err := l.Create(ctx, &nodesv1.CreateRequest{Node: req.GetNode()}); err != nil {
				return nil, l.handleError(err, "couldn't CREATE node", "name", nodeID)
			}
		} else {
			return nil, l.handleError(err, "couldn't GET node", "name", nodeID)
		}
	}

	// Perform update if node exists
	if err == nil {
		l.logger.Debug("updating node that joined", "nodeID", nodeID)
		res, err := l.Update(ctx, &nodesv1.UpdateRequest{Id: nodeID, Node: req.GetNode()})
		if err != nil {
			return nil, err
		}
		if res != nil {
			node = res.GetNode()
		}
	}

	resp := &nodesv1.JoinResponse{
		Id: req.GetNode().GetMeta().GetName(),
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), node.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("object-version").String(), node.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(events.NodeJoin, node, eventLabels))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (l *local) Forget(ctx context.Context, req *nodesv1.ForgetRequest, _ ...grpc.CallOption) (*nodesv1.ForgetResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Forget")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	node, err := l.Repo().Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	_, err = l.Delete(ctx, &nodesv1.DeleteRequest{Id: req.GetId()})
	if err != nil {
		return nil, l.handleError(err, "couldn't FORGET node", "name", req.GetId())
	}

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), node.GetMeta().GetName())
	eventLabels.Set(labels.LabelPrefix("target-version").String(), node.GetVersion())

	err = l.exchange.Forward(ctx, events.NewEvent(events.NodeForget, node, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing FORGET event", "name", req.GetId(), "event", "NodeForget")
	}
	return &nodesv1.ForgetResponse{
		Id: req.Id,
	}, nil
}

func (l *local) Connect(ctx context.Context, opt ...grpc.CallOption) (nodesv1.NodeService_ConnectClient, error) {
	return nil, nil
}

func (l *local) Upgrade(ctx context.Context, req *nodesv1.UpgradeRequest, _ ...grpc.CallOption) (*nodesv1.UpgradeResponse, error) {
	ctx, span := tracer.Start(ctx, "node.Upgrade")
	defer span.End()

	l.mu.Lock()
	defer l.mu.Unlock()

	// Decorate label with some labels
	eventLabels := labels.New()
	eventLabels.Set(labels.LabelPrefix("object-id").String(), req.GetNodeId())
	eventLabels.Set(labels.LabelPrefix("target-version").String(), req.GetTargetVersion())

	err := l.exchange.Forward(ctx, events.NewEvent(events.NodeUpgrade, req, eventLabels))
	if err != nil {
		return nil, l.handleError(err, "error publishing UPGRADE event", "name", req.GetNodeSelector(), "event", "NodeUpgrade")
	}

	return &nodesv1.UpgradeResponse{}, nil
}

func (l *local) Condition(ctx context.Context, req *typesv1.ConditionRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	err := l.exchange.Publish(ctx, events.NewEvent(events.ConditionReported, req))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (l *local) Repo() repository.NodeRepository {
	if l.repo != nil {
		return l.repo
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return repository.NewNodeInMemRepo()
}
