// Package event implements the event service
package event

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/amimof/voiyd/pkg/events"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/pkg/repository"

	eventsv1 "github.com/amimof/voiyd/api/services/events/v1"
)

const Version string = "event/v1"

var ErrClientExists = errors.New("client already exists")

type NewServiceOption func(s *EventService)

func WithLogger(l logger.Logger) NewServiceOption {
	return func(s *EventService) {
		s.logger = l
	}
}

func WithExchange(e *events.Exchange) NewServiceOption {
	return func(s *EventService) {
		s.exchange = e
		s.exchange.AddForwarder(s)
	}
}

type EventService struct {
	eventsv1.UnimplementedEventServiceServer
	local    eventsv1.EventServiceClient
	logger   logger.Logger
	exchange *events.Exchange
}

func (n *EventService) Create(ctx context.Context, req *eventsv1.CreateRequest) (*eventsv1.CreateResponse, error) {
	return n.local.Create(ctx, req)
}

func (n *EventService) Get(ctx context.Context, req *eventsv1.GetRequest) (*eventsv1.GetResponse, error) {
	return n.local.Get(ctx, req)
}

func (n *EventService) Delete(ctx context.Context, req *eventsv1.DeleteRequest) (*eventsv1.DeleteResponse, error) {
	return n.local.Delete(ctx, req)
}

func (n *EventService) List(ctx context.Context, req *eventsv1.ListRequest) (*eventsv1.ListResponse, error) {
	return n.local.List(ctx, req)
}

func (n *EventService) Register(server *grpc.Server) error {
	server.RegisterService(&eventsv1.EventService_ServiceDesc, n)
	// eventsv1.RegisterEventServiceServer(server, n)
	return nil
}

func (s *EventService) Subscribe(req *eventsv1.SubscribeRequest, stream eventsv1.EventService_SubscribeServer) error {
	// Identify the client
	ctx := stream.Context()

	ctx, span := tracer.Start(ctx, "service.event.Subscribe")
	defer span.End()

	clientID := req.ClientId
	peer, _ := peer.FromContext(ctx)

	eventChan := s.exchange.Subscribe(ctx, events.ALL...)

	md, _ := metadata.FromIncomingContext(ctx)

	span.SetAttributes(
		attribute.String("client.id", clientID),
		attribute.String("peer.addr", peer.Addr.String()),
	)

	s.logger.Debug("client connected", "clientId", clientID, "address", peer.Addr.String(), "controller", md.Get("voiyd_controller_name"))

	go func() {
		for {
			select {
			case n := <-eventChan:

				err := stream.Send(n)
				if err != nil {
					s.logger.Error("unable to emit event to clients", "error", err, "eventType", n.GetType().String(), "objectId", n.GetObjectId(), "eventId", n.GetMeta().GetName(), "clientId", req.ClientId)
					return
				}
			case <-ctx.Done():

				s.logger.Info("node stream context cancelled", "reason", ctx.Err())

				// Get node name from context
				if md, ok := metadata.FromIncomingContext(ctx); ok {
					if nodeName, ok := md["voiyd_node_name"]; ok && len(nodeName) > 0 {
						_, err := s.Publish(ctx, &eventsv1.PublishRequest{Event: &eventsv1.Event{ObjectId: nodeName[0], Type: eventsv1.EventType_NodeForget}})
						if err != nil {
							s.logger.Error("error publishing event", "error", err)
						}
					}
				}
				return
			}
		}
	}()

	<-ctx.Done()
	return nil
}

func (s *EventService) Forward(ctx context.Context, event *eventsv1.Event) error {
	ev := event

	if ev.GetMeta().GetName() == "" {
		ev.GetMeta().Name = uuid.New().String()
	}
	ev.GetMeta().Created = timestamppb.Now()
	ev.GetMeta().Updated = timestamppb.Now()
	ev.GetMeta().ResourceVersion = 1

	_, err := s.local.Create(ctx, &eventsv1.CreateRequest{Event: ev})
	if err != nil {
		return err
	}

	s.logger.Debug("forwarded published event", "event", ev.GetType().String(), "objectID", ev.GetClientId(), "clientID", ev.GetClientId())
	return nil
}

func (s *EventService) Publish(ctx context.Context, req *eventsv1.PublishRequest) (*eventsv1.PublishResponse, error) {
	ev := req.GetEvent()

	if ev.GetMeta().GetName() == "" {
		ev.GetMeta().Name = uuid.New().String()
	}
	ev.GetMeta().Created = timestamppb.Now()
	ev.GetMeta().Updated = timestamppb.Now()
	ev.GetMeta().ResourceVersion = 1

	err := s.exchange.Publish(ctx, ev)
	if err != nil {
		return nil, err
	}

	res, err := s.local.Create(ctx, &eventsv1.CreateRequest{Event: ev})
	if err != nil {
		return nil, err
	}

	s.logger.Debug("published event", "event", ev.GetType().String(), "objectID", ev.GetClientId(), "clientID", ev.GetClientId())
	return &eventsv1.PublishResponse{Event: res.GetEvent()}, nil
}

func NewService(repo repository.EventRepository, opts ...NewServiceOption) *EventService {
	s := &EventService{
		logger: logger.ConsoleLogger{},
	}
	for _, opt := range opts {
		opt(s)
	}

	s.local = &local{
		repo: repo,
	}
	return s
}
