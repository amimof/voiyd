package task

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/amimof/voiyd/api/types/v1"
	"github.com/amimof/voiyd/pkg/events"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/pkg/repository"

	tasksv1 "github.com/amimof/voiyd/api/services/tasks/v1"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func initTestServer() (*grpc.Server, *bufconn.Listener) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	svc := NewService(repository.NewTaskInMemRepo(), WithExchange(events.NewExchange()), WithLogger(&logger.DevNullLogger{}))
	tasksv1.RegisterTaskServiceServer(s, svc)
	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()
	return s, lis
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func initDB(ctx context.Context, c tasksv1.TaskServiceClient) error {
	baseTask := tasksv1.Task{
		Meta: &types.Meta{
			Name: "test-task",
			Labels: map[string]string{
				"team":        "backend",
				"environment": "production",
				"role":        "root",
			},
		},
		Config: &tasksv1.Config{
			Image: "docker.io/library/nginx:latest",
			PortMappings: []*tasksv1.PortMapping{
				{
					Name:       "http",
					HostPort:   8080,
					TargetPort: 80,
					Protocol:   "TCP",
				},
			},
			Envvars: []*tasksv1.EnvVar{
				{
					Name:  "HTTP_PROXY",
					Value: "proxy.foo.com",
				},
			},
			Args: []string{
				"--config /mnt/cfg/config.yaml",
			},
			Mounts: []*tasksv1.Mount{
				{
					Name:        "temp",
					Source:      "/tmp",
					Destination: "/mnt/tmp",
					Type:        "bind",
				},
			},
			NodeSelector: map[string]string{
				"voiyd.io/arch": "amd64",
				"voiyd.io/os":   "linux",
			},
		},
	}

	// Create 5 copies of baseTask but with different names
	for i := 1; i < 10; i++ {
		task := &baseTask
		task.Meta.Name = fmt.Sprintf("test-task-%d", i)
		_, err := c.Create(ctx, &tasksv1.CreateRequest{Task: task})
		if err != nil {
			return err
		}
	}

	bareBoneTask := &tasksv1.Task{
		Meta: &types.Meta{
			Name: "bare-task",
		},
		Config: &tasksv1.Config{
			Image: "docker.io/library/nginx:latest",
		},
	}

	// Create bare bone task so test nilness
	_, err := c.Create(ctx, &tasksv1.CreateRequest{Task: bareBoneTask})
	if err != nil {
		return err
	}

	return nil
}

func Test_TaskService_Status(t *testing.T) {
	server, _ := initTestServer()
	defer server.Stop()

	ctx := context.Background()

	//nolint:staticcheck
	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	}()

	client := tasksv1.NewTaskServiceClient(conn)
	if err := initDB(ctx, client); err != nil {
		log.Fatalf("error initializing DB: %v", err)
	}

	testCases := []struct {
		name   string
		expect *tasksv1.Status
		patch  *tasksv1.Status
		taskID string
		mask   string
	}{
		{
			name:   "should be equal",
			taskID: "test-task-1",
			mask:   "phase",
			patch: &tasksv1.Status{
				Phase: wrapperspb.String("creating"),
			},
			expect: &tasksv1.Status{
				Phase: wrapperspb.String("creating"),
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req := &tasksv1.UpdateStatusRequest{Id: tt.taskID, Status: tt.patch, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{tt.mask}}}
			_, err := client.UpdateStatus(ctx, req)
			if err != nil {
				t.Fatal("error updating task", err)
			}

			expectedVal := protoreflect.ValueOfMessage(tt.expect.ProtoReflect())

			updated, err := client.Get(ctx, &tasksv1.GetRequest{Id: tt.taskID})
			if err != nil {
				t.Fatal("error getting task", err)
			}

			resultVal := protoreflect.ValueOfMessage(updated.GetTask().GetStatus().ProtoReflect())

			if !resultVal.Equal(expectedVal) {
				t.Errorf("\ngot:\n%v\nwant:\n%v", resultVal, expectedVal)
			}
		})
	}
}

func Test_TaskService_Equal(t *testing.T) {
	server, _ := initTestServer()
	defer server.Stop()

	ctx := context.Background()

	//nolint:staticcheck
	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	}()

	client := tasksv1.NewTaskServiceClient(conn)
	if err := initDB(ctx, client); err != nil {
		log.Fatalf("error initializing DB: %v", err)
	}

	testCases := []struct {
		name   string
		expect *tasksv1.Task
		patch  *tasksv1.Task
	}{
		{
			name: "should be equal",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-1",
				},
				Config: &tasksv1.Config{
					// Updates image tag
					Image: "docker.io/library/nginx:v1.27.3",
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:v1.27.3",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req := &tasksv1.PatchRequest{Id: tt.patch.Meta.Name, Task: tt.patch}
			res, err := client.Patch(ctx, req)
			if err != nil {
				t.Fatal("error updating task", err)
			}

			expectedVal := protoreflect.ValueOfMessage(tt.expect.GetConfig().ProtoReflect())
			resultVal := protoreflect.ValueOfMessage(res.GetTask().GetConfig().ProtoReflect())

			if !resultVal.Equal(expectedVal) {
				t.Errorf("\ngot:\n%v\nwant:\n%v", resultVal, expectedVal)
			}
		})
	}
}

func Test_TaskService_Patch(t *testing.T) {
	server, _ := initTestServer()
	defer server.Stop()

	ctx := context.Background()

	//nolint:staticcheck
	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	}()

	client := tasksv1.NewTaskServiceClient(conn)
	if err := initDB(ctx, client); err != nil {
		log.Fatalf("error initializing DB: %v", err)
	}

	testCases := []struct {
		name   string
		expect *tasksv1.Task
		patch  *tasksv1.Task
	}{
		{
			name: "should only update task image",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-1",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:v1.27.2",
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-1",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:v1.27.2",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{},
			},
		},
		{
			name: "should add to environment variables",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-2",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTPS_PROXY",
							Value: "proxy.a.b:8443",
						},
						{
							Name:  "HTTP_PROXY",
							Value: "new-proxy.foo.com:8080",
						},
					},
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-2",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "new-proxy.foo.com:8080",
						},
						{
							Name:  "HTTPS_PROXY",
							Value: "proxy.a.b:8443",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{},
			},
		},
		{
			name: "should replace volume mounts",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-3",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/var/lib/nginx/config",
							Destination: "/etc/nginx/config.d",
							Type:        "bind",
							Options:     []string{"rbind", "rw"},
						},
					},
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-3",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/var/lib/nginx/config",
							Destination: "/etc/nginx/config.d",
							Type:        "bind",
							Options:     []string{"rbind", "rw"},
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{},
			},
		},
		{
			name: "should add label to node selector",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-4",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					NodeSelector: map[string]string{
						"voiyd.io/unschedulable": "true",
					},
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-4",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch":          "amd64",
						"voiyd.io/os":            "linux",
						"voiyd.io/unschedulable": "true",
					},
				},
				Status: &tasksv1.Status{},
			},
		},
		{
			name: "should replace args",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-5",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					Args:  []string{"--config /mnt/cfg/config.yaml", "--log-level debug", "--insecure-skip-verify"},
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-5",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
						"--log-level debug",
						"--insecure-skip-verify",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{},
			},
		},
		{
			name: "should add to port mappings",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-6",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "metrics",
							TargetPort: 9000,
						},
						{
							Name:       "http",
							HostPort:   8088,
							TargetPort: 80,
							Protocol:   "TCP",
						},
						{
							Name:       "https",
							TargetPort: 8443,
						},
					},
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-6",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8088,
							TargetPort: 80,
							Protocol:   "TCP",
						},
						{
							Name:       "metrics",
							TargetPort: 9000,
						},
						{
							Name:       "https",
							TargetPort: 8443,
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{},
			},
		},
		{
			name: "should add to empty args field",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "bare-task",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "bare-task",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
				},
				Status: &tasksv1.Status{},
			},
		},
		{
			name: "should only update status",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-7",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
				},
				Status: &tasksv1.Status{
					Phase: wrapperspb.String("RUNNING"),
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-7",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{
					Phase: wrapperspb.String("RUNNING"),
				},
			},
		},
		{
			name: "should not add volumes with identic names",
			patch: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-8",
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					Mounts: []*tasksv1.Mount{
						{
							Name:        "data",
							Source:      "/data",
							Destination: "/mnt/data",
							Type:        "bind",
						},
						{
							Name:        "data",
							Source:      "/backup",
							Destination: "/mnt/backup",
							Type:        "bind",
						},
					},
				},
				Status: &tasksv1.Status{
					Phase: wrapperspb.String("RUNNING"),
				},
			},
			expect: &tasksv1.Task{
				Meta: &types.Meta{
					Name: "test-task-8",
					Labels: map[string]string{
						"team":        "backend",
						"environment": "production",
						"role":        "root",
					},
				},
				Config: &tasksv1.Config{
					Image: "docker.io/library/nginx:latest",
					PortMappings: []*tasksv1.PortMapping{
						{
							Name:       "http",
							HostPort:   8080,
							TargetPort: 80,
							Protocol:   "TCP",
						},
					},
					Envvars: []*tasksv1.EnvVar{
						{
							Name:  "HTTP_PROXY",
							Value: "proxy.foo.com",
						},
					},
					Args: []string{
						"--config /mnt/cfg/config.yaml",
					},
					Mounts: []*tasksv1.Mount{
						{
							Name:        "temp",
							Source:      "/tmp",
							Destination: "/mnt/tmp",
							Type:        "bind",
						},
						{
							Name:        "data",
							Source:      "/backup",
							Destination: "/mnt/backup",
							Type:        "bind",
						},
					},
					NodeSelector: map[string]string{
						"voiyd.io/arch": "amd64",
						"voiyd.io/os":   "linux",
					},
				},
				Status: &tasksv1.Status{
					Phase: wrapperspb.String("RUNNING"),
				},
			},
		},
	}

	// Run tests
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req := &tasksv1.PatchRequest{Id: tt.patch.Meta.Name, Task: tt.patch}
			res, err := client.Patch(ctx, req)
			if err != nil {
				t.Fatal("error updating task", err)
			}

			exp := tt.expect
			// Ignore timestamps
			res.Task.Meta.Created = nil
			res.Task.Meta.Updated = nil
			res.Task.Meta.ResourceVersion = 0
			if !proto.Equal(exp, res.Task) {
				t.Errorf("\ngot:\n%v\nwant:\n%v", res.Task, exp)
			}
		})
	}
}
