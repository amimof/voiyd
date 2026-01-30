package nodeupgradecontroller

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/jarcoal/httpmock"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/amimof/voiyd/pkg/client"
	"github.com/amimof/voiyd/pkg/condition"
	"github.com/amimof/voiyd/pkg/logger"
	"github.com/amimof/voiyd/services/node"

	nodesv1 "github.com/amimof/voiyd/api/services/nodes/v1"
	typesv1 "github.com/amimof/voiyd/api/types/v1"
	nodesclientv1 "github.com/amimof/voiyd/pkg/client/node/v1"
)

func TestReplaceBinary_Success(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "new-binary")
	dstPath := filepath.Join(dir, "current-binary")

	// Create source and destination files in tmp path
	if err := os.WriteFile(srcPath, []byte("NEW"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dstPath, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Setup node controller
	c := &Controller{}
	if err := c.replaceBinary(srcPath, dstPath); err != nil {
		t.Fatalf("replaceBinary() error = %v", err)
	}

	// Backup exists with old content
	backupPath := dstPath + "_backup"
	backup, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("reading backup: %v", err)
	}
	if string(backup) != "OLD" {
		t.Errorf("backup content = %q, want %q", string(backup), "OLD")
	}

	// Destination has new content
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if string(got) != "NEW" {
		t.Errorf("dst content = %q, want %q", string(got), "NEW")
	}

	// Source was renamed, so it should not exist
	if _, err := os.Stat(srcPath); !os.IsNotExist(err) {
		t.Errorf("src should be renamed away, got err = %v", err)
	}
}

func TestDownloadBinary_Success(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup mock client
	nodeMockClient := nodesclientv1.NewMockNodeServiceClient(ctrl)
	nodesV1 := nodesclientv1.NewClientV1(nodesclientv1.WithClient(nodeMockClient))

	// Mock the Get() call that retrieves the node before reporting conditions
	nodeMockClient.
		EXPECT().
		Get(
			gomock.Any(), // context
			gomock.Eq(&nodesv1.GetRequest{Id: "test-node"}),
		).
		Return(&nodesv1.GetResponse{
			Node: &nodesv1.Node{
				Meta: &typesv1.Meta{
					Name:            "test-node",
					Generation:      1,
					ResourceVersion: 1,
				},
			},
		}, nil).
		Times(1)

	// Expect condition report for NodeReady=False with ReasonUpgrading
	nodeMockClient.
		EXPECT().
		Condition(
			gomock.Any(), // context
			gomock.Any(), // ConditionRequest - we validate it in DoAndReturn
			gomock.Any(), // grpc.CallOption
		).
		DoAndReturn(func(ctx context.Context, req *typesv1.ConditionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
			// Validate the condition request
			if req.GetResourceVersion() != node.Version {
				t.Errorf("expected resource_version=%s, got %s", node.Version, req.GetResourceVersion())
			}
			report := req.GetReport()
			if report.GetResourceId() != "test-node" {
				t.Errorf("expected resource_id=test-node, got %s", report.GetResourceId())
			}
			if report.GetType() != string(condition.NodeReady) {
				t.Errorf("expected condition type=%s, got %s", condition.NodeReady, report.GetType())
			}
			if report.GetStatus() != typesv1.ConditionStatus_CONDITION_STATUS_FALSE {
				t.Errorf("expected status=FALSE, got %v", report.GetStatus())
			}
			if report.GetReason() != string(condition.ReasonUpgrading) {
				t.Errorf("expected reason=%s, got %s", condition.ReasonUpgrading, report.GetReason())
			}
			return &emptypb.Empty{}, nil
		}).
		Times(1)

	// Setup http client and activate httpmock on it
	httpClient := &http.Client{}
	httpmock.ActivateNonDefault(httpClient)
	defer httpmock.DeactivateAndReset()

	// Register mock responder for GitHub release metadata call
	httpmock.RegisterResponder(
		"GET",
		"https://api.github.com/repos/amimof/voiyd/releases/tags/v0.0.8",
		httpmock.NewJsonResponderOrPanic(
			200,
			map[string]any{
				"tag_name":     "v0.0.8",
				"name":         "v0.0.8",
				"published_at": "2025-12-28T19:30:48Z",
				"assets": []map[string]string{
					{
						"name":                 "voiyd-node-linux-amd64",
						"browser_download_url": "https://github.com/amimof/voiyd/releases/download/v0.0.8/voiyd-node-linux-amd64",
					},
				},
			},
		),
	)

	// Register mock responder for the binary download
	httpmock.RegisterResponder(
		"GET",
		"https://github.com/amimof/voiyd/releases/download/v0.0.8/voiyd-node-linux-amd64",
		httpmock.NewStringResponder(200, "BINARYDATA"),
	)

	c := &Controller{
		tmpPath: dir,
		clientset: &client.ClientSet{
			NodeV1Client: nodesV1,
		},
		node: &nodesv1.Node{
			Meta: &typesv1.Meta{ // check actual type
				Name: "test-node",
			},
		},
		logger:     logger.ConsoleLogger{}, // or custom
		httpClient: httpClient,
		nodeVersion: &nodeVersion{
			version:   "v0.0.8",
			commit:    "3d934e5ea0edd697d922816170934add6ea761d8",
			branch:    "master",
			goversion: "1.25.2",
		},
	}

	ctx := context.Background()
	path, err := c.downloadBinary(ctx, "v0.0.8", "amd64")
	if err != nil {
		t.Fatalf("downloadBinary() error = %v", err)
	}

	// File is created in tmpPath with expected contents
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading temp file: %v", err)
	}

	if string(data) != "BINARYDATA" {
		t.Errorf("downloaded data = %q, want %q", string(data), "BINARYDATA")
	}

	// Verify httpmock calls
	expect := "GET https://api.github.com/repos/amimof/voiyd/releases/tags/v0.0.8"
	info := httpmock.GetCallCountInfo()
	if info[expect] != 1 {
		t.Errorf("expected 1 call to release API, got %d", info[expect])
	}

	expect = "GET https://github.com/amimof/voiyd/releases/download/v0.0.8/voiyd-node-linux-amd64"
	if info[expect] != 1 {
		t.Errorf("expected 1 call to binary download, got %d", info[expect])
	}
}
