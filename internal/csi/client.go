package csi

import (
	"context"
	"errors"
	"log/slog"
	"net"

	csipbv1 "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client interface {
	NodeSupportsStageUnstage(ctx context.Context, logger *slog.Logger) (bool, error)
	NodeSupportsVolumeCondition(ctx context.Context, logger *slog.Logger) (bool, error)
	GetDriverName(ctx context.Context, logger *slog.Logger) (string, error)
	IsHealthy(ctx context.Context, logger *slog.Logger) (bool, error)
	Close() error
}

type client struct {
	grpcClient *grpc.ClientConn
	csipbv1.NodeClient
	csipbv1.IdentityClient
}

var _ Client = &client{}

func newGrpcConn(addr string, logger *slog.Logger) (*grpc.ClientConn, error) {
	network := "unix"
	logger.Info("creating new gRPC connection", "protocol", network, "endpoint", addr)

	return grpc.NewClient(
		string(addr),
		grpc.WithAuthority("localhost"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, target)
		}),
	)
}

func NewClient(addr string, logger *slog.Logger) (Client, error) {
	conn, err := newGrpcConn(addr, logger)
	if err != nil {
		return nil, err
	}

	return &client{
		grpcClient:     conn,
		NodeClient:     csipbv1.NewNodeClient(conn),
		IdentityClient: csipbv1.NewIdentityClient(conn),
	}, nil
}

func (c *client) Close() error {
	return c.grpcClient.Close()
}

func (c *client) GetDriverName(ctx context.Context, logger *slog.Logger) (string, error) {
	logger.Info("calling GetPluginInfo rpc to get the driver name")
	resp, err := c.IdentityClient.GetPluginInfo(ctx, &csipbv1.GetPluginInfoRequest{})
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", errors.New("response is nil")
	}
	return resp.Name, nil
}

func (c *client) IsHealthy(ctx context.Context, logger *slog.Logger) (bool, error) {
	logger.Info("calling NodeGetInfo rpc to check if the node service is healthy")
	resp, err := c.IdentityClient.Probe(ctx, &csipbv1.ProbeRequest{})
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, errors.New("response is nil")
	}
	return resp.Ready.Value, nil
}
func (c *client) nodeGetCapabilities(ctx context.Context) ([]*csipbv1.NodeServiceCapability, error) {
	if c.NodeClient == nil {
		return []*csipbv1.NodeServiceCapability{}, errors.New("nodeclient is nil")
	}

	req := &csipbv1.NodeGetCapabilitiesRequest{}
	resp, err := c.NodeClient.NodeGetCapabilities(ctx, req)
	if err != nil {
		return []*csipbv1.NodeServiceCapability{}, err
	}
	return resp.GetCapabilities(), nil
}

func (c *client) nodeSupportsCapability(ctx context.Context, logger *slog.Logger, capabilityType csipbv1.NodeServiceCapability_RPC_Type) (bool, error) {
	logger.Info("calling NodeGetCapabilities rpc to determine if the node service",
		"capability", capabilityType)
	capabilities, err := c.nodeGetCapabilities(ctx)
	if err != nil {
		return false, err
	}

	for _, capability := range capabilities {
		if capability == nil || capability.GetRpc() == nil {
			continue
		}
		if capability.GetRpc().GetType() == capabilityType {
			return true, nil
		}
	}
	return false, nil
}

func (c *client) NodeSupportsVolumeCondition(ctx context.Context, logger *slog.Logger) (bool, error) {
	return c.nodeSupportsCapability(ctx, logger, csipbv1.NodeServiceCapability_RPC_VOLUME_CONDITION)
}

func (c *client) NodeSupportsStageUnstage(ctx context.Context, logger *slog.Logger) (bool, error) {
	return c.nodeSupportsCapability(ctx, logger, csipbv1.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME)
}
