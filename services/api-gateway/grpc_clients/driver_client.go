package grpc_clients

import (
	"os"
	pb "ride-sharing/shared/proto/driver" // Trip service Proto import

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Connection with driver service
type driverServiceClient struct {
	Client pb.DriverServiceClient
	conn   *grpc.ClientConn
}

func NewDriverServiceClient() (*driverServiceClient, error) {
	driverServiceUrl := os.Getenv("DRIVER_SERVICE_URL")
	if driverServiceUrl == "" {
		driverServiceUrl = "trip-service:9093"
	}
	conn, err := grpc.NewClient(driverServiceUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewDriverServiceClient(conn)
	return &driverServiceClient{
		Client: client,
		conn:   conn,
	}, nil
}

func (c *driverServiceClient) Close() {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return
		}
	}
}
