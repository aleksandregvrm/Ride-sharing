package main

import (
	"context"
	pb "ride-sharing/shared/proto/driver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcHandler struct {
	pb.UnimplementedDriverServiceServer
	Service *Service
}

func newGrpcHandler(s *grpc.Server, service *Service) {
	handler := &grpcHandler{
		Service: service,
	}
	pb.RegisterDriverServiceServer(s, handler)
}

func (h *grpcHandler) RegisterDriver(ctx context.Context, req *pb.RegisterDriverRequest) (*pb.RegisterDriverResponse, error) {
	driverID := req.GetDriverID()
	packageSlug := req.GetPackageSlug()

	driver, err := h.Service.RegisterDriver(driverID, packageSlug)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "An error occurred with registering the driver %v", err)
	}

	return &pb.RegisterDriverResponse{
		Driver: driver,
	}, nil
}

func (h *grpcHandler) UnregisterDriver(ctx context.Context, req *pb.UnregisterDriverRequest) (*pb.UnregisterDriverResponse, error) {
	driverID := req.GetDriverID()

	driver, err := h.Service.UnregisterDriver(driverID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "An error occurred with unregistering the driver %v", err)
	}
	return &pb.UnregisterDriverResponse{
		DriverID: driver.Id,
	}, nil
}
