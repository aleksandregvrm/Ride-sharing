package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	grpcserver "google.golang.org/grpc"
)

var GrpcAddr = ":9092"

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	lis, err := net.Listen("tcp", GrpcAddr)
	if err != nil {
		log.Fatalf("An error has occurred when starting a grpc server: %v", err)
	}
	service := NewService()

	// Launching the Grpc server with the Driver service as a dependency
	grpcServer := grpcserver.NewServer()

	newGrpcHandler(grpcServer, service)

	log.Printf("Starting gRPC server Driver service on port: %s", lis.Addr().String())

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("failed to server: %v", err)
			// cancel()
		}
	}()

	// Wait for the shutdown signal
	<-ctx.Done()
	grpcServer.GracefulStop()

}
