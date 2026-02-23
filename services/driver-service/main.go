package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"ride-sharing/shared/tracing"
	"syscall"

	grpcserver "google.golang.org/grpc"
)

var GrpcAddr = ":9092"

func main() {
	rabbitMQUri := env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")

	// Initialize tracing
	tracerCfg := tracing.Config{
		ServiceName:    "driver-service",
		Environment:    env.GetString("ENVIRONMENT", "development"),
		JaegerEndpoint: env.GetString("JAEGER_ENDPOINT", "development"),
	}

	ctx, cancel := context.WithCancel(context.Background())

	shDown, err := tracing.InitTracer(tracerCfg)
	if err != nil {
		log.Fatalf("Failed to Initialize the tracer, %v", err)
	}

	defer cancel()
	defer shDown(ctx)

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

	// Connecting Rabbit MQ server...
	rabbitMq, err := messaging.NewRabbitMQ(rabbitMQUri)
	if err != nil {
		log.Fatalf("An error has occurred when starting a grpc server: %v", err)
	}

	log.Println("Starting Rabbit MQ server")
	defer rabbitMq.Close()

	consumer := NewTripConsumer(rabbitMq, service)

	go func() {
		if err := consumer.Listen(); err != nil {
			log.Fatalf("Failed to listen to the message: %v", err)
		}
	}()
	// Launching the Grpc server with the Driver service as a dependency
	grpcServer := grpcserver.NewServer(tracing.WithTracingInterceptors()...)

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
