package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"ride-sharing/services/trip-service/internal/infrastructure/events"
	"ride-sharing/services/trip-service/internal/infrastructure/grpc"
	"ride-sharing/services/trip-service/internal/infrastructure/repository"
	"ride-sharing/services/trip-service/internal/service"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"syscall"

	grpcserver "google.golang.org/grpc"
)

var GrpcAddr = ":9093"

func main() {
	rabbitMQUri := env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")
	inmemRepo := repository.NewInmemRepository()
	svc := service.NewService(inmemRepo)

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

	// Connecting Rabbit MQ server...
	rabbitMq, err := messaging.NewRabbitMQ(rabbitMQUri)
	if err != nil {
		log.Fatalf("An error has occurred when starting a grpc server: %v", err)
	}

	defer rabbitMq.Close()

	publisher := events.NewTripEventPublisher(rabbitMq)

	driverConsumer := events.NewDriverConsumer(rabbitMq, svc)

	paymentConsumer := events.NewPaymentConsumer(rabbitMq, svc)

	go paymentConsumer.Listen()

	go driverConsumer.Listen()

	log.Println("Starting Rabbit MQ server")
	// Launching the Grpc server with the trip service as a dependency
	grpcServer := grpcserver.NewServer()

	grpc.NewGrpcHandler(grpcServer, svc, publisher)

	log.Printf("Starting gRPC server trip service on port: %s", lis.Addr().String())

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
