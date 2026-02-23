package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"ride-sharing/shared/tracing"
)

var (
	httpAddr = env.GetString("HTTP_ADDR", ":8081")
)

func main() {
	// Initialize tracing
	tracerCfg := tracing.Config{
		ServiceName:    "api-gateway",
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

	rabbitMQUri := env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")

	// Connecting Rabbit MQ server...
	rabbitMq, err := messaging.NewRabbitMQ(rabbitMQUri)
	if err != nil {
		log.Fatalf("An error has occurred when starting a grpc server: %v", err)
	}

	log.Println("Starting Rabbit MQ server")
	defer rabbitMq.Close()

	log.Println("Starting API Gateway")

	mux := http.NewServeMux()

	mux.Handle("POST /trip/preview", tracing.WrapHandlerFunc(enableCORS(handleTripPreview), "/trip/preview"))
	mux.Handle("POST /trip/start", tracing.WrapHandlerFunc(enableCORS(handleTripStart), "/trip/start"))
	mux.Handle("/ws/drivers", tracing.WrapHandlerFunc(enableCORS(func(w http.ResponseWriter, r *http.Request) {
		handleDriversWebSocket(w, r, rabbitMq)
	}), "/ws/drivers"))
	mux.Handle("/ws/riders", tracing.WrapHandlerFunc(enableCORS(func(w http.ResponseWriter, r *http.Request) {
		handleRidersWebsocket(w, r, rabbitMq)
	}), "/ws/riders"))
	mux.Handle("/webhook/stripe", tracing.WrapHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleStripeWebhook(w, r, rabbitMq)
	}, "/webhook/stripe"))

	server := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf("Server listening on port: %s", httpAddr)
		serverErrors <- server.ListenAndServe()
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		log.Printf("Error starting the server: %v", err)
	case sig := <-shutdown:
		log.Printf("Server has been shutdown, due to: %v", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Print("There has been an error with shutting down the server gracefully. Reason: %v", err)
			server.Close()
		}
	}
}
