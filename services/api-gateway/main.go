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
)

var (
	httpAddr = env.GetString("HTTP_ADDR", ":8081")
)

func main() {
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

	mux.HandleFunc("POST /trip/preview", enableCORS(handleTripPreview))
	mux.HandleFunc("POST /trip/start", enableCORS(handleTripStart))
	mux.HandleFunc("/ws/drivers", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		handleDriversWebSocket(w, r, rabbitMq)
	}))
	mux.HandleFunc("/ws/riders", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		handleRidersWebsocket(w, r, rabbitMq)
	}))
	mux.HandleFunc("/webhook/stripe", func(w http.ResponseWriter, r *http.Request) {
		handleStripeWebhook(w, r, rabbitMq)
	})

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
