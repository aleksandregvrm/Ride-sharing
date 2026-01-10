package main

import (
	"encoding/json"
	"log"
	"net/http"
	"ride-sharing/services/api-gateway/grpc_clients"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"
	"ride-sharing/shared/proto/driver"
)

var (
	connManager = messaging.NewConnectionManager()
)

func handleRidersWebsocket(w http.ResponseWriter, r *http.Request, rb *messaging.RabbitMQ) {
	conn, err := connManager.Upgrade(w, r)
	if err != nil {
		log.Printf("Websocket upgrade has failed, - %v", err)
		return
	}

	defer conn.Close()

	userID := r.URL.Query().Get("userID")

	if userID == "" {
		log.Println("User ID has not been provided.")
		return
	}
	// Add connection to the manager
	connManager.Add(userID, conn)

	defer connManager.Remove(userID)

	// Initialize queue consumers.
	queues := []string{messaging.NotifyDriverNoDriversFoundQueue, messaging.NotifyDriverAssignQueue}

	for _, q := range queues {
		consumer := messaging.NewQueueConsumer(rb, connManager, q)
		if err := consumer.Start(); err != nil {
			log.Printf("Failed to start consumer for queue: %s; err: %v", q, err)
		}
	}

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("An error has occurred with reading the WebSocket message, - %v", err)
			break
		}

		log.Printf("Received message: %s", message)
	}

}

func handleDriversWebSocket(w http.ResponseWriter, r *http.Request, rb *messaging.RabbitMQ) {
	conn, err := connManager.Upgrade(w, r)
	if err != nil {
		log.Printf("Websocket upgrade has failed, - %v", err)
		return
	}

	defer conn.Close()

	userID := r.URL.Query().Get("userID")
	if userID == "" {
		log.Println("User ID has not been provided.")
		return
	}

	packageSlug := r.URL.Query().Get("packageSlug")

	if packageSlug == "" {
		log.Println("No Package Slug Detected.")
		return
	}

	// Add connection to the manager
	connManager.Add(userID, conn)

	ctx := r.Context()

	driverService, err := grpc_clients.NewDriverServiceClient()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		connManager.Remove(userID)
		driverService.Client.UnregisterDriver(ctx, &driver.UnregisterDriverRequest{
			DriverID: userID,
		})
		driverService.Close()
		log.Println("Driver has been unregistered", userID)
	}()

	driverData, err := driverService.Client.RegisterDriver(ctx, &driver.RegisterDriverRequest{
		DriverID:    userID,
		PackageSlug: packageSlug,
	})

	if err != nil {
		log.Printf("Error registering driver: %v", err)
		return
	}

	if err := connManager.SendMessage(userID, contracts.WSMessage{
		Type: contracts.DriverCmdRegister,
		Data: driverData.Driver,
	}); err != nil {
		log.Printf("Error sending message: %v", err)
		return
	}

	// Initialize queue consumers.
	queues := []string{messaging.DriverCmdTripRequestQueue}

	for _, q := range queues {
		consumer := messaging.NewQueueConsumer(rb, connManager, q)
		if err := consumer.Start(); err != nil {
			log.Printf("Failed to start consumer for queue: %s; err: %v", q, err)
		}
	}
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("An error has occurred with reading the WebSocket message, - %v", err)
			break
		}
		log.Printf("Received message: %s", message)

		type driverMessage struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}

		var driverMsg driverMessage

		if err := json.Unmarshal(message, &driverMsg); err != nil {
			log.Printf("An error has occurred with reading the WebSocket message, - %v", err)
			continue
		}

		// Handle the different message types
		switch driverMsg.Type {
		case contracts.DriverCmdLocation:
			continue
		case contracts.DriverCmdTripAccept, contracts.DriverCmdTripDecline:
			if err := rb.PublishMessage(ctx, driverMsg.Type, contracts.AmqpMessage{
				OwnerID: userID,
				Data:    driverMsg.Data,
			}); err != nil {
				log.Printf("An error has occurred with reading the WebSocket message, - %v", err)
				continue
			}
		default:
			log.Printf("Unknown message type: %s", driverMsg.Type)
		}

	}
}
