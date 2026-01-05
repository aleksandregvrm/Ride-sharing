package main

import (
	"log"
	"net/http"
	"ride-sharing/services/api-gateway/grpc_clients"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/proto/driver"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func handleRidersWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("An error has occurred with connection to the WebSockets, - %v", err)
		return
	}

	defer conn.Close()

	userID := r.URL.Query().Get("userID")

	if userID == "" {
		log.Println("User ID has not been provided.")
		return
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

func handleDriversWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("An error has occurred with connection to the WebSockets, - %v", err)
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

	ctx := r.Context()

	driverService, err := grpc_clients.NewDriverServiceClient()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
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

	msg := contracts.WSMessage{
		Type: "driver.cmd.register",
		Data: driverData.Driver,
	}

	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Error sending message: %v", err)
		return
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
