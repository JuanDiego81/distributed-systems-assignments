package main

import (
	proto "ChitChat/grpc"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
	"os"
    "os/signal"
    "syscall"

	"google.golang.org/grpc"
)

type ChitChat_databaseServer struct {
	proto.UnimplementedChitChatServer
	mu       sync.Mutex
	clients  map[string]proto.ChitChat_ChatServer
	messages chan *proto.ChatMessage // channel for broadcasting messages
	nextID int
}

func NewServer() *ChitChat_databaseServer {  // this creates a new instance of the server struct from above
	s := &ChitChat_databaseServer{
		clients:  make(map[string]proto.ChitChat_ChatServer),
		messages: make(chan *proto.ChatMessage, 100),   // buffer of 100 msg, broadcaster should read msgs often so we should never fill the channel
		nextID: 1,
	}

	// Start goroutine for broadcasting messages
	go s.handleBroadcasts()
	return s
}

// This goroutine continuously reads from the messages channel and sends to all clients
func (s *ChitChat_databaseServer) handleBroadcasts() {
	for msg := range s.messages {
		s.mu.Lock()   // lock to protect maps so no other go routine can add or remove clients
		for id, client := range s.clients {
			err := client.Send(msg)  // sending message to all clients
			if err != nil {
				log.Printf("Error sending to %s: %v", id, err)
			}
		}
		s.mu.Unlock()
	}
}

func (s *ChitChat_databaseServer) Chat(stream proto.ChitChat_ChatServer) error {   // one instance of this method is run per connected client
	var clientID string
	var username string  // hold the name of the client who joins

	for {
		msg, err := stream.Recv()   // receives a message from a client
		if err == io.EOF {
			s.removeClient(clientID)
			return nil
		}
		if err != nil {
			log.Printf("Recv error: %v", err)
			return err
		}

		switch msg.Type {
		case "join":
			s.mu.Lock()
			clientID= fmt.Sprintf("user-%d", s.nextID)
			s.nextID++
			s.clients[clientID] = stream    
			s.mu.Unlock()
			
			username = msg.Sender
			// s.addClient(username, stream)   // You pass stream into addClient() because that’s how the server remembers the communication channel it can use later to send messages to that specific client.
			log.Printf("[JOIN] %s joined (id: %s)", username, clientID)  // this is just a log for the server interface, the timestamp gets added automatically like this:  2025/10/23 18:33:37 [JOIN] JD joined
			// Broadcast system message
			s.messages <- &proto.ChatMessage{   // it's sending the message pointer to s.messages channel that has the strcut from below
				Id:          clientID,
				Sender:      "Server",
				Content:     fmt.Sprintf("%s joined the chat (id: %s)", username, clientID),
				Type:        "system",
				LogicalTime: time.Now().Format(time.Stamp),
			}

		case "leave":
			log.Printf("[LEAVE] %s (id: %s) left", username, clientID)
			s.removeClient(clientID)
			s.messages <- &proto.ChatMessage{
				Id:          clientID,
				Sender:      "Server",
				Content:     fmt.Sprintf("%s left the chat (id: %s)", username, clientID),
				Type:        "system",
				LogicalTime: time.Now().Format(time.Stamp),
			}
			return nil
			
		case "message":
			newMsg := &proto.ChatMessage{
			Id:          clientID,
			Sender:      username,
			Content:     msg.Content,
			Type:        "message",
			LogicalTime: time.Now().Format(time.Stamp),
    	}
		log.Printf("[MSG] %s (id: %s): %s", username, clientID, msg.Content)
    	s.messages <- newMsg
	}
}
}// This msg (struct) goes to the messages channel and then in the go routine gets sent to all other clients

/* func (s *ChitChat_databaseServer) addClient(name string, stream proto.ChitChat_ChatServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[name] = stream
} */

func (s *ChitChat_databaseServer) removeClient(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, id)
}

func main() {
	server := NewServer()
	grpcServer := grpc.NewServer()
	proto.RegisterChitChatServer(grpcServer, server)

	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("[SERVER] Failed to listen: %v", err)
	}

	log.Printf("[SERVER] Startup: listening on port 5050")

	// Run gRPC server in a separate goroutine
	// this go routine is to capture when the server gets killed
	go func() {     
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Printf("[SERVER] Error: %v", err)
		}
	}()

	// Channel to catch OS interrupt/termination signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Wait here until a signal is received
	<-quit
	log.Printf("[SERVER] Shutdown signal received")

	// Gracefully stop accepting new connections
	grpcServer.GracefulStop()
	log.Printf("[SERVER] Gracefully stopped")
}
