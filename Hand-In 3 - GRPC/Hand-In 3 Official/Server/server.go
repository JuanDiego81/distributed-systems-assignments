package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	proto "ChitChat/grpc"

	"google.golang.org/grpc"
)

type chitChatServer struct {
	proto.UnimplementedChitChatServer

	mu       sync.RWMutex
	clients  map[string]proto.ChitChat_ChatServer
	messages chan *proto.ChatMessage
	nextID   int
	// Lamport clock on server
	clock uint64
}

func newServer() *chitChatServer {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // timestamp in log lines
	return &chitChatServer{
		clients:  make(map[string]proto.ChitChat_ChatServer),
		messages: make(chan *proto.ChatMessage, 1024),
	}
}

func (s *chitChatServer) Chat(stream proto.ChitChat_ChatServer) error {
	var (
		clientID       string
		username       string
		announcedLeave bool // ensure "left" is broadcast once
	)

	// ensure a "left" broadcast on unexpected disconnect
	defer func() {
		if clientID == "" || announcedLeave {
			return
		}
		s.mu.Lock()
		s.clock++ // server local event
		sys := &proto.ChatMessage{
			Id:          clientID,
			Sender:      "Server",
			Content:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d.", username, s.clock),
			Type:        "system",
			LogicalTime: s.clock,
		}
		delete(s.clients, clientID)
		s.mu.Unlock()

		log.Printf("[Server][DISCONNECT] id=%s user=%s", clientID, username)
		log.Printf("[Server][BROADCAST] %s", sys.Content)
		s.messages <- sys
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Printf("[Server][RECV_ERR] %v", err)
			return err
		}

		// Lamport receive rule: t = max(t, t')
		s.mu.Lock()
		oldClock := s.clock
		if msg.LogicalTime > s.clock {
			s.clock = msg.LogicalTime
		}

		switch msg.Type {
		case "join":
			if clientID == "" {
				// Increment only if this is not the very first client (use oldClock)
				if oldClock > 0 {
					s.clock++
				}
				s.nextID++
				clientID = fmt.Sprintf("user-%d", s.nextID)
				username = msg.Sender
				s.clients[clientID] = stream

				// use current clock for the system broadcast
				sys := &proto.ChatMessage{
					Id:          clientID,
					Sender:      "Server",
					Content:     fmt.Sprintf("Participant %s joined Chit Chat at logical time %d.", username, s.clock),
					Type:        "system",
					LogicalTime: s.clock,
				}
				s.mu.Unlock()

				log.Printf("[Server][CONNECT] id=%s user=%s", clientID, username)
				log.Printf("[Server][BROADCAST] %s", sys.Content)
				s.messages <- sys
				continue
			}
			s.mu.Unlock()

		case "leave":
			if clientID != "" && !announcedLeave {
				s.clock++ // Increment for leave event
				announcedLeave = true
				sys := &proto.ChatMessage{
					Id:          clientID,
					Sender:      "Server",
					Content:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d.", username, s.clock),
					Type:        "system",
					LogicalTime: s.clock,
				}
				delete(s.clients, clientID)
				s.mu.Unlock()

				log.Printf("[Server][DISCONNECT] id=%s user=%s", clientID, username)
				log.Printf("[Server][BROADCAST] %s", sys.Content)
				s.messages <- sys
				return nil
			}
			s.mu.Unlock()

		case "message":
			s.clock++ // Increment for message event
			out := &proto.ChatMessage{
				Id:          clientID,
				Sender:      username,
				Content:     msg.Content,
				Type:        "message",
				LogicalTime: s.clock,
			}
			s.mu.Unlock()

			log.Printf("[Server][MSG] user=%s id=%s L=%d: %s", username, clientID, out.LogicalTime, out.Content)
			s.messages <- out

		default:
			s.mu.Unlock()
		}
	}
}

// broadcaster with per recipient delivery logging
func (s *chitChatServer) runBroadcaster(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.messages:
			if !ok {
				return
			}
			// snapshot recipients
			s.mu.RLock()
			recipients := make(map[string]proto.ChitChat_ChatServer, len(s.clients))
			for id, st := range s.clients {
				recipients[id] = st
			}
			s.mu.RUnlock()

			for id, st := range recipients {
				if err := st.Send(msg); err != nil {
					log.Printf("[Server][DELIVER_ERR] to=%s type=%s L=%d err=%v (removing)", id, msg.Type, msg.LogicalTime, err)
					s.mu.Lock()
					delete(s.clients, id)
					s.mu.Unlock()
					continue
				}
				log.Printf("[Server][DELIVER] to=%s type=%s L=%d", id, msg.Type, msg.LogicalTime)
			}
		}
	}
}

func main() {
	// Open or create a log file
	file, err := os.OpenFile("servicelogs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer file.Close()

	multiWriter := io.MultiWriter(os.Stdout, file)
	log.SetOutput(multiWriter)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("[Server][STARTUP_ERR] %v", err)
	}

	s := newServer()
	grpcServer := grpc.NewServer()
	proto.RegisterChitChatServer(grpcServer, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.runBroadcaster(ctx)

	// graceful shutdown via OS signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("[Server][SHUTDOWN] signal=%v", sig)
		cancel()
		close(s.messages)
		grpcServer.GracefulStop()
		log.Printf("[Server][STOPPED]")
	}()

	log.Printf("[Server][STARTUP] ChitChat server started on %s", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[Server][SERVE_ERR] %v", err)
	}
}
