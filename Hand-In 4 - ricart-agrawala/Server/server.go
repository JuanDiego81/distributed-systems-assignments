package server

import (
	"context" // Provides context.Context for request scoping, deadlines, cancellation.
	"fmt"     // For formatted error strings (fmt.Errorf).
	"log"     // For basic logging if the Node's logger isn't available.
	"net"     // For creating a TCP listener (net.Listen).

	"google.golang.org/grpc" // The gRPC server library.
	pb "ricart-agrawala/grpc" // Generated code from your .proto (Mutex service + messages).
)

// NodeAPI defines what a Node must implement for the server to forward calls.
// In other words, the server does not implement Ricart–Agrawala itself — it delegates
// the logic to whatever object implements these methods (your Node struct does).
type NodeAPI interface {
	HandleIncomingRequest(fromID int32, timestamp int32) // Called when a Request RPC arrives.
	HandleIncomingReply(fromID int32)                    // Called when a Reply RPC arrives.
	Logf(format string, args ...any)                     // Logging hook with node context (id/clock/state).
}

// Server implements the gRPC Mutex service and delegates logic to the Node.
// It receives RPC messages (Request, Reply) and simply forwards them to the Node logic.
type Server struct {
	pb.UnimplementedMutexServer // Embeds default (no-op) implementations; forward compatible.
	node NodeAPI                // The Node implementing RA logic that handles incoming messages.
}

// New creates a new Server instance bound to a specific Node.
// This lets the server know which Node's logic it should delegate messages to.
func New(node NodeAPI) *Server {
	return &Server{node: node} // Store the Node so we can call its handlers later.
}

// Request handler: called whenever another node sends a Request() RPC.
// A Request means "I want to enter the critical section".
func (s *Server) Request(ctx context.Context, req *pb.RequestMsg) (*pb.Ack, error) {
	if req == nil {                 // Defensive: if the message is nil, just acknowledge and return.
		return &pb.Ack{}, nil
	}
	// Forward to the Node's Ricart–Agrawala logic:
	// update Lamport clock, decide defer vs immediate reply, etc.
	s.node.HandleIncomingRequest(req.GetFromId(), req.GetTimestamp())

	// Return an empty Ack — this just signals that the RPC was processed successfully.
	return &pb.Ack{}, nil
}

// Reply handler: called whenever another node sends a Reply() RPC.
// A Reply means "I grant you permission to enter the critical section".
func (s *Server) Reply(ctx context.Context, rep *pb.ReplyMsg) (*pb.Ack, error) {
	if rep == nil {                 // Defensive: handle unexpected nil messages gracefully.
		return &pb.Ack{}, nil
	}
	// Forward to the Node logic: count this reply towards N-1 total needed replies.
	s.node.HandleIncomingReply(rep.GetFromId())

	// Again, return an empty Ack to indicate successful processing.
	return &pb.Ack{}, nil
}

// Serve starts a gRPC server on the given address (host:port) and binds it to our Server.
// This is what allows other nodes to reach this node's Mutex service.
func Serve(node NodeAPI, addr string) error {
	// 1) Start listening on a TCP port (e.g., "127.0.0.1:5001").
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	// 2) Create a new gRPC server (no TLS, since we're using insecure creds in local testing).
	grpcServer := grpc.NewServer()

	// 3) Register our Server implementation with the gRPC runtime.
	// The generated code knows about the Mutex service and will call our methods on incoming RPCs.
	pb.RegisterMutexServer(grpcServer, New(node))

	// 4) Log that the server is now running.
	if node != nil {
		node.Logf("gRPC server listening at %s", addr)
	} else {
		log.Printf("[server] gRPC server listening at %s", addr)
	}

	// 5) Start serving — this blocks forever until the server is stopped or an error occurs.
	return grpcServer.Serve(lis)
}
