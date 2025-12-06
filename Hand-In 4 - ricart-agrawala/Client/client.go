package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "ricart-agrawala/grpc" // generated proto package
)

// Logger type lets us inject Node’s logger
type Logger func(format string, args ...any)

// ClientManager caches gRPC clients and manages connections to peers.
// Each node can call SendRequest/SendReply to communicate with others.
type ClientManager struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn // cached open connections
	clients map[string]pb.MutexClient   // cached stubs for service calls
	timeout time.Duration               // RPC timeout
	logf    Logger                      // optional log hook
}

// NewClientManager creates a new manager with timeout and logger.
func NewClientManager(timeout time.Duration, logf Logger) *ClientManager {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &ClientManager{
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]pb.MutexClient),
		timeout: timeout,
		logf:    logf,
	}
}

// getClient returns a cached client connection or opens a new one.
func (m *ClientManager) getClient(addr string) (pb.MutexClient, error) {
	m.mu.Lock()
	if c, ok := m.clients[addr]; ok {
		m.mu.Unlock()
		return c, nil
	}
	m.mu.Unlock()

	// Create a new insecure (non-TLS) connection for local testing
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", addr, err)
	}

	client := pb.NewMutexClient(conn)

	m.mu.Lock()
	m.conns[addr] = conn
	m.clients[addr] = client
	m.mu.Unlock()

	if m.logf != nil {
		m.logf("[client] connected to %s", addr)
	}
	return client, nil
}

// Close closes all open gRPC connections
func (m *ClientManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for addr, c := range m.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", addr, err)
		}
		delete(m.conns, addr)
		delete(m.clients, addr)
	}
	return firstErr
}

// SendRequest sends a REQUEST RPC to another node
func (m *ClientManager) SendRequest(addr string, fromID int32, lamportTS int32) (*pb.Ack, error) {
	client, err := m.getClient(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	req := &pb.RequestMsg{FromId: fromID, Timestamp: lamportTS}
	ack, err := client.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("request -> %s: %w", addr, err)
	}
	return ack, nil
}

// SendReply sends a REPLY RPC to a peer
func (m *ClientManager) SendReply(addr string, fromID int32) (*pb.Ack, error) {
	client, err := m.getClient(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	rep := &pb.ReplyMsg{FromId: fromID}
	ack, err := client.Reply(ctx, rep)
	if err != nil {
		return nil, fmt.Errorf("reply -> %s: %w", addr, err)
	}
	return ack, nil
}

// BroadcastRequest concurrently sends REQUEST to all peers
func (m *ClientManager) BroadcastRequest(addrs []string, fromID int32, lamportTS int32) map[string]error {
	errs := make(map[string]error)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, addr := range addrs {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.SendRequest(addr, fromID, lamportTS)
			if err != nil {
				mu.Lock()
				errs[addr] = err
				mu.Unlock()
				if m.logf != nil {
					m.logf("[client] Request to %s failed: %v", addr, err)
				}
			}
		}()
	}
	wg.Wait()
	return errs
}

