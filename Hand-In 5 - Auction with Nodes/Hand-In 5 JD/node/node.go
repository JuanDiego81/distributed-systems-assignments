package node

import (
	"context"
	// "fmt"
	"log"
	"net"
	"strings"
	"time"

	proto "Auction/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	ID          int
	Address     string
	IsPrimary   bool
	PrimaryAddr string
	Peers       []string     // Other nodes that must receive replication.
	Duration    time.Duration
}

type Node struct {
	proto.UnimplementedAuctionServiceServer
	cfg         Config
	state       *AuctionState
	peerClients map[string]proto.AuctionServiceClient   // the key of this map is the address of other node, for example: "127.0.0.1:5001", and the value are the all the gRPC services
	server      *grpc.Server   // Handles incoming requests, for example, receive replicates from the primary
} 

func NewNode(cfg Config) *Node {   // node Constructor
	return &Node{
		cfg:         cfg,
		state:       NewAuctionState(cfg.Duration),
		peerClients: make(map[string]proto.AuctionServiceClient),
	}
}

func (n *Node) Start() error {
	lis, err := net.Listen("tcp", n.cfg.Address)
	if err != nil {
		return err
	}

	n.server = grpc.NewServer()
	proto.RegisterAuctionServiceServer(n.server, n)

	n.initPeers()

	log.Printf("Node %d up on %s (primary=%v)", n.cfg.ID, n.cfg.Address, n.cfg.IsPrimary)
	return n.server.Serve(lis)
}

func (n *Node) initPeers() {
	n.peerClients = make(map[string]proto.AuctionServiceClient)

	for _, p := range n.cfg.Peers {
		if strings.TrimSpace(p) == "" || p == n.cfg.Address {
			continue
		}

		log.Printf("[Node %d] Attempting to connect to peer: %s", n.cfg.ID, p)

		conn, err := grpc.NewClient(p, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[Node %d] Failed to connect to peer %s: %v", n.cfg.ID, p, err)
			continue
		}

		log.Printf("[Node %d] Connected to peer: %s", n.cfg.ID, p)
		n.peerClients[p] = proto.NewAuctionServiceClient(conn)
	}
}


func (n *Node) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {

	// If this node is not the primary, forward request to primary
	if !n.cfg.IsPrimary {
		primary := n.peerClients[n.cfg.PrimaryAddr]
		log.Printf("[Node %d] Forwarding BID to primary (%s): bidder=%s amount=%d", n.cfg.ID, n.cfg.PrimaryAddr, req.Bidder, req.Amount)
		return primary.Bid(ctx, req)
	}

	// Apply bid to local state (primary only)
	outcome, err := n.state.ApplyBid(req.Bidder, req.Amount)
	if outcome != "success" {
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		log.Printf("[Node %d] Bid rejected locally: outcome=%s error=%s", n.cfg.ID, outcome, errMsg)
		return &proto.BidResponse{Outcome: outcome, Error: errMsg}, nil
	}

	// Check if auction is closed after Local Apply
	closed := n.state.isOver()
	log.Printf("[Node %d] Bid accepted locally. Closed state=%v", n.cfg.ID, closed)

	// Replication to peers
	acks := 1 // primary counts as an ack
	for addr, c := range n.peerClients {

		// Skip forwarding to itself
		if addr == n.cfg.Address {
			continue
		}

		log.Printf("[Node %d] Sending replication → %s (bidder=%s amount=%d closed=%v)",
			n.cfg.ID, addr, req.Bidder, req.Amount, closed)

		ack, err := c.Replicate(ctx, &proto.ReplicationRequest{
			Bidder: req.Bidder,
			Amount: req.Amount,
			Closed: closed,
		})

		if err != nil {
			log.Printf("[Node %d] Replication failure to %s: %v", n.cfg.ID, addr, err)
			continue
		}

		if ack.Ok {
			log.Printf("[Node %d] Replication acknowledged by %s", n.cfg.ID, addr)
			acks++
		} else {
			log.Printf("[Node %d] Replication rejected by %s", n.cfg.ID, addr)
		}
	}

	// Check quorum
	totalNodes := len(n.peerClients) + 1 // + primary
	required := (totalNodes / 2) + 1

	if acks < required {
		log.Printf("[Node %d] Replication quorum NOT met (%d/%d). Rejecting commit.", n.cfg.ID, acks, required)
		return &proto.BidResponse{
			Outcome: "exception",
			Error:   "replication failed",
		}, nil
	}

	log.Printf("[Node %d] Replication QUORUM MET (%d/%d). Commit confirmed.", n.cfg.ID, acks, required)

	return &proto.BidResponse{Outcome: "success"}, nil
}


func (n *Node) Result(ctx context.Context, req *proto.ResultRequest) (*proto.ResultResponse, error) {
	return n.state.GetResult(), nil
}

func (n *Node) Replicate(ctx context.Context, req *proto.ReplicationRequest) (*proto.ReplicationAck, error) {
	log.Printf(
		"[Node %d] Replication received -> bidder=%s amount=%d closed=%v",
		n.cfg.ID, req.Bidder, req.Amount, req.Closed,
	)

	// Apply bid if present
	if req.Amount > 0 {
		_, err := n.state.ApplyBid(req.Bidder, req.Amount)
		if err != nil {
			log.Printf("[Node %d]  Replicated bid rejected: %v", n.cfg.ID, err)
			return &proto.ReplicationAck{Ok: false}, nil
		}
	}

	// Apply closure ONLY if node is NOT primary
	if req.Closed && !n.cfg.IsPrimary {
		n.state.ForceClose()
		log.Printf("[Node %d]  Auction closed (replicated from primary)", n.cfg.ID)
	}

	return &proto.ReplicationAck{Ok: true}, nil
}



