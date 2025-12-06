package main

import (
	// proto "Auction/grpc"
	"Auction/node"
	"flag"
	"strings"
	"time"
)

func main() {
	id := flag.Int("id", 1, "")   // node id
	addr := flag.String("addr", ":5001", "")  // where the noe listes
	primary := flag.Bool("primary", false, "")    // whether this node is the primary (leader)
	primaryAddr := flag.String("primary-addr", ":5001", "")   // Where backups forward bid requests
	peersStr := flag.String("peers", "", "")    // Other nodes to communicate with
	duration := flag.Int("duration", 20, "")   // How long the auction stays open

	flag.Parse()   // read an applies copmmand line arguments

	peers := []string{}
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")   // to create the slices of the peers
	}

	cfg := node.Config{
		ID:          *id,
		Address:     *addr,
		IsPrimary:   *primary,
		PrimaryAddr: *primaryAddr,
		Peers:       peers,
		Duration:    time.Duration(*duration) * time.Second,
	}

	node.NewNode(cfg).Start()
}
