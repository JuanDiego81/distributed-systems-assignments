package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	node "ricart-agrawala/Node"
	server "ricart-agrawala/Server"
)


// Hardcoded service discovery (ID maps to address)
var cluster = map[int32]string{
	1: "127.0.0.1:5001",
	2: "127.0.0.1:5002",
	3: "127.0.0.1:5003",
}

func buildPeers(selfID int32) map[int32]string {
	peers := make(map[int32]string, len(cluster)-1)
	for id, addr := range cluster {
		if id != selfID {
			peers[id] = addr
		}
	}
	return peers
}

func keys(m map[int32]string) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func main() {
	// Choose which hardcoded node to run: go run ./cmd --id=1
	idFlag := flag.Int("id", 1, "Node ID (1..N) from hardcoded cluster")
	flag.Parse()

	selfID := int32(*idFlag)
	selfAddr, ok := cluster[selfID]
	if !ok {
		log.Fatalf("unknown node id %d; valid: %v", selfID, keys(cluster))
	}
	peers := buildPeers(selfID)

	// Logger with microseconds so logs can prove ordering
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	logger.Printf("node %d at %s; peers=%v", selfID, selfAddr, peers)

	// Construct node (Lamport + RA logic)
	n := node.NewNode(selfID, selfAddr, peers, 3*time.Second, logger)

	// Start gRPC server (listens on selfAddr)
	go func() {
		if err := server.Serve(n, selfAddr); err != nil {
			logger.Fatalf("gRPC serve error: %v", err)
		}
	}()

	// Interactive loop: type'req' to request CS, type 'quit' to exit
	go func() {
		in := bufio.NewScanner(os.Stdin)
		logger.Println("Press type 'req' to request CS. Type 'quit' then ENTER to exit.")
		for in.Scan() {
			cmd := strings.TrimSpace(in.Text())
			if cmd == "quit" {
				logger.Println("quitting loop")
				return
			}
			if cmd == "req" {
				logger.Println("requesting CS...")
				time.Sleep(5 * time.Second)
				n.EnterCS()
				n.Logf("In CS. Doing something")
				time.Sleep(3 * time.Second)
				n.ExitCS()
				logger.Println("done (released CS). Press type 'req' again or 'quit' to exit.")
				continue
			}
			logger.Printf("unknown command %q (use 'req', or 'quit')", cmd)
		}
		if err := in.Err(); err != nil {
			logger.Printf("stdin error: %v", err)
		}
	}()


	// Graceful shutdown on Control+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Println("shutting down")
	n.Close()
}
