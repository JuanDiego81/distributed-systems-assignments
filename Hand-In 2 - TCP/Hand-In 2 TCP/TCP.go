// Easy solution
package main

import (
	"fmt"
	"math/rand"
	"time"
)

// Packet simulates a TCP packet
type Packet struct {
	clientName string
	seq        int
	ack        int
	packetType string // "SYN", "SYN+ACK", "ACK"
}

func main() {
	rand.Seed(time.Now().UnixNano())

	numClients := 5

	// Shared channels
	clientToServer := make(chan Packet)
	serverToClient := make(chan Packet)

	// Launch the server
	go server(clientToServer, serverToClient)

	// Launch multiple clients
	for i := 1; i <= numClients; i++ {
		clientName := fmt.Sprintf("Client %d", i)
		go client(clientName, clientToServer, serverToClient)
	}

	time.Sleep(5 * time.Second) // wait for all handshakes
}

// Server function
func server(clientToServer, serverToClient chan Packet) {
	handshakeDone := make(map[string]bool)

	for {
		p := <-clientToServer

		// Skip clients who completed handshake
		if handshakeDone[p.clientName] {
			continue
		}

		// Handle packet types
		if p.packetType == "SYN" {
			// Step 2: received SYN
			fmt.Println("[Server] Received SYN from", p.clientName, "seq:", p.seq)

			// Generate server ISN
			serverISN := rand.Intn(4001) + 1000

			// Step 3: send SYN+ACK
			serverToClient <- Packet{
				clientName: p.clientName,
				seq:        serverISN,
				ack:        p.seq + 1,
				packetType: "SYN+ACK",
			}
			fmt.Println("[Server] Sent SYN+ACK to", p.clientName, "seq:", serverISN, "ack:", p.seq+1)

		} else if p.packetType == "ACK" {
			// Step 6: receive final ACK
			fmt.Println("[Server] Received final ACK from", p.clientName, "ack:", p.ack)
			fmt.Println("[Server] Handshake complete with", p.clientName, "!")
			handshakeDone[p.clientName] = true
		}
	}
}

// Client function
func client(clientName string, clientToServer, serverToClient chan Packet) {
	// Step 1: send SYN
	clientSYN := rand.Intn(4001) + 1000
	clientToServer <- Packet{
		clientName: clientName,
		seq:        clientSYN,
		packetType: "SYN",
	}
	fmt.Println(clientName, "Sent SYN seq:", clientSYN)

	// Step 4: receive SYN+ACK
	synAck := <-serverToClient
	fmt.Println(clientName, "Received SYN+ACK seq:", synAck.seq, "ack:", synAck.ack)

	// Step 5: send final ACK
	clientToServer <- Packet{
		clientName: clientName,
		seq:        clientSYN + 1,
		ack:        synAck.seq + 1,
		packetType: "ACK",
	}
	fmt.Println(clientName, "Sent final ACK ack:", synAck.seq+1)
	fmt.Println(clientName, "Handshake complete, ready to send data!")
}






	

