# Distributed Systems — Hand-Ins

Assignments from the Distributed Systems course at ITU, implemented in Go. Each hand-in explores a different core problem in distributed computing — mutual exclusion, unreliable transport, RPC, coordination, replication, and consensus — through a small working system rather than just theory. Every folder below has its own README with setup/run instructions.

## Contents

| # | Hand-in | Topic |
|---|---------|-------|
| 1 | [Dining Philosophers](./Hand-In%201%20Dining%20Philosophers) | Classic deadlock problem solved with goroutines and channels; one philosopher picks up forks in reverse order to break the circular-wait condition. |
| 2 | [TCP](./Hand-In%202%20-%20TCP) | A simulated TCP handshake and packet exchange over Go channels, plus a written discussion of ordering, reliability, and the 3-way handshake. |
| 3 | [gRPC Chat](./Hand-In%203%20-%20GRPC) | A multi-client chat server built with gRPC and Protocol Buffers, using bidirectional streaming for message broadcast. |
| 4 | [Ricart–Agrawala](./Hand-In%204%20-%20ricart-agrawala) | Distributed mutual exclusion via the Ricart–Agrawala algorithm, using Lamport clocks to order critical-section requests across peer nodes over gRPC. |
| 5 | [Auction with Nodes](./Hand-In%205%20-%20Auction%20with%20Nodes) | A replicated auction service with a primary node and backup replicas; bids are forwarded to the primary and replicated to backups for fault tolerance. |

## Stack

- **Language:** Go
- **RPC/IPC:** gRPC + Protocol Buffers (Hand-Ins 3–5), raw TCP and channels (Hand-Ins 1–2)
- **Concepts covered:** deadlock avoidance, reliable transport, RPC, Lamport clocks & mutual exclusion, primary-backup replication, consensus (Raft)

## Running a hand-in

Each subfolder is an independent Go module. `cd` into it and follow its own README — most run as `go run ./cmd` or `go run Server/server.go` / `go run Client/client.go` in separate terminals to simulate multiple machines.
