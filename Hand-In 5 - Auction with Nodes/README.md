 To run the distributed auction system, open three separate terminals and start one node in each. First, start the primary node by running:
go run ./cmd -id=1 -addr=:5001 -primary=true -peers=:5002,:5003 -duration=2000.
Then start the two replica nodes using:
go run ./cmd -id=2 -addr=:5002 -primary-addr=:5001 -peers=:5001,:5003 and
go run ./cmd -id=3 -addr=:5003 -primary-addr=:5001 -peers=:5001,:5002.
Once all three nodes are running, the system is ready to accept client requests. In a separate terminal, you can place bids or query the auction state using the client application. For example, to submit a bid, run:
go run ./client -addr=:5002 bid juan 50, and to check the current result, run:
go run ./client -addr=:5003 result.
You may send requests to any node; bids will automatically be forwarded to the primary and replicated to the backups. The auction remains open for the duration specified (in this example, 2000 seconds), after which further bids will be rejected and calls to result will return the final winner.