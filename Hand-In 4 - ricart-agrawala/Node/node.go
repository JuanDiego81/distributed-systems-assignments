package node

import (
	"fmt"
	"log"
	"sync"
	"time"

	client "ricart-agrawala/Client"
)

// RA states
const (
	StateReleased = "RELEASED"
	StateWanted   = "WANTED"
	StateHeld     = "HELD"
)

// Node holds RA state + networking handles 
type Node struct {
	ID   int32
	Addr string

	mu     sync.Mutex
	Clock  int32             // Lamport logical clock
	State  string            // RELEASED/WANTED/HELD
	ReqTS  int32             // timestamp of our latest REQUEST
	Peers  map[int32]string  // id -> address

	deferred map[int32]struct{} // set of peers we owe a REPLY to

	repliesNeeded int
	repliesCh     chan struct{} // counts incoming REPLYs
	cm            *client.ClientManager
	logger        *log.Logger
}

// Constructor: inject identity, peers, and a client manager
// 'peers' must exclude self. 'timeout' sets per-RPC deadline.
func NewNode(id int32, addr string, peers map[int32]string, timeout time.Duration, logger *log.Logger) *Node {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	n := &Node{
		ID:       id,
		Addr:     addr,
		Clock:    0,
		State:    StateReleased,
		Peers:    make(map[int32]string, len(peers)),
		deferred: make(map[int32]struct{}),
		cm:       client.NewClientManager(timeout, nil),
		logger:   logger,
	}
	for pid, paddr := range peers {
		if pid != id {
			n.Peers[pid] = paddr
		}
	}
	return n
}

// Logf: unified node logger with context (id/clock/state)
func (n *Node) Logf(format string, args ...any) {
	prefix := fmt.Sprintf("[node %d lamport-clock=%d state=%s] ", n.ID, n.Clock, n.State)
	msg := prefix + fmt.Sprintf(format, args...)
	if n.logger != nil {
		n.logger.Println(msg)
	} else {
		log.Println(msg)
	}
}

// Lamport helpers
// onReceive: max(clock, ts) + 1   (used when receiving REQUEST)
// onSend: clock++ when send and return it (even if REPLY has no timestamp, the clock still advances locally)
func (n *Node) onReceive(ts int32) {
	n.mu.Lock()
	if n.Clock < ts {
		n.Clock = ts
	}
	n.Clock++
	n.mu.Unlock()
}

func (n *Node) onSend() int32 {
	n.mu.Lock()
	n.Clock++
	ts := n.Clock
	n.mu.Unlock()
	return ts
}

// RA ordering: (ts, id) lower wins because of longer wait
func lessPair(myTS int32, myID int32, otherTS int32, otherID int32) bool {
	if myTS < otherTS {
		return true
	}
	if myTS > otherTS {
		return false
	}
	return myID < otherID
}

// EnterCS: broadcast REQUEST and wait for (N-1) replies
func (n *Node) EnterCS() {
    // 1) Switch to WANTED 
    n.mu.Lock()
    n.State = StateWanted
    n.mu.Unlock()

    // 2) Increase Lamport before sending (outside lock to avoid deadlock)
    reqTS := n.onSend()

    // 3) Print Request timestamp + prepare reply counter
    n.mu.Lock()
    n.ReqTS = reqTS
    n.repliesNeeded = len(n.Peers)
    n.repliesCh = make(chan struct{}, n.repliesNeeded)
    n.mu.Unlock()

    n.Logf("WANTS CS (ReqTimestamp=%d); broadcasting REQUEST to %d peers", reqTS, n.repliesNeeded)

    // 4) Broadcast REQUEST to peers
    addrs := make([]string, 0, len(n.Peers))
    for _, addr := range n.Peers {
        addrs = append(addrs, addr)
    }
    _ = n.cm.BroadcastRequest(addrs, n.ID, reqTS)

    // 5) Wait for (N-1) replies
    for i := 0; i < n.repliesNeeded; i++ {
        <-n.repliesCh
    }

    // 6) Enter HELD
    n.mu.Lock()
    n.State = StateHeld
    n.mu.Unlock()
    n.Logf("ENTER CS")
}

// ExitCS: RELEASED and send REPLY to any deferred requests.
// call onSend() before each REPLY to increase our local Lamport clock,
func (n *Node) ExitCS() {
	// Snapshot and clear deferred set
	n.mu.Lock()
	n.State = StateReleased
	toReply := make([]int32, 0, len(n.deferred))
	for pid := range n.deferred {
		toReply = append(toReply, pid)
	}
	n.deferred = make(map[int32]struct{})
	n.mu.Unlock()

	n.Logf("EXIT CS; replying to %d deferred request(s)", len(toReply))

	// Send REPLY to all deferred requesters
	for _, pid := range toReply {
		addr, ok := n.Peers[pid]
		if !ok {
			n.Logf("WARN: no address for peer %d (cannot send deferred REPLY)", pid)
			continue
		}
		_ = n.onSend() // advance local clock (not transmitted)
		if _, err := n.cm.SendReply(addr, n.ID); err != nil {
			n.Logf("ERROR sending deferred REPLY to %d@%s: %v", pid, addr, err)
		} else {
			n.Logf("Sent deferred REPLY to %d@%s", pid, addr)
		}
	}
}

// HandleIncomingRequest: RA decision (defer vs immediate reply) 
// On REQUEST(Ti, pi):
//  - onReceive(Ti), update lamport
//  - If HELD or (WANTED && our (ReqTS,ID) has higher priority) -> defer
//  - Else send immediate REPLY
func (n *Node) HandleIncomingRequest(fromID int32, timestamp int32) {
	// Lamport receive update
	n.onReceive(timestamp)

	// Read current state safely
	n.mu.Lock()
	state := n.State
	myTS := n.ReqTS
	myID := n.ID
	n.mu.Unlock()

	deferAction := false
	if state == StateHeld {
		deferAction = true
	} else if state == StateWanted && lessPair(myTS, myID, timestamp, fromID) {
		// We have priority -> defer other's request
		deferAction = true
	}

	if deferAction {
		n.mu.Lock()
		n.deferred[fromID] = struct{}{}
		n.mu.Unlock()
		n.Logf("REQ from %d(ts=%d) DEFERRED (state=%s, myReqTS=%d)", fromID, timestamp, state, myTS)
		return
	}

	// Immediate REPLY
	addr, ok := n.Peers[fromID]
	if !ok {
		n.Logf("WARN: REQ from %d but no address in Peers", fromID)
		return
	}
	_ = n.onSend() // advance local clock (not transmitted)
	if _, err := n.cm.SendReply(addr, n.ID); err != nil {
		n.Logf("ERROR sending immediate REPLY to %d@%s: %v", fromID, addr, err)
	} else {
		n.Logf("Sent immediate REPLY to %d@%s", fromID, addr)
	}
}

//
// HandleIncomingReply: count replies (no timestamp available) 
func (n *Node) HandleIncomingReply(fromID int32) {
	// Non-blocking because channel is buffered with repliesNeeded capacity
	select {
	case n.repliesCh <- struct{}{}:
		n.Logf("REPLY received from %d (progress %d/%d)",
			fromID,
			len(n.repliesCh),
			n.repliesNeeded,
		)
	default:
		// Duplicate or late reply (e.g., after ExitCS); ignore safely.
		n.Logf("REPLY from %d ignored (not waiting)", fromID)
	}
}

// Graceful close of client connections 
func (n *Node) Close() {
	if err := n.cm.Close(); err != nil {
		n.Logf("client manager close error: %v", err)
	}
}
