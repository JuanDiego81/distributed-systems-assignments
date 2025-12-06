package node

import (
	"sync"
	"time"

	proto "Auction/grpc"
)

type AuctionState struct {
	mu sync.Mutex     // one goroutine handle at a time

	start      time.Time    // timestamp when aution begin
	duration   time.Duration   // auction lifetime
	closed     bool     
	highestBid int32
	highestBidder string

	registered map[string]bool      // tracks who has placed a bid
	lastBid    map[string]int32		// Stores each bidder’s most recent bid for enforcing increasing value rule.
}

func NewAuctionState(duration time.Duration) *AuctionState {    // constructor
	return &AuctionState{
		start:      time.Now(),
		duration:   duration,
		registered: make(map[string]bool),
		lastBid:    make(map[string]int32),
	}
}

func (a *AuctionState) isOver() bool {
	return a.closed
}

func (a *AuctionState) CheckTimeout() bool {
	return time.Since(a.start) >= a.duration
}


func (a *AuctionState) ApplyBid(bidder string, amount int32) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Reject if closed
	if a.closed {
		return "fail", ErrAuctionOver
	}

	// Reject empty or invalid numbers
	if amount <= 0 {
		return "exception", ErrInvalidAmount
	}

	// First time bidder is registered
	if !a.registered[bidder] {
		a.registered[bidder] = true
	}

	// The rule: bid must be higher than the CURRENT highest bid overall
	if amount <= a.highestBid {
		return "fail", ErrBidNotHigher // or "Bid must beat the current highest!"
	}

	// Record latest bid from bidder
	a.lastBid[bidder] = amount

	// Update global leader
	a.highestBid = amount
	a.highestBidder = bidder

	return "success", nil
}



func (a *AuctionState) GetResult() *proto.ResultResponse {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.isOver() {
		return &proto.ResultResponse{
			Over:          true,
			HighestBidder: a.highestBidder,
			HighestBid:    a.highestBid,
			Message:       "auction finished",
		}
	}

	return &proto.ResultResponse{
		Over:          false,
		HighestBidder: a.highestBidder,
		HighestBid:    a.highestBid,
		Message:       "auction still going",
	}
}

func (a *AuctionState) ForceClose() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
}
