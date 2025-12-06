package node

import "errors"

var (
	ErrAuctionOver   = errors.New("auction is over")
	ErrInvalidAmount = errors.New("invalid bid amount")
	ErrBidNotHigher  = errors.New("bid must be higher than last bid")
)
