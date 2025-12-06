package main

import (
	proto "Auction/grpc"
	"context"
	"flag"
	"fmt"
	// "log"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", ":5001", "")  // to read server from the command lines, lets you choose which node you talk to.
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("bid <name> <amount> or result")
		return
	}

	conn, _ := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client :=proto.NewAuctionServiceClient(conn)

	cmd := flag.Arg(0)   // to get the input from user
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	switch cmd {
	case "bid":
		name := flag.Arg(1)   // to caputrue name of bidder
		amt, _ := strconv.Atoi(flag.Arg(2))    // capture amoung
		res, _ := client.Bid(ctx, &proto.BidRequest{Bidder: name, Amount: int32(amt)}) // response
		fmt.Println(res)

	case "result":
		res, _ := client.Result(ctx, &proto.ResultRequest{})    // get response
		fmt.Println(res)
	}
}
