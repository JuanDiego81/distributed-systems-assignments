package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	proto "ChitChat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// define a method to find the maximum number (for logical clock)
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter your name: ")
	name, _ := reader.ReadString('\n')
	name = strings.TrimSpace(name)
	if name == "" {
		name = fmt.Sprintf("anonymous-%d", time.Now().Unix()%10000)
	}

	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Client][ERR] %v", err)
	}
	defer conn.Close()

	client := proto.NewChitChatClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Chat(ctx)
	if err != nil {
		log.Fatalf("[Client][STREAM_OPEN_ERR] %v", err)
	}

	var (
		lc      uint64 = 0 // client-side Lamport clock
		exiting bool
	)

	// send join (Lamport local event before sending)
	lc++
	_ = stream.Send(&proto.ChatMessage{
		Sender:      name,
		Type:        "join",
		LogicalTime: lc,
	})

	// receive
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if !exiting {
					fmt.Printf("[ERR] recv: %v\n", err)
				}
				return
			}

			// Lamport receive: only update clock for actual messages from other participants
			if in.Type == "message" && in.Sender != name {
				lc = max(lc, in.LogicalTime) + 1
			}

			switch in.Type {
			case "system":
				fmt.Printf("%s\n", in.Content)
			case "message":
				fmt.Printf("%s (%s): %s\n", in.Sender, in.Id, in.Content)
			default:
				// ignore
			}
		}
	}()

	// send message
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Type your messages. Use /exit to leave.")
	for scanner.Scan() {
		//Validate if string is UTF-8 and check length is below 128 characters
		txt := strings.TrimSpace(scanner.Text())
		if !utf8.ValidString(txt) {
			fmt.Println("[ERR] Message not in valid UTF-8 format. Please try again.")
			continue
		}
		if len(txt) > 128 {
			fmt.Println("[ERR] Message exceeds 128 characters. Please try again.")
			continue
		}
		if txt == "" {
			continue
		}
		if txt == "/exit" {
			exiting = true
			lc++ // local event before leaving
			_ = stream.Send(&proto.ChatMessage{
				Sender:      name,
				Type:        "leave",
				LogicalTime: lc,
			})
			_ = stream.CloseSend()
			fmt.Println("[INFO] You left the chat.")
			break
		}

		lc++ // local event before sending message
		if err := stream.Send(&proto.ChatMessage{
			Sender:      name,
			Content:     txt,
			Type:        "message",
			LogicalTime: lc,
		}); err != nil {
			if !exiting {
				fmt.Printf("[ERR] send: %v\n", err)
			}
			break
		}
	}

	<-done

	if scanErr := scanner.Err(); scanErr != nil && !exiting {
		fmt.Printf("[ERR] scanner: %v\n", scanErr)
	}
}
