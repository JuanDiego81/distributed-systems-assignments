package main

import (
	proto "ChitChat/grpc"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to server
	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewChitChatClient(conn)
	stream, err := client.Chat(context.Background())  // the stream is an open communication channel between client and server
	if err != nil {
		log.Fatalf("Error starting chat: %v", err)
	}

	// Ask for username
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter your name: ")
	name, _ := reader.ReadString('\n')
	name = strings.TrimSpace(name)

	// Send join message
	stream.Send(&proto.ChatMessage{   // this sends the message to the server and then the server sends it to the rest of clients
		Sender:      name,
		Type:        "join",
		LogicalTime: time.Now().Format(time.Stamp),
	})

	fmt.Printf("[INFO] Connected to Chit Chat as %s\n", name)
	fmt.Println("Type your message and press Enter to send. Type /exit to leave.")

	// Goroutine for receiving messages
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error receiving: %v", err)
			}

			switch in.Type {
			case "system":
				// System messages (join/leave)
				fmt.Printf("[%s] %s\n", in.LogicalTime, in.Content)
			case "message":
				// Normal user messages
				fmt.Printf("[%s] %s (%s): %s\n", in.LogicalTime, in.Sender, in.Id, in.Content)
		}
	}
}()

	// Main goroutine for sending messages
	for {
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)

		if text == "/exit" {
			stream.Send(&proto.ChatMessage{
				Sender:      name,
				Type:        "leave",
				LogicalTime: time.Now().Format(time.Stamp),
			})
			fmt.Println("[INFO] You left the chat.")
			break
		}

		if text == "" {
			continue
		}

		stream.Send(&proto.ChatMessage{
			Sender:      name,
			Content:     text,
			Type:        "message",
			LogicalTime: time.Now().Format(time.Stamp),
		})
	}
}

