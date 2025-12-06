package main

import (
	"fmt"
	"time"
)

func main() {
	// Channels for fork requests
	fork1Req := make(chan chan string)
	fork2Req := make(chan chan string)
	fork3Req := make(chan chan string)
	fork4Req := make(chan chan string)
	fork5Req := make(chan chan string)

	// Start fork goroutines
	go fork("fork1", fork1Req)
	go fork("fork2", fork2Req)
	go fork("fork3", fork3Req)
	go fork("fork4", fork4Req)
	go fork("fork5", fork5Req)

	// Start philosopher goroutines
	go philo("Philo 1", fork1Req, fork2Req)
	go philo("Philo 2", fork2Req, fork3Req)
	go philo("Philo 3", fork3Req, fork4Req)
	go philo("Philo 4", fork4Req, fork5Req)
	go philo("Philo 5", fork5Req, fork1Req) 

	// Keep main alive long enough
	time.Sleep(20 * time.Second)
}

// fork goroutine: manages a single fork
func fork(name string, requests chan chan string) {
	for {
		replyChan := <-requests          // wait for philosopher request
		replyChan <- name                // give the fork to philosopher
		fmt.Println(name, "is now on the table") // fork is available again after philosopher finishes
	}
}

// philosopher goroutine
func philo(name string, leftForkReq, rightForkReq chan chan string) {
	for i := 0; i < 3; i++ { // eat 3 times
		fmt.Println(name, "is thinking")
		time.Sleep(300 * time.Millisecond)

		if name == "Philo 5" {   // this breaks the deadlock as it breaks the cycle
			
			rightReply := make(chan string)
			leftReply := make(chan string)

			fmt.Println(name, "wants right fork")
			rightForkReq <- rightReply
			fmt.Println(name, "wants left fork")
			leftForkReq <- leftReply

			right := <-rightReply
			left := <-leftReply

			fmt.Println(name, "is eating with", left, "and", right)
			time.Sleep(500 * time.Millisecond)

			fmt.Println(name, "finished eating and is thinking again")
			time.Sleep(200 * time.Millisecond)

		} else {
			
			leftReply := make(chan string)
			rightReply := make(chan string)

			fmt.Println(name, "wants left fork")
			leftForkReq <- leftReply
			fmt.Println(name, "wants right fork")
			rightForkReq <- rightReply

			left := <-leftReply
			right := <-rightReply

			fmt.Println(name, "is eating with", left, "and", right)
			time.Sleep(500 * time.Millisecond)

			fmt.Println(name, "finished eating and is thinking again")
			time.Sleep(200 * time.Millisecond)
		}
	}
}



