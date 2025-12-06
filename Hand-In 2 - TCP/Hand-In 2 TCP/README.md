a. What are packets in your implementation? What data structure do you use to transmit data and meta-data?
Answer:
In this simulation, a packet is represented by the Go struct type Packet.
It contains the following fields:

* clientName: to identify which client sent the packet.

* seq: the sequence number of the packet.

* ack: the acknowledgement number.

* packetType: specifies the kind of packet (SYN, SYN+ACK, or ACK).

Packets are transmitted between clients and the server using Go channels. Channels act as the communication medium, simulating how packets would be transmitted across a network.


b. Does your implementation use threads or processes? Why is it not realistic to use threads?
Answer:
Our implementation uses threads (goroutines in Go). This is not fully realistic because, in real TCP communication, messages are exchanged between different machines over a network. Using threads in a single program only simulates concurrency, but it does not capture the real-world challenges of distributed systems, such as network delays, message loss, reordering, or independent process failures.


c. In case the network changes the order in which messages are delivered, how would you handle message re-ordering?
Answer:
In this simulation, we assume packets arrive in order.

If we wanted to handle packets arriving out of order like in a real network, we could:

1. Keep incoming packets in a temporary buffer.

2. Check each packet’s sequence number against the one we expect.

3. Only process packets that are in the right order.

4. Hold onto or put back any out-of-order packets until the missing ones arrive.

5. That’s basically how TCP makes sure everything comes in the correct order.


d. In case messages can be delayed or lost, how does your implementation handle message loss?
Answer: 
Our simulation does not handle message lost, it assumes packets always arrive.

A realistic way to handle message lost is:

* If an ACK is not received within a timeout period, the sender would retransmit the packet.

* TCP uses timeouts and retransmission mechanisms to ensure reliable delivery.

* Sequence and acknowledgement numbers allow the receiver to detect missing or duplicated packets.

e. Why is the 3-way handshake important?

* It ensures client and server are ready to communicate
* It syncs the sequence numbers so both sides know where to start sending data and nothing gets lost.
* It prevents old or duplicate connections from being confused with new ones.

