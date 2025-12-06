# Chit Chat
In this assignment, you will design and implement **Chit Chat**, a distributed chat service where participants can join, exchange messages, and leave the conversation at any time. Chit Chat is a lively playground for exploring the essence of distributed systems: communication, coordination, and the ordering of events in a world without a single shared clock.

## System Specification
The system must satisfy the following specifications:

1. **Chit Chat is a distributed service** that enables clients to exchange chat messages using gRPC for all communication. Students must design the gRPC API, including all service methods and message types.
2. **Distributed topology:** The Chit Chat system must follow a distributed topology consisting of one service process and multiple client processes. Each client runs as an independent process that communicates with the service via gRPC. A minimal configuration must include one service instance and at least three concurrently active clients.
3. **Message publishing:** Each participant can publish a valid chat message at any time. A valid message is a UTF-8 encoded string with a maximum length of 128 characters. Publishing is performed through a gRPC call to the Chit Chat service.
4. **Message broadcasting:** The Chit Chat service must broadcast each published message to all currently active participants. Each broadcast must include the message content and a logical timestamp.
5. **Participant join:** Participants may join the system at any time. When a new participant X joins, the service must broadcast a message of the form:
   > "Participant X joined Chit Chat at logical time L"
   This message must be delivered to all participants, including the newly joined one.
6. **Participant leave:** Participants may leave the system at any time. When a participant X leaves, the service must broadcast a message of the form:
   > "Participant X left Chit Chat at logical time L"
   This message must be delivered to all remaining participants.
7. **Message handling:** When a participant receives any broadcast message, it must:
   - Display the message content and its logical timestamp on the client interface.
   - Log the message content and its logical timestamp.

## Technical Requirements
- The system must be implemented in Go.
- The gRPC framework must be used for client–server communication, using Protocol Buffers (you must provide a `.proto` file) for message definitions.
- The Go `log` standard library must be used for structured logging of events (e.g., client join/leave notifications, message delivery, and server startup/shutdown).
- Concurrency (local to the server or the clients) must be handled using Go routines and channels for synchronized communication between components.
- Every client and the server must be deployed as separate processes.
- Each client connection must be served by a dedicated goroutine managed by the server.
- The system must support multiple concurrent client connections without blocking message delivery.
- The system must log the following events:
  - Server startup and shutdown
  - Client connection and disconnection events
  - Broadcast of join/leave messages
  - Message delivery events
- Log messages must include:
  - Timestamp
  - Component name (Server/Client)
  - Event type
  - Relevant identifiers (e.g., Client ID).
- The system can be started with at least three (3) nodes (two clients and a server), and it must be able to handle "join" of at least one client and "leave" of at least one client.

## Hand-in Requirements
- You must hand in a report (single PDF file) via LearnIT.
- You must provide a link to a Git repo with your source code in the report.
- In the report, you must:
  - Discuss whether you are going to use server-side streaming, client-side streaming, or bidirectional streaming.
  - Describe your system architecture - do you have a server-client architecture, peer-to-peer, or something else?
  - Describe what RPC methods are implemented, of what type, and what message types are used for communication.
  - Describe how you have implemented the calculation of the timestamps.
  - Provide a diagram that traces a sequence of RPC calls together with the Lamport timestamps, corresponding to a chosen sequence of interactions: Client X joins, Client X publishes, ..., Client X leaves.
- You must include system logs that document the requirements are met, in both the appendix of your report and your repo.
- Your repo must include a `README.md` file that describes how to run your program.
- Your repo must be structured as follows:
```
project-root/
├── client/    # contains the client code
├── grpc/      # contains .proto file
├── server/    # contains the server code
└── README.md  # readme file
```

