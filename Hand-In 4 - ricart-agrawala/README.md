# ds-handin-04

To start the system:

1) Open 3 terminals

2) Type go run ./cmd --id=1, go run ./cmd --id=2, go run ./cmd --id=3 in each terminal

3) Type 'req' or 'quit' to interact with the terminals and see how the algorithm works
It takes 5 seconds to send a request, during that time you can type 'req' in others terminals to send other requests
The algorithm will decide which node could run first based on the Lamport clock, smaller timestamp will enter CS first

4) 'Ctrl + C' or 'control + C' to shutdown the nodes