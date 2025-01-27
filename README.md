# Multi-Paxos with Gemini API Integration

This project is an implementation of the Multi-Paxos consensus algorithm in Python using socket programming. It simulates a distributed system that ensures fault tolerance and consistency across clients, integrated with the Gemini API to provide reliable answers to user queries.

## Features
- **Multi-Paxos Consensus Algorithm**: Ensures agreement on a single value across distributed nodes.
- **Fault Tolerance**: Handles failures gracefully, ensuring the system remains consistent.
- **Gemini API Integration**: Fetches answers to client queries and shares the consensus-based response with all clients.
- **Server-Client Architecture**: Implements a distributed system where the server acts as the leader and clients are proposers/acceptors.
- **Uniform State Sharing**: Guarantees that all clients receive the same response once consensus is reached.

## How It Works
1. **Input Query**: A client inputs a question or request.
2. **Consensus Process**:
   - The client sends the input to the server, which runs the Multi-Paxos algorithm.
   - Proposals are shared among all nodes (clients) for agreement.
3. **Gemini API Query**:
   - Once consensus is reached, the server queries the Gemini API for an answer.
   - The response is validated and shared across all clients.
4. **Fault Tolerance**: If a client or server fails, the system continues to operate, ensuring consistency among the remaining nodes.

## Architecture

### Components
- **Server**: Acts as the leader to manage consensus and communicate with the Gemini API.
- **Clients**: Participate in the consensus process and receive the agreed-upon response.
- **Gemini API**: Provides the answer to the consensus-approved query.

### Workflow
1. A client submits a query.
2. The server initiates the Multi-Paxos process to reach consensus.
3. Upon consensus, the server fetches the response from the Gemini API.
4. The server broadcasts the API response to all clients.

## Installation

### Prerequisites
- Python 3.x
- Access to the Gemini API (API key is required)

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/multi-paxos-gemini.git
   cd multi-paxos-gemini
