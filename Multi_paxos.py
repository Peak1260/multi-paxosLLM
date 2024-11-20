# We will be implementing multi paxos algorithm in this file
from dataclasses import dataclass
from typing import Dict, Set, Tuple, Optional, List
from enum import Enum
import threading
import queue
import socket
import sys
import time

'''
Each node should keep track of the current leader of the system. If an acceptor receives an operation
on its command line interface, it should forward the operation to the leader. The leader should
respond back to the acceptor with an acknowledgement that the request has been received and place
the operation into its local queue. If this acceptor doesn’t receive an acknowledgement from the
leader, then it should time out and become a proposer to propose the operation. If the acceptor
receives an acknowledgement, then the acceptor no longer needs to remember the operation. If an
acceptor receives an operation and doesn’t know who the leader is, it should also become a proposer.
If another acceptor receives a forwarded operation, it should forward the operation to its known
leader. 

Multi-Paxos is an extension of Paxos where an existing leader who have already decided on a single
operation will stay as the leader for subsequent operations instead of starting a leader election for
every new entry.
A ballot number in Multi-Paxos should be a 3-tuple: <seq_num, pid, op_num>, where op_num
captures the number of consensus operations that have been executed (i.e. create a context, create
a query in a context, and save a selected answer). The flow for your implementation of the protocol
should look similar to the following:

1. Election Phase: A node intending to become the leader becomes a proposer and broadcasts
PREPARE messages with its ballot number to all acceptors.

2. A proposer must obtain a majority of PROMISE messages from acceptors for its ballot number
to become the leader

3. An acceptor should NOT reply PROMISE to a PREPARE if the acceptor’s number of operations
is larger than the op num from the ballot number

4. Replication/Normal Phase: Once a node becomes the leader, it should maintain a queue
of pending operations waiting for consensus from the system

5. The leader then broadcasts ACCEPT messages with the next pending operation to all acceptors

6. An acceptor should NOT reply ACCEPTED to an ACCEPT if the acceptor’s number of applied
operations is greater than the op num from the ballot number

7. Decision Phase: After a majority of acceptors reply ACCEPTED to the leader, the leader will
Remove the operation from its queue
    • Broadcast DECIDE messages to all nodes
    • Apply the appropriate operation (creating a new context, querying, or selecting an answer). 
    Note that the leader needs to also tell other nodes which server that the creating
    a query request originated from so that other nodes know where to forward the server
    response.

8. Upon receiving the DECIDE message from the leader, an acceptor will similarly apply the
appropriate operations.

9. The leader should start a new normal/replication phase for the next pending operation in its
queue with an updated ballot number

'''

# Network2.py



class NetworkSimulation:
    def __init__(self, base_port):
        self.base_port = base_port
        self.server_socket = None
        self.nodes = []
        self.node_ports = []

    def start_server(self):
        """Start the server to listen for incoming node connections."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', self.base_port))
        self.server_socket.listen(5)
        print(f"Server started on port {self.base_port}")
        
        # Accept connections from nodes
        while True:
            node_socket, node_address = self.server_socket.accept()
            print(f"New connection from {node_address}")
            
            # Assign a dynamic port for this node and send it back to the node
            assigned_port = self.base_port + len(self.nodes) + 1  # Assign a unique port
            self.node_ports.append(assigned_port)
            node_socket.sendall(str(assigned_port).encode('utf-8'))
            print(f"Assigned port {assigned_port} to new node")
            
            self.nodes.append(node_socket)
            threading.Thread(target=self.handle_node, args=(node_socket, assigned_port)).start()

    def handle_node(self, node_socket, assigned_port):
        """Handle communication with a single node."""
        try:
            while True:
                message = node_socket.recv(1024).decode('utf-8')

                
                if not message:
                    break
                print(f"Received: {message}\n")
                
                # Relay the message to all other nodes
                for other_node_socket in self.nodes:
                    if other_node_socket != node_socket:
                        other_node_socket.sendall(message.encode('utf-8'))
                
        finally:
            node_socket.close()
        
        def send_message(self, to_pid, message):
            if to_pid < len(self.nodes):
                self.nodes[to_pid].sendall(message.encode('utf-8'))

    def start_node(self):
        """Start a node that connects to the server."""
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect(('localhost', self.base_port))
        print(f"Node connected to server on port {self.base_port}")
        
        # Receive the assigned port from the server
        assigned_port = int(node_socket.recv(1024).decode('utf-8'))
        print(f"Node assigned port {assigned_port}")

        threading.Thread(target=self.handle_node, args=(node_socket, assigned_port)).start() # --------------------------

        # Send a test message to the server
        while True:
            message = input(f"Node {assigned_port} - Enter message: ")
            if message.lower() == 'exit':
                break
            node_socket.sendall(message.encode('utf-8'))

        node_socket.close()

# ----------------- Multi-Paxos Implementation -----------------


class MessageType(Enum):
    PREPARE = "PREPARE"
    PROMISE = "PROMISE"
    ACCEPT = "ACCEPT"
    ACCEPTED = "ACCEPTED"
    DECIDE = "DECIDE"


@dataclass
class BallotNumber:
    seq_num: int
    pid: int
    op_num: int

    def __lt__(self, other):
        return (self.seq_num, self.pid, self.op_num) < (other.seq_num, other.pid, other.op_num)
    
class PaxosNode:
    def __init__(self, pid: int, network: NetworkSimulation, num_nodes: int):
        self.pid = pid
        self.network = network
        self.num_nodes = num_nodes
        
        # Everyone should have AcceptNum, AcceptVal, BallotNum, Leader
        self.accept_num = None
        self.accept_val = None
        self.current_operation_num = 0
        self.ballot_num = BallotNumber(0, self.pid, 0) # Initial Ballot Number
        # self.isleader = False
        self.who_is_leader = None

        # Leader should have a queue of pending operations
        self.pending_operations = queue.Queue() # only the leaader has this

        self.accepted_operations: List[Tuple[BallotNumber, int]] = [] # (ballot_num, op_num)

        self.node_ports = [9001,9002,9003]
        self.server_port = 9000

    def handle_client_request(self, operation):
        # If the node is not the leader, forward the operation to the leader
        if self.who_is_leader != self.pid:
            self.network.send_message(self.pid, operation)
        else:
            # If the node is the leader, add the operation to the pending operations queue
            self.pending_operations.put(operation)
            self.propose_operation()
    
    def start_election(self):
        self.ballot_num = BallotNumber(self.ballot_num.seq_num + 1, self.pid, self.current_operation_num)
        
        prepare_message = (MessageType.PREPARE, self.ballot_num)
        self.send_prepare(prepare_message) # make this into its own thread, TODO
   
    def send_prepare(self, prepare_message):
        for i in range(self.num_nodes):
            if i != self.pid:
                self.network.send_message(self.pid, i, prepare_message)
            
                
def main():
    if len(sys.argv) != 3:
        print("Usage: python network_simulation.py <mode> <base_port>")
        sys.exit(1)

    mode = sys.argv[1]  # 'server' or 'node'
    base_port = int(sys.argv[2])

    simulation = NetworkSimulation(base_port)

    if mode == 'server':
        simulation.start_server()
    elif mode == 'node':
        simulation.start_node()
    else:
        print("Invalid mode. Use 'server' or 'node'.")
        sys.exit(1)

if __name__ == "__main__":
    main()















