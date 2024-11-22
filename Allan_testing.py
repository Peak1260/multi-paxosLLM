# NEED TO IMPLEMENT CENTRAL SERVER (9000) that receives all the gemini commands


import socket
import threading
import time

class Node:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id # ID of process 1 = 1, process 2 = 2, process 3 = 3
        self.port = port # Port of process 1 = 9001, process 2 = 9002, process 3 = 9003
        self.peers = peers  # List of (host, port) tuples for other nodes
        self.current_leader = 1 # Current leader of the network for right now is hardcoded to 1
        self.ballot_tuple = (0,node_id,0)  # (seq_num, pid, op_num)
        self.accepted_ballot_num = 0 # Highest ballot number accepted by the acceptor
        self.accepted_val_num = 0 # previous value accepted by the acceptor
        self.count_responses = 0 # ONLY LEADER USES THIS TO DETERMINE A MAJORITY, FOR NOW, ASSUME WE NEED 2
        self.queue = []
        self.contexts = {}

        # Initialize server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', self.port))
        self.server_socket.listen(5)
        
    def start(self):
        print(f"Node {self.node_id} started on port {self.port}")
        threading.Thread(target=self.listen).start()

    def listen(self):
        while True:
            conn, addr = self.server_socket.accept()
            print (f"Node {self.node_id} connected to {addr}")
            threading.Thread(target=self.handle_connection, args=(conn,)).start()

    def handle_connection(self, conn):
        try:
            data = conn.recv(1024).decode()
            if data:
                self.process_message(data)
        finally:
            conn.close()

    def process_message(self, message):
        # Implement logic to process PREPARE, PROMISE, ACCEPT, DECIDE, etc.
        print(f"Node {self.node_id} received: {message}")

        # if message.startswith("PREPARE"):
        # elif message.startswith("PROMISE"):

        # ------------------------------------------------------------
        if message.startswith("ACCEPT "):
            command = message.split(" ", 3)[3]
            print("command: ", command) 
            list_of_messages = message.split() 
            print("list_of_messages: ", list_of_messages) # list_of_messages:  ['ACCEPT', '0,', '0', 'create', '0']
            ballot = int(list_of_messages[1])
            operation_num = int(list_of_messages[2])
            self.handle_accept(ballot, operation_num, command) # acceptors handle this


        elif message.startswith("ACCEPTED"): # Leader handles this
            # Message looks like: ACCEPTED 0 3 create 0
            self.count_responses += 1 
            command = message.split(" ", 3)[3]
            
            # LOGIC FOR HANDLING ACCEPTED MESSAGES # ------------------------------------------------ TODO

            if self.current_leader == self.node_id:
                if self.count_responses == 2: # This is to ensure, decide is only done ONCE
                    self.count_responses = 0
                    self.decide_operation(command)
                else:
                    print("Not enough ACCEPTED messages received yet")
                    
                

        elif message.startswith("DECIDE"):
            print("DECIDE message received")
            # _, operation_num = message.split()
            # self.handle_decide(operation_num)

        else: # WE NEED To REMEMBER TO ACTUALLY PROCESS A FORWARDED MESSAGE FROM NON-LEADER NODES #------------- TODO
            print("Different message type")


    def broadcast(self, message):
        print(f"Node {self.node_id} broadcasting: {message}")
        for peer in self.peers:
            if (self.peers[1] != self.port):
                self.send_message(peer, message)


    def send_message(self, peer, message):
        print(f"Node {self.node_id} sending to {peer}: {message}")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(peer)
        s.sendall(message.encode())
        s.close()




        


    # MULTIPAXOS # ----------------------------------------------------------------------------------------------------------------------
    # def start_election(self): # Leader sends out PREPARE messages
    #     self.ballot_tuple = (self.ballot_tuple[0] + 1, self.node_id, len(self.queue))
    #     prepare_message = f"PREPARE {self.ballot_tuple}"
    #     self.broadcast(prepare_message)

    # def handle_prepare(self, ballot): # Acceptors reply to leader with PROMISE
    #     if ballot > self.accepted_ballot_num:
    #         self.accepted_ballot_num = ballot
    #         promise_message = f"PROMISE {ballot} {self.node_id}"
    #         self.send_message(self.current_leader, promise_message)
    

    def replicate_operation(self, command): # Leader sends out Accept messages
        
        # logic for obtaiing previously accepted value #------------------------------------------------ TODO

        accept_message = f"ACCEPT {self.ballot_tuple[0]} {self.ballot_tuple[2]} {command}" # ACCEPT seq_num, op_num, command
        self.broadcast(accept_message)

    def handle_accept(self, ballot, operation_num, command): # Acceptors Reply to leader with Accepted
        if ballot >= self.accepted_ballot_num:
            self.queue.append(operation_num)

            self.accepted_ballot_num = ballot
            self.accepted_val_num = command

            accepted_message = f"ACCEPTED {ballot} {self.node_id} {command}"
            # respond to leader with accepted message
            
            
            # wait 3 seconds
            time.sleep(3)
            node.send_message(node.peers[node.current_leader - 1], accepted_message)
            # self.send_message(self.current_leader, accepted_message)

    
    def decide_operation(self, command): # Leader sends out DECIDE messages
        # Apply the operation_num locally
        
        decide_message = f"DECIDE {command}"
        self.broadcast(decide_message)

        #self.apply_operation(operation_num)



    # def handle_decide(self, operation_num): # Acceptors apply the operation_num locally
    #     self.apply_operation(operation_num)


    # def apply_operation(self, operation_num):
    #     op_type, *args = operation_num.split()
    #     if op_type == "create":
    #         self.contexts[args[0]] = []
    #     elif op_type == "query":
    #         context_id, query_string = args
    #         self.contexts[context_id].append(query_string)
    #     elif op_type == "choose":
    #         # Handle LLM response selection logic here
    #         pass
    #     elif op_type == "view":
    #         print(self.contexts.get(args[0], "Context not found"))
    #     elif op_type == "viewall":
    #         print(self.contexts)
    

    # -----------------LEADER ELECTION DOESN'T WORK YET------------------------------------------
    # def monitor_leader(self):
    #     while True:
    #         time.sleep(5)
    #         if self.current_leader is None or not self.check_leader_alive():
    #             self.start_election()

    # def check_leader_alive(self): # Hardcoding that node 1 is always the leader
    #     # Implement a mechanism to verify leader liveness (e.g., heartbeat)
    #     if self.node_id ==  1:
    #         return True 
    #     return False




if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    port = 9000 + node_id
    peers = [("localhost", 9000 + i + 1) for i in range(3) if (i+1) != node_id]
    print(f"Node {node_id} peers: {peers}") # for debugging
    node = Node(node_id, port, peers)
    node.start()
    # threading.Thread(target=node.monitor_leader).start()

    while True:
        command = input("Enter command: ")
        if node.current_leader == node_id:
            print("I am Leader: Replicating operation_num...")
            node.replicate_operation(command)


        elif node.current_leader: # Forward command to leader
            print("I am NOT Leader: Forwarding command to leader...")
            node.send_message(node.peers[node.current_leader - 1], command)
            # node.send_message(node.current_leader, command) # ------------------------------------------- testing

        else:
            print("No leader. Need to start election...")
            # node.start_election()
