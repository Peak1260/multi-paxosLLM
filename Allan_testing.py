# NEED TO IMPLEMENT CENTRAL SERVER (9000) that receives all the gemini commands

# We need to implement when a node is not the leader, it forwards the command to the leader - DONE
# Instead of leader broadcasing to all nodes an ACCEPT message or PROMIS message, we need to track the pid from who responded so 
# We can send the message directly back to the right node that responded
# Test out replicate operation more times to see if logic holds true - DONE

import socket
import threading
import time

class CentralServer:
    def __init__(self, port=9000):
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', self.port))
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.listen(5)

    def start(self):
        print(f"Central server started on port {self.port}")
        while True:
            conn, addr = self.server_socket.accept()
            print(f"Connection received from {addr}")
            threading.Thread(target=self.handle_node_connection, args=(conn,)).start()

    def handle_node_connection(self, conn):
        try:
            data = conn.recv(1024).decode()
            if data:
                print(f"Central server received value: {data}")
        finally:
            conn.close()

class Node:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id # ID of process 1 = 1, process 2 = 2, process 3 = 3
        self.port = port # Port of process 1 = 9001, process 2 = 9002, process 3 = 9003
        self.peers = peers  # List of (host, port) tuples for other nodes
        self.current_leader = None # Current leader of the network for right now is hardcoded to 1
        self.ballot_tuple = [0,node_id,0]  # (seq_num, pid, op_num)
        self.accepted_ballot_num = 0 # Highest ballot number accepted by the acceptor
        self.accepted_val_num = 0 # previous value accepted by the acceptor
        self.count_responses = 0 # ONLY LEADER USES THIS TO DETERMINE A MAJORITY, FOR NOW, ASSUME WE NEED 2
        self.queue = []
        self.contexts = {}
        self.promise_responses_dict = {} # Dictionary to store the promises received from the acceptors
        self.myVal = ""

        # Initialize server socket
        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.node_socket.bind(('localhost', self.port))
        self.node_socket.listen(5)
        
    def start(self):
        print(f"Node {self.node_id} started on port {self.port}")
        threading.Thread(target=self.listen).start()

    def listen(self):
        while True:
            conn, addr = self.node_socket.accept()
            print (f"Node {self.node_id} connected to {addr}")
            threading.Thread(target=self.handle_connection, args=(conn,)).start()

    def handle_connection(self, conn):
        try:
            data = conn.recv(1024).decode()
            if data:
                self.process_message(data)
        finally:
            conn.close()

    def send_to_central_server(self, value):
        central_server_port = 9000  
        print(f"Node {self.node_id} sending value to central server: {value}")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', central_server_port))
            s.sendall(value.encode())
        finally:
            s.close()

    def process_message(self, message):
        # Implement logic to process PREPARE, PROMISE, ACCEPT, DECIDE, etc.
        time.sleep(3)
        print(f"Node {self.node_id} received: {message}")

        if message.startswith("PREPARE"): # Acceptors handle this
            _, ballot, node_id = message.split()
            ballot = int(ballot)
            node_id = int(node_id)
            self.handle_prepare(ballot, node_id)
    

        elif message.startswith("PROMISE"): # Leader handles this
            # self.count_responses += 1 
            _, ballot, node_id, accepted_ballot_num, accepted_val_num = message.split()
            ballot = int(ballot)
            node_id = int(node_id)
            accepted_ballot_num = int(accepted_ballot_num)
            accepted_val_num = int(accepted_val_num)

            self.promise_responses_dict[node_id] = [accepted_ballot_num, accepted_val_num] # mapping response from acceptor to its accepted ballot and value

            self.handle_promise(ballot, accepted_ballot_num, accepted_val_num, node_id)
            
            # if self.count_responses == 2: # This is to ensure, decide is only done ONCE
            #     self.count_responses = 0
            #     self.handle_promise(ballot, accepted_ballot_num, accepted_val_num, node_id)
            # else:
            #     print("Not enough PROMISE messages received yet")

        elif message.startswith("LEADER"):
            _, leader_id = message.split()
            leader_id = int(leader_id)
            self.handle_leader(leader_id) 

        # ------------------------------------------------------------
        elif message.startswith("ACCEPT "):
            command = message.split(" ", 3)[3]
            #print("command: ", command) # for debugging
            list_of_messages = message.split() 
            #print("list_of_messages: ", list_of_messages) # list_of_messages:  ['ACCEPT', '0', '0', 'create', '0'] #for debugging
            ballot = int(list_of_messages[1])
            operation_num = int(list_of_messages[2])
            self.handle_accept(ballot, operation_num, command) # acceptors handle this


        elif message.startswith("ACCEPTED"): # Leader handles this
            # Message looks like: ACCEPTED 0 3 create 0
            # self.count_responses += 1 
            command = message.split(" ", 3)[3]

            # We want to parse out the node_id
            list_of_messages = message.split()
            node_id = int(list_of_messages[2])
            # print("ACCEPTED node_id: ", node_id) # for debugging
            
            # LOGIC FOR HANDLING ACCEPTED MESSAGES # ------------------------------------------------ TODO


            self.decide_operation(command, node_id)

            # if self.current_leader == self.node_id:
            #     if self.count_responses == 2: # This is to ensure, decide is only done ONCE
            #         self.count_responses = 0
            #         self.decide_operation(command)
            #     else:
            #         print("Not enough ACCEPTED messages received yet")
                    
                

        elif message.startswith("DECIDE"):
            print("DECIDE message received")
            command = message.split(" ", 1)[1]
            self.handle_decide(command)

        else: 
            print("Replicating operations:", message)
            self.replicate_operation(message)


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
    def start_election(self, command): # Leader sends out PREPARE messages
        self.myVal = command
        self.ballot_tuple[0] += 1
        prepare_message = f"PREPARE {self.ballot_tuple[0]} {self.node_id}"
        self.broadcast(prepare_message)
        time.sleep(1)

    def handle_prepare(self, ballot, node_id): # Acceptors reply to leader with PROMISE
        if ballot >= self.ballot_tuple[0]:
            self.ballot_tuple[0] = ballot  # Update the ballot tuple
            promise_message = f"PROMISE {ballot} {self.node_id} {self.accepted_ballot_num} {self.accepted_val_num}"
            node.send_message(("localhost", 9000 + node_id), promise_message)  # Send promise back to the leader
            time.sleep(1)

    def replicate_operation(self, command): # Leader sends out Accept messages
        # logic for obtaiing previously accepted value #------------------------------------------------ TODO
        accept_message = f"ACCEPT {self.ballot_tuple[0]} {self.ballot_tuple[2]} {command}" # ACCEPT seq_num, op_num, command
        
        # send to the other peers
        # for peer in self.peers:
        #     self.send_message(peer, accept_message)

        self.broadcast(accept_message)



    def handle_promise(self, ballot, accepted_ballot_num, accepted_val_num, node_id): # Leader handles all the promomises
        self.current_leader = self.node_id
        # print(f"Current leader set to: {self.current_leader}")
        leader_message = f"LEADER {self.current_leader}"
        # self.broadcast(leader_message) # Broadcast to all nodes who the leader is
        # DON'T BROADCAST LEADER MESSAGE, SEND TO THE RIGHT NODE WHO RESPONDED TO THE PROMISE
        self.send_message(("localhost", 9000 + node_id), leader_message)

        # Leader is going to look at dictionary of promises and determine the highest ballot number and value
        # first the leader will check if every acceptor has an acceptvalnum of 0 so leader can use its command
        # psudocode: if all acceptvals are 0, then send out my command in accept message
        counter_of_acceptvals = 0
        highest_accepted_ballot_num = 0
        for accepted_thing in self.promise_responses_dict.values(): # accepted_thing = [accepted_ballot_num, accepted_val_num]
            if accepted_thing[1] == 0: # if acceptval is 0
                counter_of_acceptvals += 1 # increment counter
            else:
                if accepted_thing[0] > highest_accepted_ballot_num: # if acceptval is not 0, then check if the ballot number is higher than the current highest
                    highest_accepted_ballot_num = accepted_thing[0] # update the highest accepted ballot number

        if counter_of_acceptvals == len(peers): # If all acceptvals are 0, then send out my command in accept message
            self.accepted_val_num = self.myVal # set accepted value to myVal
        else:
            for accepted_thing in self.promise_responses_dict.values(): # accepted_thing = [accepted_ballot_num, accepted_val_num]
                if accepted_thing[0] == highest_accepted_ballot_num: # if the ballot number is the highest, then set the accepted value to that
                    self.myVal = accepted_thing[1] # set myVal to the accepted value
                    self.accepted_val_num = accepted_thing[1] # set accepted value to the accepted value
                    break
    
        command = self.accepted_val_num # same as myVal
        # Send "ACCEPT" message to all peers
        accept_message = f"ACCEPT {self.ballot_tuple[0]} {self.ballot_tuple[2]} {command}"  # Use command instead of my_val

        node.send_message(("localhost", 9000 + node_id), accept_message)  # Send accept message to the right node who responded to the promise
        time.sleep(1)

        # DONT WANT TO BROADCAST ACCEPT MESSAGE, SEND TO THE RIGHT NODE WHO RESPONDED TO THE PROMISE
        # self.broadcast(accept_message)  # Send to all peers



    def handle_leader(self, leader_id):
        self.current_leader = leader_id
        print(f"Node {self.node_id} recognizes Node {self.current_leader} as the leader.")
    

    def handle_accept(self, ballot, operation_num, command): # Acceptors Reply to leader with Accepted
        # print(f"Current leader set to: {self.current_leader}")
        if ballot >= self.accepted_ballot_num:
            self.queue.append(operation_num)

            self.accepted_ballot_num = ballot
            self.accepted_val_num = command

            accepted_message = f"ACCEPTED {ballot} {self.node_id} {command}"
            # respond to leader with accepted message
            
            
            if self.current_leader is not None:
                self.send_message(("localhost", 9000 + self.current_leader), accepted_message) 
                
            else:
                print("Current leader is not set. Cannot send ACCEPTED message.")
            
        time.sleep(1)

    
    def decide_operation(self, command, node_id): # Leader sends out DECIDE messages
        decide_message = f"DECIDE {command}"
        # leader now increments operation number 
        self.ballot_tuple[2] += 1
        self.count_responses += 1
        print("count_responses: ", self.count_responses)


        # DON'T BROADCAST DECIDE MESSAGE, SEND TO THE RIGHT NODE WHO RESPONDED TO THE PROMISE
        # self.broadcast(decide_message)


        # if self.current_leader != self.node_id:
        self.send_message(("localhost", 9000 + node_id), decide_message) # Send to the right node who responded to the promise

        # self.send_message(("localhost", 9000 + self.current_leader), decide_message) # Send to the right node who responded to the promise
       
        if self.count_responses == 1:
            print("Node {self.node_id} sending value to central server: {command}", self.node_id, command)
            # self.send_to_central_server(command)
            self.send_message(("localhost", 9000 + self.current_leader), decide_message)
        elif self.count_responses == len(self.peers):
            print("Reseting count_responses")
            self.count_responses = 0
            print("count_responses: ", self.count_responses)

        time.sleep(1)




    def handle_decide(self, command): # Acceptors apply the operation_num locally
        # self.apply_operation(command)
        self.send_to_central_server(command)


    def apply_operation(self, operation_num):
        op_type, *args = operation_num.split()
        if op_type == "create":
            self.contexts[args[0]] = []
        elif op_type == "query":
            context_id, query_string = args
            self.contexts[context_id].append(query_string)
        elif op_type == "choose":
            # Handle LLM response selection logic here
            pass
        elif op_type == "view":
            print(self.contexts.get(args[0], "Context not found"))
        elif op_type == "viewall":
            print(self.contexts)

if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    port = 9000 + node_id
    peers = [("localhost", 9000 + i + 1) for i in range(3) if (i+1) != node_id]
    print(f"Node {node_id} peers: {peers}") # for debugging
    if node_id != 0:
        node = Node(node_id, port, peers)
        threading.Thread(target = node.start).start()

    if node_id == 0:  # Start the Central Server only on node 0
        server = CentralServer()
        threading.Thread(target=server.start).start()

    while True:
        command = input()
        if node.current_leader == node_id:
            print("I am Leader: Replicating operation_num...")
            node.replicate_operation(command)
            
        elif node.current_leader: # Forward command to leader
            print("I am NOT Leader: Forwarding command to leader...")
            node.send_message(("localhost", 9000 + node.current_leader), command)

        else:
            print("No leader. Need to start election...")
            node.start_election(command)
