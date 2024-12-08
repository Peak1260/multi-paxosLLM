import socket
import threading
import time
import google.generativeai as genai
import os
import sys
os.environ["GRPC_VERBOSITY"] = "NONE"

class CentralServer:
    def __init__(self, peers):
        self.node_id = 0
        self.port = 9000
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', self.port))
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.listen(5)
        self.num_nodes = 3
        self.peers = peers
        self.active_connections = {}
        self.connection_lock = threading.Lock()
        self.timeout_interval = 6  # 5 seconds timeout
        self.heartbeat_interval = 1  # 2 seconds between heartbeats
        self.failed_nodes = set()  # Track nodes that have failed

    def start(self):
        print(f"Central server started on port {self.port}")
        
        # Start heartbeat monitoring thread
        heartbeat_thread = threading.Thread(target=self.monitor_node_connections)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        
        # Accept incoming connections
        while True:
            conn, addr = self.server_socket.accept()
            threading.Thread(target=self.handle_node_connection, args=(conn, addr)).start()

    def handle_node_connection(self, conn, addr):
        try:
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break
                
                print(f"Central server received value: {data}")
                
                with self.connection_lock:
                    # Update last heartbeat time for this connection
                    self.active_connections[addr[1]] = time.time()
                    
                    # Check if this is a previously failed node reconnecting
                    node_id = addr[1] - 9000
                    if node_id in self.failed_nodes:
                        self.handle_node_reconnection(node_id)
                
                self.process_command(data)
        except Exception as e:
            print(f"Connection error with {addr}: {e}")
        finally:
            with self.connection_lock:
                if addr[1] in self.active_connections:
                    del self.active_connections[addr[1]]
            conn.close()

    def monitor_node_connections(self):
        while True:
            current_time = time.time()
            failed_nodes = []
            
            with self.connection_lock:
                # Check for timeout on active connections
                for port, last_heartbeat in list(self.active_connections.items()):
                    if current_time - last_heartbeat > self.timeout_interval:
                        # Identify which node failed based on port
                        failed_node = port - 9000
                        failed_nodes.append(failed_node)
                        
                        # Remove from active connections
                        del self.active_connections[port]
            
            # Broadcast node failures
            for node in failed_nodes:
                self.handle_node_failure(node)
            
            # Wait before next check
            time.sleep(self.heartbeat_interval)

    def handle_node_failure(self, failed_node):
        # Add to failed nodes set
        self.failed_nodes.add(failed_node)
        
        # Remove failed node from peers
        self.peers = [peer for peer in self.peers if peer[1] != 9000 + failed_node]
        
        # Broadcast timeout message to all remaining nodes
        timeout_message = f"TIMEOUT {failed_node}"
        self.broadcast(timeout_message)
        
        print(f"Node {failed_node} timed out and removed from peers")

    def handle_node_reconnection(self, reconnected_node):
        # Remove from failed nodes
        self.failed_nodes.discard(reconnected_node)
        
        # Add back to peers if not already present
        reconnected_port = 9000 + reconnected_node
        if not any(peer[1] == reconnected_port for peer in self.peers):
            self.peers.append(('localhost', reconnected_port))
        
        # Broadcast node reconnection to other nodes
        reconnect_message = f"RECONNECT {reconnected_node}"
        self.broadcast(reconnect_message)
        print(f"Node {reconnected_node} has reconnected")

    def send_message(self, peer, message):
        try:
            print(f"Node {self.node_id} sending to {peer}: {message}")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(peer)
            s.sendall(message.encode())
            
            # Maintain the connection in active connections
            with self.connection_lock:
                self.active_connections[peer[1]] = time.time()
            
            s.close()
        except Exception as e:
            print(f"Error sending message to {peer}: {e}")
            # If sending fails, treat it as a node failure
            failed_node = peer[1] - 9000
            self.handle_node_failure(failed_node)
        
    def broadcast(self, message):
        print(f"Node {self.node_id} broadcasting: {message}")
        for peer in self.peers:
            self.send_message(peer, message)

    def process_command(self, command):
        if command.startswith("faillink"):
            parts = command.split()
            cmd = parts[0].lower()
            src_port, dest_port = int(parts[1]) + 9000, int(parts[2]) + 9000
            if cmd == "faillink" and len(parts) == 3:
                for peer in self.peers:
                    if peer[1] == src_port or peer[1] == dest_port:
                        self.send_message(peer, command) 
                        print("Sent", command,  "to node:", peer)
                print("Fail link between", src_port, "and", dest_port)
            else:
                print("Invalid faillink command")
            
        elif command.startswith("fixlink"):
            parts = command.split()
            cmd = parts[0].lower()
            src_port, dest_port = int(parts[1]) + 9000, int(parts[2]) + 9000

            if cmd == "fixlink" and len(parts) == 3:
                for peer in self.peers:
                    if peer[1] == src_port or peer[1] == dest_port:
                        self.send_message(peer, command) 
                        print("Sent", command,  "to node:", peer)
                print("Fix link between", src_port, "and", dest_port)
            else:
                print("Invalid fixlink command")
                
        elif command.startswith("failnode"):
            parts = command.split()
            cmd = parts[0].lower()
            target_node = int(parts[1])

            if cmd == "failnode" and len(parts) == 2:
                self.broadcast(command)
                self.peers = [peer for peer in self.peers if peer[1] != 9000 + target_node]
                print("We just failed node:", parts[1])
            else:
                print("Invalid failnode command")

class Node:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id # ID of process 1 = 1, process 2 = 2, process 3 = 3
        self.port = port # Port of process 1 = 9001, process 2 = 9002, process 3 = 9003
        self.peers = peers  # List of (host, port) tuples for other nodes
        self.current_leader = None # Current leader of the network 
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
        self.node_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.node_socket.bind(('localhost', self.port))
        self.node_socket.listen(5)
        
    def start(self):
        print(f"Node {self.node_id} started on port {self.port}")
        threading.Thread(target=self.listen).start()

    def listen(self):
        while True:
            conn, addr = self.node_socket.accept()
            #print (f"Node {self.node_id} connected to {addr}")
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
        print(f"Node {self.node_id} received: {message}")

        

        if message.startswith("PREPARE"): # Acceptors handle this
            time.sleep(3)
            _, ballot, node_id, _ = message.split()
            ballot = int(ballot)
            node_id = int(node_id)
            self.handle_prepare(ballot, node_id)
    

        elif message.startswith("PROMISE"): # Leader handles this
            time.sleep(3)
            list_of_messages = message.split(" ")
  
            ballot = int(list_of_messages[1])
            node_id = int(list_of_messages[2])
            accepted_ballot_num = int(list_of_messages[3])
            accepted_val_num = " ".join(list_of_messages[4:]) 

            self.promise_responses_dict[node_id] = [accepted_ballot_num, accepted_val_num] # mapping response from acceptor to its accepted ballot and value

            self.handle_promise(ballot, node_id, accepted_val_num)


        elif message.startswith("LEADER"):
            time.sleep(1)
            _, leader_id = message.split()
            if leader_id == "None":
                leader_id = None
            else:
                leader_id = int(leader_id)
                
            self.handle_leader(leader_id) 
            print("got here")

    
        elif message.startswith("ACCEPT "):
            time.sleep(3)
    
            # Split the message into parts
            list_of_messages = message.split(" ")
            # print("list_of_messages: ", list_of_messages)  # For debugging
            
            # Extract parts
            ballot = int(list_of_messages[1])
            node_id = int(list_of_messages[2])
            operation_num = int(list_of_messages[3])
            
            # Combine the rest of the message as the command
            command = " ".join(list_of_messages[4:])  # Join any remaining parts

            # Pass extracted values to handle_accept
            self.handle_accept(ballot, node_id, operation_num, command)


        elif message.startswith("ACCEPTED"): # Leader handles this
            # Message looks like: ACCEPTED 1 3 0 create 0
            time.sleep(3)
            command = message.split(" ", 4)[4]

            # We want to parse out the node_id
            list_of_messages = message.split()
            ballot = int(list_of_messages[1])
            node_id = int(list_of_messages[2])
            op_num = int(list_of_messages[3])
            # print("ACCEPTED node_id: ", node_id) # for debugging

            self.decide_operation(command, ballot, node_id, op_num)          

        elif message.startswith("DECIDE"):
            time.sleep(3)
            print("DECIDE message received")
            command = message.split(" ", 4)[4]
            self.handle_decide(command)
        
        elif message.startswith("faillink"):# message = "faillink src dest"
            # print("Node", self.node_id, "received message:", message) # for debugging
            # Traverse the peers list and get rid of the other node so they are not a peer anymore
            command = message.split()
            src_port = int(command[1]) + 9000
            dest_port = int(command[2]) + 9000
            for peer in self.peers:
                if peer[1] == src_port or peer[1] == dest_port:
                    self.peers.remove(peer)
                    print(self.peers)
                    print("Node", self.node_id, "removed peer:", peer)

        elif message.startswith("fixlink"):# message = "faillink src dest"
            # print("Node", self.node_id, "received message:", message) # for debugging
            command = message.split()
            src_port = int(command[1]) + 9000
            dest_port = int(command[2]) + 9000

            # append the peer that isn't myself
            if src_port != self.port:
                self.peers.append(("localhost", src_port))
                print("new peers list: ", self.peers)
            else:
                self.peers.append(("localhost", dest_port))
                print("new peers list: ", self.peers)
                
        elif message.startswith("failnode"): # message = "failnode node_id"
            command = message.split() # failnode node_id
            target_port = int(command[1]) + 9000

            if target_port == self.port: # If the node that failed is me
                print("Node", self.node_id, "failed.")
                if self.current_leader == self.node_id:
                    self.current_leader = None
                    leader_message = f"LEADER {self.current_leader}"
                    self.broadcast(leader_message)
                    
                self.peers = []
                print("new peers list is empty: ", self.peers)
                
                os._exit(0)

            else: # If the node that failed is not me
                for peer in self.peers:
                    if peer[1] == target_port:
                        self.peers.remove(peer)
                        print("new peers list: ", self.peers)
                        break         
            
        else: 
            time.sleep(3)
            print("Replicating operations:", message)
            self.replicate_operation(message)


    def broadcast(self, message):
        print(f"Node {self.node_id} broadcasting: {message}")
        for peer in self.peers:
            self.send_message(peer, message)


    def send_message(self, peer, message):
        print(f"Node {self.node_id} sending to {peer}: {message}")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(peer)
        s.sendall(message.encode())
        s.close()
        
    def reset_broadcast_promise(self):
        """Reset the broadcast promise flag."""
        if hasattr(self, "broadcast_promise"):
            self.broadcast_promise = False
    
    def reset_broadcast_decide(self):
        """Reset the broadcast decide flag."""
        if hasattr(self, "broadcast_decide"):
            self.broadcast_decide = False


    # MULTIPAXOS # ----------------------------------------------------------------------------------------------------------------------
    def start_election(self, command): # Leader sends out PREPARE messages
        self.myVal = command
        self.ballot_tuple[0] += 1
        prepare_message = f"PREPARE {self.ballot_tuple[0]} {self.ballot_tuple[1]} {self.ballot_tuple[2]}"
        self.broadcast(prepare_message)

    def handle_prepare(self, ballot, node_id): # Acceptors reply to leader with PROMISE
        if ballot > self.ballot_tuple[0]:
            self.ballot_tuple[0] = ballot  # Update the ballot tuple
            promise_message = f"PROMISE {ballot} {node_id} {self.accepted_ballot_num} {self.accepted_val_num}"
            node.send_message(("localhost", 9000 + node_id), promise_message)  # Send promise back to the leader
        elif ballot == self.ballot_tuple[0]:
            if node_id > self.node_id:
                self.ballot_tuple[0] = ballot  # Update the ballot tuple
                promise_message = f"PROMISE {ballot} {node_id} {self.accepted_ballot_num} {self.accepted_val_num}"
                node.send_message(("localhost", 9000 + node_id), promise_message)  # Send promise back to the leader

    def replicate_operation(self, command): # Leader sends out Accept messages
        accept_message = f"ACCEPT {self.ballot_tuple[0]} {self.ballot_tuple[1]} {self.ballot_tuple[2]} {command}" # ACCEPT seq_num, pid, op_num, command
        self.broadcast(accept_message)


    def handle_promise(self, ballot, node_id, accepted_val_num): # Leader handles all the promises
        if hasattr(self, "broadcast_promise") and self.broadcast_promise:
            return  # Exit if already broadcasted

            # Mark as broadcasted
        self.broadcast_promise = True
            
        self.current_leader = node_id
        leader_message = f"LEADER {self.current_leader}"
        # DON'T BROADCAST LEADER MESSAGE, SEND TO THE RIGHT NODE WHO RESPONDED TO THE PROMISE
        for peer in self.peers:
            if peer[1] != 9000 + node_id:  # Skip sending to itself
                self.send_message(peer, leader_message)

        # Leader is going to look at dictionary of promises and determine the highest ballot number and value
        # first the leader will check if every acceptor has an acceptvalnum of 0 so leader can use its command
        # psudocode: if all acceptvals are 0, then send out my command in accept message
        counter_of_acceptvals = 0
        highest_accepted_ballot_num = 0
        for accepted_thing in self.promise_responses_dict.values(): # accepted_thing = [accepted_ballot_num, accepted_val_num]
            if isinstance(accepted_thing[1], str) and accepted_thing[1].isdigit():
                accepted_val_num = int(accepted_thing[1])  # Convert to int if it's a string representation of a number
            else:
                accepted_val_num = accepted_thing[1]
                
            if accepted_val_num == 0: # if acceptval is 0
                counter_of_acceptvals += 1 # increment counter
            else:
                if accepted_thing[0] > highest_accepted_ballot_num: # if acceptval is not 0, then check if the ballot number is higher than the current highest
                    highest_accepted_ballot_num = accepted_thing[0] # update the highest accepted ballot number

        if counter_of_acceptvals == 1: # If all acceptvals are 0, then send out my command in accept message
            self.accepted_val_num = self.myVal # set accepted value to myVal
   
        else:
            for accepted_thing in self.promise_responses_dict.values(): # accepted_thing = [accepted_ballot_num, accepted_val_num]
                if accepted_thing[0] == highest_accepted_ballot_num: # if the ballot number is the highest, then set the accepted value to that
                    self.myVal = accepted_thing[1] # set myVal to the accepted value
                    self.accepted_val_num = self.myVal # set accepted value to the accepted value
                    break
    
        command = self.accepted_val_num # same as myVal
        # Send "ACCEPT" message to all peers
        accept_message = f"ACCEPT {ballot} {node_id} {self.ballot_tuple[2]} {command}"  # Use command instead of my_val

        for peer in self.peers:
            if peer[1] != 9000 + node_id:  # Skip sending to itself
                self.send_message(peer, accept_message)

    def handle_leader(self, leader_id):
        self.current_leader = leader_id
        #print(f"Node {self.node_id} recognizes Node {self.current_leader} as the leader.")

    def handle_accept(self, ballot, node_id, operation_num, command): # Acceptors Reply to leader with Accepted
        self.reset_broadcast_decide()
        self.reset_broadcast_promise()
        if ballot >= self.accepted_ballot_num:
            self.queue.append(operation_num)

            self.accepted_ballot_num = ballot
            self.accepted_val_num = command

            accepted_message = f"ACCEPTED {ballot} {node_id} {operation_num} {command}"
            # respond to leader with accepted message
            
            if self.current_leader is not None:
                self.send_message(("localhost", 9000 + self.current_leader), accepted_message) 
            else:
                print("Current leader is not set. Cannot send ACCEPTED message.")

    def decide_operation(self, command, ballot, node_id, op_num): # Leader sends out DECIDE messages
        if hasattr(self, "broadcast_decide") and self.broadcast_decide:
            return  # Exit if already broadcasted

            # Mark as broadcasted
        self.broadcast_decide = True
        
        decide_message = f"DECIDE {ballot} {node_id} {op_num} {command}"

        for peer in self.peers:
            if peer[1] != 9000 + node_id:  # Skip sending to itself
                self.send_message(peer, decide_message)
                
        self.send_to_central_server(command) #send to gemini
        self.apply_operation(command) #send to gemini

    def handle_decide(self, command): # Acceptors apply the operation_num locally
        self.send_to_central_server(command) #send to gemini
        self.apply_operation(command) #send to gemini

    def apply_operation(self, command):
        self.reset_broadcast_decide()
        if command.startswith("create"):
            self.contexts[int(command.split()[1])] = "" # create context
            print(f"Node {self.node_id} created a context ID:", self.contexts)

        elif command.startswith("query"):
            # -----------------------Gemini-------------------------------
            genai.configure(api_key="AIzaSyC64zw3CDFPuK1IJsyB_PyGA355XnmT2zw")
            model = genai.GenerativeModel("gemini-1.5-flash")
  
            # -----------------------Gemini---------------------------------

            command_list = command.split() # query context_id query_string --> Query the LLM on a context ID with a query string
            context_id = int(command_list[1]) # context_id = num
            
            # After the for loop, if the contextID is not found, then print out error message
            if context_id not in self.contexts.keys():
                print(f"Context ID {context_id} not found.")
            
            else:
                # Example input: query 1 What is the weather like today?
                # We want query = What is the weather like today?
                self.ballot_tuple[2] += 1
                print("Current operation number:", self.ballot_tuple[2])
                
                
                query = command.split(" ", 2)[2] # query = "What is the weather like today?"
                # print(query) # for debugging
                query_string = "Query: " + query # query_string = "QUERY: What is the weather like today?"
                self.contexts[context_id] = self.contexts[context_id] + query_string

                gemini_context = self.contexts[context_id] # gemini_context = "Query: What is the weather like today?"
                response = model.generate_content(gemini_context, generation_config=genai.types.GenerationConfig(temperature = 0.1)) # response = "Answer: Sunny and 75 degrees"
                # response_text = "Answer: " + response.text
                # print(response_text)

                # MAYBE HERE WE CAN ASK FOR USER INPUT TO PICK THE FINAL ANSWER INSTEAD OF IT ALWAYS BEING THE LEADER'S ANSWER --------------------------

                if self.node_id == self.current_leader: # leader's answer will always be the final answer
                    self.contexts[context_id] = self.contexts[context_id] + " " + "Answer: " + response.text.strip() + " "
                    print(f"Node {self.node_id} queried context:", self.contexts)

                    time.sleep(3)
                    print("Reaching consensus on the final answer")
                    answer_with_context_id_command = "Answer: " + response.text.strip() + " " + str(context_id)

                    self.replicate_operation(answer_with_context_id_command)
                    

        elif command.startswith("Answer: "):
            self.ballot_tuple[2] += 1
            
            print("Current operation number:", self.ballot_tuple[2])
            # If we are the leader, we can skip this step because we already added the response to the context
            # If we are NOT the leader, we need to add the response to the context
            if self.node_id != self.current_leader:
                # I want to separate the string into two parts: Answer: Sunny and 75 degrees 1 where 1 is the context ID
                command_list = command.split() # Answer: Sunny and 75 degrees 1
                # I want response text to be "Answer: Sunny and 75 degrees"
                response_text = " ".join(command_list[0:-1]) # response_text = "Answer: Sunny and 75 degrees"
                # print("Command starts with Answer: --> response_text: ", response_text) # for debugging
                # I want context_id to be 1
                context_id = int(command_list[-1]) # context_id = 1
                # print("Command starts with Answer: --> context_id: ", context_id) # for debugging

                self.contexts[context_id] = self.contexts[context_id] + " " + response_text + " "
            print(f"Node {self.node_id} new context dictionary:", self.contexts)

        elif command.startswith("viewall"):
            print(self.contexts)
        
        elif command.startswith("view "): # view <context_id>
            context_id = int(command.split()[1])
            print(self.contexts[context_id])
            # print(f"Node {self.node_id} viewed context ID {context_id}: {self.contexts.get(context_id, 'Context not found')}")
        
        else:
            print(f"Command -> {command} <- not recognized.")   

if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    port = 9000 + node_id
    peers = [("localhost", 9000 + i + 1) for i in range(3) if (i+1) != node_id]
    print(f"Node {node_id} peers: {peers}") # for debugging
    
    running_flag = True
    server_running = True
    node_running = True
    
    while running_flag:
        if node_id == 0: 
            server = CentralServer(peers)
            threading.Thread(target=server.start).start()
            
            while server_running:
                command = input()
                if command.lower() == "exit":
                    server_running = False
                    running_flag = False
                    break
                server.process_command(command)
        else:
            node = Node(node_id, port, peers)
            threading.Thread(target=node.start).start()
            
            while node_running:
                command = input()
                if command.lower() == "exit":
                    node_running = False
                    running_flag = False
                    break
                if node.current_leader == node_id:
                    print("I am Leader: Replicating operation_num...")
                    node.replicate_operation(command)
                    
                elif node.current_leader: # Forward command to leader
                    print("I am NOT Leader: Forwarding command to leader...")
                    node.send_message(("localhost", 9000 + node.current_leader), command)

                else:
                    print("No leader. Need to start election...")
                    node.start_election(command)
    
    os._exit(0)