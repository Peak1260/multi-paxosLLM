import socket
import threading
import time
import google.generativeai as genai
import os
import sys
os.environ["GRPC_VERBOSITY"] = "NONE"

class CentralServer:
    def __init__(self):
        self.node_id = 0
        self.port = 9000
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', self.port))
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.listen(5)
        self.num_nodes = 3
        self.peers = [] # starts off as an empty list 
        self.contexts = {}
        self.current_leader = 0

    def start(self):
        print(f"Central server started on port {self.port}")
        while True:
            conn, addr = self.server_socket.accept()
            threading.Thread(target=self.handle_node_connection, args=(conn,)).start()

    def handle_node_connection(self, conn):
        try:
            data = conn.recv(1024).decode()
            if data:
                if not (data.startswith("ALIVE") or data.startswith("LEADER") or data.startswith("choose")): 
                    print(f"Central server received value: {data}")
                self.process_command(data)
        finally:
            conn.close()

    def send_message(self, peer, message):
        if not message.startswith("ALIVE"):
            print(f"Node {self.node_id} sending to {peer}: {message}")

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(peer)
        s.sendall(message.encode())
        s.close()
        
    def broadcast(self, message):
        for peer in self.peers:
            self.send_message(peer, message)

    def process_command(self, command):

        # faillink src dest
        if command.startswith("faillink"):
            parts = command.split()
            cmd = parts[0].lower()
            src_port, dest_port = int(parts[1]) + 9000, int(parts[2]) + 9000
            if cmd == "faillink" and len(parts) == 3:
                # traverse peers list and send message to both nodes whos link is being failed
                for peer in self.peers:
                    if peer[1] == src_port or peer[1] == dest_port:
                        self.send_message(peer, command) 
                        # print("Sent", command,  "to node:", peer)
                print("Fail link between", src_port, "and", dest_port)
            else:
                print("Invalid faillink command")
            
        # fixlink src dest
        elif command.startswith("fixlink"):
            parts = command.split()
            cmd = parts[0].lower()
            src_port, dest_port = int(parts[1]) + 9000, int(parts[2]) + 9000

            if cmd == "fixlink" and len(parts) == 3:
                for peer in self.peers:
                    if peer[1] == src_port or peer[1] == dest_port:
                        self.send_message(peer, command) 
                        # print("Sent", command,  "to node:", peer)
                print("Fix link between", src_port, "and", dest_port)
            else:
                print("Invalid fixlink command")
                
        # failnode node_id
        elif command.startswith("failnode"):
            parts = command.split()
            cmd = parts[0].lower()
            target_node = int(parts[1])

            if cmd == "failnode" and len(parts) == 2:
                self.broadcast(command)
                # self.peers = [peer for peer in self.peers if peer[1] != 9000 + target_node]
                # remove the node from the peers list
                self.peers.remove(("localhost", 9000 + target_node))
                # print("NEW PEERS LIST:", self.peers)
                print("We just failed node:", parts[1])
            else:
                print("Invalid failnode command")


        # ---------------------CENTRAL SERVER CREATING CONTEXT DICTIONARY---------------------
        elif command.startswith("create"):
            parts = command.split() # create context_id
            context_id = int(parts[1])
            self.contexts[context_id] = ""
            # print(f"Central Server's current contexts: {self.contexts}") # for debugging

        # query context_id query_string
        elif command.startswith("query"):
            parts = command.split() # query context_id query_string
            context_id = int(parts[1])
            query_string = " ".join(parts[2:])
            self.contexts[context_id] = self.contexts.get(context_id, "") + "Query: " + query_string + " "
            # print(f"Central Server's current contexts: {self.contexts}") # for debugging
        
        # Answer: response context_id
        elif command.startswith("Answer: "): 
            parts = command.split() # Answer: response context_id
            response = " ".join(parts[0:-1])
            context_id = int(parts[-1])
            self.contexts[context_id] = self.contexts.get(context_id, "") + response + " "
            # print(f"Central Server's current contexts: {self.contexts}") # for debugging

        # ---------------------CENTRAL SERVER CREATING CONTEXT DICTIONARY---------------------


        # RECOVERY -------------------------
        elif command.startswith("ALIVE"): # ALIVE node_id
            parts = command.split()
            node_id = int(parts[1])
            # Add this node to the peers list
            self.peers.append(("localhost", 9000 + node_id))
            print(f"Node {node_id} is alive.")
            # print("Peers list:", self.peers)

            # Broadcast to all peers that this node is alive
            if self.current_leader != 0:
                self.broadcast(command + " " + str(self.current_leader))
            else:
                self.broadcast(command)


        elif command.startswith("LEADER"): # LEADER node_id
            parts = command.split()
            self.current_leader = parts[1]

        # LOG node_id
        elif command.startswith("LOG"):
            parts = command.split()
            node_id = int(parts[1])
            print(f"Node {node_id} requested the log.")
            self.send_message(("localhost", 9000 + node_id), f"CONTEXT {self.contexts}")
        
        # RECOVERY -------------------------
               

class Node:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id # ID of process 1 = 1, process 2 = 2, process 3 = 3
        self.port = port # Port of process 1 = 9001, process 2 = 9002, process 3 = 9003
        self.peers = peers  # List of (host, port) tuples for other nodes
        self.current_leader = 0 # Current leader of the network 
        self.ballot_tuple = [0,node_id,0]  # (seq_num, pid, op_num)
        self.accepted_ballot_num = 0 # Highest ballot number accepted by the acceptor
        self.accepted_val_num = 0 # previous value accepted by the acceptor
        self.count_responses = 0 # ONLY LEADER USES THIS TO DETERMINE A MAJORITY, FOR NOW, ASSUME WE NEED 2
        self.contexts = {}
        self.promise_responses_dict = {} # Dictionary to store the promises received from the acceptors
        self.myVal = ""
        self.candidate_answers = {} # Dictionary to store the answers of the nodes, Leader accesses this only


        # Initialize server socket
        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.node_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.node_socket.bind(('localhost', self.port))
        self.node_socket.listen(5)
        
    def start(self):
        # Send a message to the central server that we are up
        alive_message = "ALIVE " + str(self.node_id)
        self.send_to_central_server(alive_message)

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
        if not (value.startswith("LOG") or value.startswith("ALIVE") or value.startswith("LEADER") or value.startswith("choose")): 
            print(f"Node {self.node_id} sending value to central server: {value}")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', central_server_port))
            s.sendall(value.encode())
        finally:
            s.close()

    def process_message(self, message):
        # Implement logic to process PREPARE, PROMISE, ACCEPT, DECIDE, etc.
        # print(f"Node {self.node_id} received: {message}")

        if message.startswith("PREPARE"): # Acceptors handle this
            print(f"Node {self.node_id} received: {message}")
            _, ballot, node_id, _ = message.split()
            ballot = int(ballot)
            node_id = int(node_id)
            self.handle_prepare(ballot, node_id)
    

        elif message.startswith("PROMISE"): # Leader handles this
            print(f"Node {self.node_id} received: {message}")
            time.sleep(3)
            list_of_messages = message.split(" ")
  
            ballot = int(list_of_messages[1])
            node_id = int(list_of_messages[2])
            accepted_ballot_num = int(list_of_messages[3])
            accepted_val_num = " ".join(list_of_messages[4:]) 

            self.promise_responses_dict[node_id] = [accepted_ballot_num, accepted_val_num] # mapping response from acceptor to its accepted ballot and value

            self.handle_promise(ballot, node_id, accepted_val_num)


        elif message.startswith("LEADER"):
            _, leader_id = message.split()
            if leader_id == 0:
                self.current_leader = 0
            else:
                self.current_leader = int(leader_id)
                

    
        elif message.startswith("ACCEPT "):
            print(f"Node {self.node_id} received: {message}")
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
            print(f"Node {self.node_id} received: {message}")
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
            print(f"Node {self.node_id} received: {message}")
            time.sleep(3)
            command = message.split(" ", 4)[4]
            self.handle_decide(command)
        
        elif message.startswith("Candidate: "): # Leader handles this
            self.count_responses += 1

            parts = message.split() # Candidate: response context_id node_id
            response = " ".join(parts[1:-2]) # response = "response"
            context_id = int(parts[-2]) # context_id = num
            node_id = int(parts[-1]) # node_id = num
            self.candidate_answers[node_id] = response + " " + str(context_id)

            # time.sleep(1)
            if self.count_responses == len(self.peers): # If we have received all responses from the nodes
                self.count_responses = 0 # Reset the count
                # print("Candidate answers:", self.candidate_answers) # For debugging
                for candidate in self.candidate_answers.keys():
                    # self.candidate_answers[candidate] would be response + " " + context_id but I just want the response
                    # even if response is longer than one word
                    answer = self.candidate_answers[candidate].split()[0:-1] # answer = response
                    answer = " ".join(answer)
                    print("Context", context_id, "- Candidate", candidate, ":", answer)
                    print()

        elif message.startswith("faillink"):# message = "faillink src dest"
            # print("Node", self.node_id, "received message:", message) # for debugging
            # Traverse the peers list and get rid of the other node so they are not a peer anymore
            command = message.split()
            src_port = int(command[1]) + 9000
            dest_port = int(command[2]) + 9000
            for peer in self.peers:
                if peer[1] == src_port or peer[1] == dest_port:
                    self.peers.remove(peer)
                    # print("New peers list:", self.peers)
                    # print("Node", self.node_id, "removed peer:", peer)

        elif message.startswith("fixlink"):# message = "faillink src dest"
            # print("Node", self.node_id, "received message:", message) # for debugging
            command = message.split()
            src_port = int(command[1]) + 9000
            dest_port = int(command[2]) + 9000

            # append the peer that isn't myself
            if src_port != self.port:
                self.peers.append(("localhost", src_port))
                # print("New peers list:", self.peers)
            else:
                self.peers.append(("localhost", dest_port))
                # print("New peers list:", self.peers)
                
        elif message.startswith("failnode"): # message = "failnode node_id"
            command = message.split() # failnode node_id
            target_port = int(command[1]) + 9000

            if target_port == self.port: # If the node that failed is me
                print("Node", self.node_id, "failed.")
                if self.current_leader == self.node_id:
                    self.current_leader = 0
                    leader_message = f"LEADER {self.current_leader}"
                    self.broadcast(leader_message)
                    time.sleep(1)
                
                os._exit(0)

            else: # If the node that failed is not me
                for peer in self.peers:
                    if peer[1] == target_port:
                        self.peers.remove(peer)
                        # print("New peers list: ", self.peers)
                        break
        
        # RECOVERY -------------------------
        elif message.startswith("ALIVE"): # message = "ALIVE node_id leader_id"
            parts = message.split()
            node_id = int(parts[1])

            if len(parts) == 3:
                self.current_leader = int(parts[2])

            # Add this node to the peers list
            if ("localhost", 9000 + node_id) not in self.peers:
                if node_id != self.node_id:
                    self.peers.append(("localhost", 9000 + node_id))
                    # print(f"Node {node_id} is alive.")
                    # print("Peers list:", self.peers)

        elif message.startswith("CONTEXT"): # message = "CONTEXT {context_id: context, context_id: context}"
            self.contexts = eval(message.split(" ", 1)[1])
            print(f"Updated context dictionary:", self.contexts)
        # RECOVERY -------------------------

        else: # leader does this
            print(f"Node {self.node_id} received: {message}")
            print("Replicating operations:", message)
            self.replicate_operation(message)


    def broadcast(self, message):
        for peer in self.peers:
            self.send_message(peer, message)

    def send_message(self, peer, message):
        if not (message.startswith("LEADER")):
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
            try:
                node.send_message(("localhost", 9000 + node_id), promise_message)  # Send promise back to the leader
            except Exception:
                print(f"Leader {node_id} is down. Cannot send promise message.")
                
        elif ballot == self.ballot_tuple[0]:
            if node_id > self.node_id:
                self.ballot_tuple[0] = ballot  # Update the ballot tuple
                promise_message = f"PROMISE {ballot} {node_id} {self.accepted_ballot_num} {self.accepted_val_num}"
                try:
                    node.send_message(("localhost", 9000 + node_id), promise_message)  # Send promise back to the leader
                except Exception:
                    print(f"Leader {node_id} is down. Cannot send promise message.")
                

    def replicate_operation(self, command): # Leader sends out Accept messages
        time.sleep(3)
        accept_message = f"ACCEPT {self.ballot_tuple[0]} {self.ballot_tuple[1]} {self.ballot_tuple[2]} {command}" # ACCEPT seq_num, pid, op_num, command
        self.broadcast(accept_message)


    def handle_promise(self, ballot, node_id, accepted_val_num): # Leader handles all the promises


        if hasattr(self, "broadcast_promise") and self.broadcast_promise:
            return  # Exit if already broadcasted

            # Mark as broadcasted
        self.broadcast_promise = True
            
        self.current_leader = node_id
        self.send_to_central_server(f"LEADER {self.current_leader}")

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

    def handle_accept(self, ballot, node_id, operation_num, command): # Acceptors Reply to leader with Accepted
        self.reset_broadcast_decide()
        self.reset_broadcast_promise()
        if ballot >= self.accepted_ballot_num:
            self.accepted_ballot_num = ballot
            self.accepted_val_num = command

            accepted_message = f"ACCEPTED {ballot} {node_id} {operation_num} {command}"
            # respond to leader with accepted message
            
            if self.current_leader != 0:
                self.send_message(("localhost", 9000 + self.current_leader), accepted_message) 
            else:
                print("Current leader is not set. Cannot send ACCEPTED message.")
            
            # Case where node is behind in operation number
            if (self.ballot_tuple[2] < operation_num): 
                self.ballot_tuple[2] = operation_num
                print(f"Node {self.node_id} updated operation number to {operation_num} because I was behind.")
                message_to_central_server = "LOG " + str(self.node_id) 
                # ask the central server for the most recent dictionary
                self.send_to_central_server(message_to_central_server)

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
        # self.send_to_central_server(command) #send to gemini # ACCEPTOR NODES DO NOT SEND TO CENTRAL SERVER
        self.apply_operation(command) #send to gemini

    def apply_operation(self, command):
        self.reset_broadcast_decide()
        if command.startswith("create"):
            self.contexts[int(command.split()[1])] = "" # create context
            # print(f"Node {self.node_id} created a context ID:", self.contexts)

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

                # Every node will send their gemini response to leader
                # Leader will then decide which response to choose
                if self.node_id != self.current_leader:
                    send_response_to_leader = "Candidate: " + response.text.strip() + " " + str(context_id) + " " + str(self.node_id) # Answer: California 0 3 (context Id = 0 from node 3)
                    self.send_message(("localhost", 9000 + self.current_leader), send_response_to_leader)
                else: # I am the leader
                    # add my response to candidate_answers dictionary
                    self.candidate_answers[self.node_id] = response.text.strip() + " " + str(context_id) # 3: "California 0"
                    # print("self.candidate_answers is:", self.candidate_answers)

        elif command.startswith("choose"): # choose <context_ID> <node_ID>

            if self.node_id == self.current_leader:
                # Only the leader does this

                parts = command.split() # choose context_ID node_ID
                context_id = int(parts[1]) # context_id = num
                node_id = int(parts[2]) # node_id = num

                # add this to its context dictionary
                answer = self.candidate_answers[node_id] # answer = response

                # Send this update to the central server
                agree_on_answer = "Answer: " + answer

                answer_without_context_id = agree_on_answer.split()[0:-1] # answer_without_context_id = response
                answer = " ".join(answer_without_context_id)
                self.contexts[context_id] += " " + answer + " "
                # print(f"Node {self.node_id} queried context:", self.contexts)

                self.replicate_operation(agree_on_answer)

        elif command.startswith("Answer: "): # Answer: response context_id
            self.ballot_tuple[2] += 1
            print("Current operation number:", self.ballot_tuple[2])

            if self.node_id != self.current_leader:
                parts = command.split()
                context_id = int(parts[-1])
                response = " ".join(parts[0:-1])
                self.contexts[context_id] += " " + response + " "
        
        else:
            print(f"Command not recognized:", command)   

if __name__ == "__main__":
    node_id = int(sys.argv[1])

    if node_id != 0: # All the nodes do this but Central Server does not autofill its peers
        port = 9000 + node_id
        peers = [("localhost", 9000 + i + 1) for i in range(3) if (i+1) != node_id]
        # print(f"Node {node_id} peers: {peers}") # for debugging
    
    running_flag = True
    server_running = True
    node_running = True
    
    while running_flag:
        if node_id == 0: 
            server = CentralServer()
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

                elif command.startswith("view "):
                    context_id = int(command.split()[1])
                    print(node.contexts[context_id])
                elif command.startswith("viewall"):
                    print(node.contexts)

                elif node.current_leader == node_id:
                    # print("I am leader")
                    node.replicate_operation(command)
                    
                elif node.current_leader: # Forward command to leader
                    # print("I am not leader")
                    node.send_message(("localhost", 9000 + node.current_leader), command)

                else:
                    print("No leader. Need to start election")
                    node.start_election(command)
    
    os._exit(0)