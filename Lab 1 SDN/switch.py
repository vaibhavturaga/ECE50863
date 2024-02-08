
#!/usr/bin/env python

"""This is the Switch Starter Code for ECE50863 Lab Project 1
Author: Xin Du
Email: du201@purdue.edu
Last Modified Date: December 9th, 2021
"""

import sys
from datetime import date, datetime
import socket
import threading
import time
# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "switch#.log" # The log file for switches are switch#.log, where # is the id of that switch (i.e. switch0.log, switch1.log). The code for replacing # with a real number has been given to you in the main function.
K = 2
TIMEOUT = 3
neighbor_status = {}
last_keep_alive_received = {}
failed_neighbor_id = None
# Those are logging functions to help you follow the correct logging standard

# "Register Request" Format is below:
#
# Timestamp
# Register Request Sent

def register_request_sent():
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Request Sent\n")
    write_to_log(log)

# "Register Response" Format is below:
#
# Timestamp
# Register Response Received

def register_response_received():
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Response received\n")
    write_to_log(log) 

# For the parameter "routing_table", it should be a list of lists in the form of [[...], [...], ...]. 
# Within each list in the outermost list, the first element is <Switch ID>. The second is <Dest ID>, and the third is <Next Hop>.
# "Routing Update" Format is below:
#
# Timestamp
# Routing Update 
# <Switch ID>,<Dest ID>:<Next Hop>
# ...
# ...
# Routing Complete
# 
# You should also include all of the Self routes in your routing_table argument -- e.g.,  Switch (ID = 4) should include the following entry: 		
# 4,4:4

def routing_table_update(routing_table):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append("Routing Update\n")
    for row in routing_table:
        log.append(f"{row[0]},{row[1]}:{row[2]}\n")
    log.append("Routing Complete\n")
    write_to_log(log)

# "Unresponsive/Dead Neighbor Detected" Format is below:
#
# Timestamp
# Neighbor Dead <Neighbor ID>

def neighbor_dead(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Dead {switch_id}\n")
    write_to_log(log) 

# "Unresponsive/Dead Neighbor comes back online" Format is below:
#
# Timestamp
# Neighbor Alive <Neighbor ID>

def neighbor_alive(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Alive {switch_id}\n")
    write_to_log(log) 

def write_to_log(log):
    with open(LOG_FILE, 'a+') as log_file:
        log_file.write("\n\n")
        # Write to log
        log_file.writelines(log)

def process_table(data):
    global failed_neighbor_id
    #convert from string to fields
    routing_table = []
    lines = data.split('\n')
    for l in lines:
        if l.strip():
            switch, other = l.split(',',1)
            dest, next_dist = other.split(':')

            next, dist = next_dist.split(',')
            routing_table.append([int(switch), int(dest), int(next), int(dist)])
            print([int(switch), int(dest), int(next), int(dist)])
            if int(next) == -1:
                print("AAAAAAAAAAAAAAAAAAAAAAAAAA")
                failed_neighbor_id = int(dest)
    print("failed_neighbor_id: ", failed_neighbor_id)
    print("routing_table: ", routing_table)
    routing_table_update(routing_table)


def process_register_response(data):
    global neighbor_status
    lines = data.split('\n')
    #print(data)
    for l in lines:
        if l.strip():
            neighbor_id, hostname, port = l.split()
            #print("id: ", neighbor_id, "hostname: ", hostname, "port: ", port)
            neighbor_status[int(neighbor_id)] = (hostname, int(port), True)

def register_with_controller(switch_id, cont_host, cont_port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        message = f"{switch_id} Register_Request".encode()
        s.sendto(message, (cont_host, cont_port))
        register_request_sent()
        
        data,_ = s.recvfrom(1024)
        data = data.decode()

        register_response_received()
        process_register_response(data)

        tables,_ = s.recvfrom(1024)
        # print("recieved tables")
        process_table(tables.decode())
        

def send_topology_update(my_id, controller_host, controller_port):
    message = f"Topology_Update\n{my_id}\n"
    message += "\n".join([f"{neighbor_id} {'True' if status else 'False'}" for neighbor_id, (_, _, status) in neighbor_status.items()])
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        print("topology update:", message)
        s.sendto(message.encode(), (controller_host, controller_port))

def keep_alive(my_id, hostname, port):
    while True:
        send_keep_alive(my_id)
        handle_keep_alive_timeouts(my_id, hostname, port)
        time.sleep(K)

        #time.sleep(K)
#1.	Each switch sends a Keep Alive message every K seconds to each of the neighboring switches that it thinks is ‘alive’.
def send_keep_alive(my_id):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        for neighbor_id, (hostname, port, alive) in neighbor_status.items():
            #check if neighbor is alive and not myself
            if neighbor_id != my_id and alive and (failed_neighbor_id is None or (failed_neighbor_id != neighbor_id)):
                message = f"{my_id} KEEP_ALIVE".encode()
                s.sendto(message, (hostname, port))
                #print("sent keep alive")


def handle_keep_alive_timeouts(my_id, controller_host, controller_port):
    current_time = datetime.now()
    for neighbor_id in list(neighbor_status.keys()):
        if (current_time - last_keep_alive_received.get(neighbor_id, datetime.now())).total_seconds() > TIMEOUT:
            if neighbor_status[neighbor_id][2]:  # if neighbor was considered alive
                neighbor_status[neighbor_id] = (neighbor_status[neighbor_id][0], neighbor_status[neighbor_id][1], False)
                neighbor_dead(neighbor_id)
                # print("neighbor dead")
        else:
            if not neighbor_status[neighbor_id][2] and neighbor_id != failed_neighbor_id:  # if neighbor was considered dead and now is alive
                print(f"{neighbor_id}: {neighbor_status[neighbor_id][2]}")
                neighbor_status[neighbor_id] = (neighbor_status[neighbor_id][0], neighbor_status[neighbor_id][1], True)
                # print("now alive")
        if neighbor_id != failed_neighbor_id:
            send_topology_update(my_id, controller_host, controller_port)


def listen_for_keep_alive(my_id, controller_host, controller_port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        #time.sleep(K)
        s.bind(('', neighbor_status[my_id][1]))
        while True:
            data, addr = s.recvfrom(1024)
            data = data.decode()
            #print("received keep alive")
            # print(data)
            if "KEEP_ALIVE" in data:
                neighbor_id = int(data.split()[0])
                if neighbor_id == failed_neighbor_id:
                    continue
                last_keep_alive_received[neighbor_id] = datetime.now()
                    
                if not neighbor_status[neighbor_id][2]:
                    print("back online")
                    # Neighbor comes back online
                    neighbor_status[neighbor_id] = (neighbor_status[neighbor_id][0], neighbor_status[neighbor_id][1], True)
                    neighbor_alive(neighbor_id)  
                    send_topology_update(my_id, controller_host, controller_port) 
            else: 
                process_table(data)

def main():

    global LOG_FILE, failed_neighbor_id

    #Check for number of arguments and exit if host/port not provided
    num_args = len(sys.argv)
    if num_args < 4:
        print ("switch.py <Id_self> <Controller hostname> <Controller Port>\n")
        sys.exit(1)

    my_id = int(sys.argv[1])
    LOG_FILE = 'switch' + str(my_id) + ".log" 

    # Write your code below or elsewhere in this file
    controller_host = sys.argv[2]
    controller_port = int(sys.argv[3])

    if "-f" in sys.argv:
        f_index = sys.argv.index("-f")
        if f_index + 1 < len(sys.argv):
            failed_neighbor_id = int(sys.argv[f_index + 1])


    register_with_controller(my_id, controller_host, controller_port)
    
    if failed_neighbor_id is not None:
        # Mark the specified neighbor as failed
        if failed_neighbor_id in neighbor_status:
            neighbor_status[failed_neighbor_id] = (neighbor_status[failed_neighbor_id][0], neighbor_status[failed_neighbor_id][1], False)

    #print(neighbor_status)
    threading.Thread(target=keep_alive, args=(my_id, controller_host, controller_port), daemon=True).start()
    threading.Thread(target=listen_for_keep_alive, args=(my_id, controller_host, controller_port), daemon=True).start()

    #listen_for_keep_alive(my_id, controller_host, controller_port)

    while True:
        time.sleep(1)
if __name__ == "__main__":
    main()