
#!/usr/bin/env python

"""This is the Controller Starter Code for ECE50863 Lab Project 1
Author: Xin Du
Email: du201@purdue.edu
Last Modified Date: December 9th, 2021
"""

import sys
from datetime import date, datetime
from collections import defaultdict
import heapq
import socket
import threading
import time
import select
# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "Controller.log"
switch_status = {} #checks if switch is dead or alive
K = 2
TIMEOUT = K * 3
last_update_time = {} #last time switch was updated 
link_status = defaultdict(lambda: defaultdict(lambda: True)) #checks if link is dead or alive
expected_switch_count = 0
graph = defaultdict(lambda: defaultdict(lambda: 9999))
switch_info = {}
# Those are logging functions to help you follow the correct logging standard

# "Register Request" Format is below:
#
# Timestamp
# Register Request <Switch-ID>

def register_request_received(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Request {switch_id}\n")
    write_to_log(log)

# "Register Responses" Format is below (for every switch):
#
# Timestamp
# Register Response <Switch-ID>

def register_response_sent(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Response {switch_id}\n")
    write_to_log(log) 

# For the parameter "routing_table", it should be a list of lists in the form of [[...], [...], ...]. 
# Within each list in the outermost list, the first element is <Switch ID>. The second is <Dest ID>, and the third is <Next Hop>, and the fourth is <Shortest distance>
# "Routing Update" Format is below:
#
# Timestamp
# Routing Update 
# <Switch ID>,<Dest ID>:<Next Hop>,<Shortest distance>
# ...
# ...
# Routing Complete
#
# You should also include all of the Self routes in your routing_table argument -- e.g.,  Switch (ID = 4) should include the following entry: 		
# 4,4:4,0
# 0 indicates ‘zero‘ distance
#
# For switches that can’t be reached, the next hop and shortest distance should be ‘-1’ and ‘9999’ respectively. (9999 means infinite distance so that that switch can’t be reached)
#  E.g, If switch=4 cannot reach switch=5, the following should be printed
#  4,5:-1,9999
#
# For any switch that has been killed, do not include the routes that are going out from that switch. 
# One example can be found in the sample log in starter code. 
# After switch 1 is killed, the routing update from the controller does not have routes from switch 1 to other switches.

def routing_table_update(routing_table):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append("Routing Update\n")
    for row in routing_table:
        log.append(f"{row[0]},{row[1]}:{row[2]},{row[3]}\n")
    log.append("Routing Complete\n")
    write_to_log(log)

# "Topology Update: Link Dead" Format is below: (Note: We do not require you to print out Link Alive log in this project)
#
#  Timestamp
#  Link Dead <Switch ID 1>,<Switch ID 2>

def topology_update_link_dead(switch_id_1, switch_id_2):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Link Dead {switch_id_1},{switch_id_2}\n")
    write_to_log(log) 

# "Topology Update: Switch Dead" Format is below:
#
#  Timestamp
#  Switch Dead <Switch ID>

def topology_update_switch_dead(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Switch Dead {switch_id}\n")
    write_to_log(log) 

# "Topology Update: Switch Alive" Format is below:
#
#  Timestamp
#  Switch Alive <Switch ID>

def topology_update_switch_alive(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Switch Alive {switch_id}\n")
    write_to_log(log) 

def write_to_log(log):
    with open(LOG_FILE, 'a+') as log_file:
        log_file.write("\n\n")
        # Write to log
        log_file.writelines(log)

def create_graph(file):
    global expected_switch_count, graph

    #open file
    with open(file, 'r') as f:

        #skip first line
        expected_switch_count = int(f.readline().strip())

        for x in f:
            if x.strip():
                s1, s2, dist = map(int, x.split())
                #add distances to node
                graph[s1][s2] = dist
                graph[s2][s1] = dist

    return graph

#3.	Once all switches have registered, the controller responds with a Register Response message to each switch which includes the following information
#a.	The id of each neighboring switch
#b.	a flag indicating whether the neighbor is alive or not (initially, all switches are alive)
#c.	for each live switch, the host/port information of that switch process.  

def send_register_response(socket):
    # Send Register Response containing information about each switch to all switches
    message = ""
    for switch_id, (hostname, port) in switch_info.items():
        message += f"{switch_id} {hostname} {port}\n"
    
    for switch_id, (hostname, port) in switch_info.items():
        print("register response:", message)
        socket.sendto(message.encode(), (hostname, port))
        register_response_sent(switch_id)

        
def handle_register_request(data, client_add, sock):
    switch_id = int(data.decode().split()[0])
    register_request_received(switch_id)
    switch_info[switch_id] = client_add
    switch_status[switch_id] = True  # switch is alive
    last_update_time[switch_id] = datetime.now()

    # Initialize link status to True for direct neighbors upon registration
    for neighbor_id in graph[switch_id]:
        link_status[switch_id][neighbor_id] = True
        link_status[neighbor_id][switch_id] = True
    
    
    if len(switch_info) == expected_switch_count:
        send_register_response(sock)
        #print("sent_response")
            # Trigger Route Update after all switches have registered
        tables = create_table()
        send_table(tables, sock)
        #print("sent tables: ", tables)

def udp_server(port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        #bind to port
        s.bind(('localhost', port))
        s.setblocking(0)

        while True:
            #print('running server...')
            ready = select.select([s], [], [], 1)

            if ready[0]:
                data, client_add = s.recvfrom(1024)
                message = data.decode()
                
                if "Register_Request" in message:
                    handle_register_request(data, client_add, s)

                
                if "Topology_Update" in message:
                    #print(("received topology_update"))
                    handle_topology_update(data, client_add, s)

            for switch_id, last_time in list(last_update_time.items()):
                if (datetime.now() - last_time).total_seconds() > TIMEOUT:
                    # print("last time", last_time)
                    # print("datetime", datetime.now())
                    # print("difference", (datetime.now() - last_time).total_seconds())
                    if switch_status.get(switch_id, True):
                        switch_status[switch_id] = False
                        topology_update_switch_dead(switch_id)

                        tables = create_table()
                        send_table(tables, s)

            # if "Keep_Alive" in message:
            #     print("keep alive")
            #     ackeep_alive(data, client_add, s, switch_info)
        
def handle_keep_alive(data, client_add, sock):
    switch_id = int(data.decode().split()[0])
    #print(switch_id)
    if switch_id in switch_info:
        last_update_time[switch_id] = datetime.now()
        if not switch_status[switch_id]:
            switch_status[switch_id] = True
            topology_update_switch_alive(switch_id)
    
def dijkstra(start):
    dist = {node: float('infinity') for node in graph}
    prev = {node: None for node in graph}
    dist[start] = 0
    pq = [(0, start)]

    while pq:
        curr_dist, curr = heapq.heappop(pq)
        for neighbor in graph[curr]:
            if link_status[curr][neighbor]:  # Consider only alive links
                weight = graph[curr][neighbor]
                distance = curr_dist + weight
                if distance < dist[neighbor]:
                    dist[neighbor] = distance
                    prev[neighbor] = curr
                    heapq.heappush(pq, (distance, neighbor))
    return dist, prev


def create_table():
    routing_tables = {}
    alive_switches = {switch_id for switch_id, alive in switch_status.items() if alive}
    for switch in alive_switches:
        #get paths and distances from dijkstra
        dists, prev = dijkstra(switch)

        table = []

        for dest in graph.keys():
            
            
            if dest not in alive_switches:
                next = -1
                shortest = 9999
            
            # if link_status[switch][dest] == False or link_status[dest][switch] == False:
            #     next= -1
            #     shortest = 9999


            elif dists[dest] == float('infinity'):
                next = -1
                shortest = 9999
            else:
                next = dest

                if next != switch:
                    while prev[next] != switch:
                        next = prev[next]
                
                shortest = dists[dest]
            table.append([switch, dest, next, shortest])
        #add to routing tables
        routing_tables[switch] = table
    #print(routing_tables)
    return routing_tables

def send_table(routing_tables, socket):
    # table for routing_table
    master_table = []

    iterations = 0
    for switch, table in routing_tables.items():
        if switch_status.get(switch, False):
            # print("sending table to: ", switch)
            address = switch_info[switch]
            routes = []

            #convert to string to send
            routes = [f"{x[0]}, {x[1]} : {x[2]}, {x[3]}" for x in table]
            message = "\n".join(routes)
            # print(address)
            socket.sendto(message.encode(), address)
            # print("sent")
            master_table.extend(table)
        iterations += 1
    
    # print("iterations: ", iterations)

    # print(master_table)
    routing_table_update(master_table)

def handle_topology_update(data, client_add, sock):
    #Format
    #<Switch-ID>
    #<Neighbor Id> <True/False indicating whether the neighbor is alive> (for all neighbors
    lines = data.decode().split('\n')
    switch_id = int(lines[1])

    last_update_time[switch_id] = datetime.now()

    changed = False
    for line in lines[2:]:
        #print(line)
        if line.strip():
            neighbor_id, alive = line.split()
            neighbor_id = int(neighbor_id)
            alive_status = alive == "True"
            if link_status[switch_id][neighbor_id] == True or link_status[neighbor_id][switch_id] == True:
                if link_status[switch_id][neighbor_id] != alive_status or link_status[neighbor_id][switch_id] != alive_status:
                    changed = True
                    link_status[switch_id][neighbor_id] = alive_status
                    link_status[neighbor_id][switch_id] = alive_status
                
                    if not alive_status:
                    # print("dead link: ", switch_id, neighbor_id)
                        topology_update_link_dead(switch_id, neighbor_id)



    # Recompute and send routing tables if there's a change in topology
    if changed:
        tables = create_table()
        send_table(tables, sock)

#1.	If the controller does not receive a Topology Update message from a switch for TIMEOUT seconds, then it considers that switch ‘dead’, and updates its topology. 
# def check_for_dead_switches(s):
#     while True:
#         time.sleep(K)
#         current_time = datetime.now()
#         for switch_id, last_time in list(last_update_time.items()):
#             if (current_time - last_time).total_seconds() > TIMEOUT:
#                 if switch_status.get(switch_id, True):  # If switch was considered alive
#                     switch_status[switch_id] = False
#                     topology_update_switch_dead(switch_id)
    
#         tables = create_table()
#         send_table(tables, s)



def main():
    #Check for number of arguments and exit if host/port not provided
    num_args = len(sys.argv)
    if num_args < 3:
        print ("Usage: python controller.py <port> <config file>\n")
        sys.exit(1)
    
    # Write your code below or elsewhere in this file
    port = int(sys.argv[1])
    config = sys.argv[2]

    #create_graph
    create_graph(config)
    create_table()
    
    
    #print('graph created')

    #start server
    udp_server(port)



if __name__ == "__main__":
    main()