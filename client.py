import socket 
import sys
import subprocess
import os
import math

# Taking of input parameters
print('Number of arguments:', len (sys.argv))
print('Argument List:', str(sys.argv))

# constants
OIP = sys.argv[sys.argv.index('-a')+1]
UDP_PORT_O = int(sys.argv[sys.argv.index('-p')+1])
FILE = sys.argv[sys.argv.index('-f')+1]
BALANCED = -(int(sys.argv[sys.argv.index('-m')+1])-2) # modified so 1 when Balanced and 0 otherwise.
SERVER_NUM = int(sys.argv[sys.argv.index('-s')+1])

MAX_RET = 30       # maximum number of times to resend something.
MAX_PAYLOAD = 100  # maximum payload in bytes
UDP_PORT_S = 4650

print ("orchestrator IP", OIP, "Port", UDP_PORT_O, "File", FILE, "Balanced:", BALANCED, "Server Number", SERVER_NUM )

# UDP setup

udp_socket_o = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP socket for the orchestrator

udp_socket_o.bind(('',4650))



##################################### STEP 1: INTENT MESSAGE, REPLY ###############################

# sending of intent message.
intent_message = "Type:0;"
o_message = "".encode()



# wait for the orchestrator message (type 1)
udp_socket_o.sendto(intent_message.encode(), (OIP,UDP_PORT_O))
o_message, addr = udp_socket_o.recvfrom(1024)
    
  


# throw error and end execution if reply is blank
if o_message.decode() == "":
  raise Exception("Orchestrator reply not received.")

# Split the contents to retrieve the data from the type 1 message
fields = (o_message.decode()).split(";")
data = ((fields[2])[6:-1]).strip("}{").split("}, {")

# processing on the string to put the values in an array, where 0 is the ip address and 1 is the name.
server1 = data[0].split(",")
server2 = data[1].split(",")
server3 = data[2].split(",")

server1[0] = (server1[0])[15:-1]
server1[1] = (server1[1])[10:-1]

server2[0] = (server2[0])[15:-1]
server2[1] = (server2[1])[10:-1]

server3[0] = (server3[0])[15:-1]
server3[1] = (server3[1])[10:-1]

o_type = int((fields[0])[5:])
TID = int((fields[1])[4:])

# give feedback on what was processesd
print(o_type,TID,server1,server2,server3)

####################################### STEP 2: LOAD BALANCING #####################################

server_weights = [0] * 3 

if not BALANCED:
  # Set the load fraction to 1 for the selected server
  server_weights[SERVER_NUM-1] = 1
else:
  # TO DO: Abstract the weighting so I could try different functions.

  # Check latency with subprocess call to ping.
  server1_ping = subprocess.check_output(['ping', server1[0], '-c', '3', '-q'], stderr=subprocess.STDOUT, universal_newlines=True)
  server2_ping = subprocess.check_output(['ping', server2[0], '-c', '3', '-q'], stderr=subprocess.STDOUT, universal_newlines=True) 
  server3_ping = subprocess.check_output(['ping', server3[0], '-c', '3', '-q'], stderr=subprocess.STDOUT, universal_newlines=True)

  # load among servers according to the ratio of their delays
 
  # getting the stats of the ping to server1. [0] min, [1] avg, [2] max, [3] stddev
  server1_stats = (server1_ping[(server1_ping.find('=')+2):].strip(" ms\n0")).split("/")
  server2_stats = (server2_ping[(server2_ping.find('=')+2):].strip(" ms\n0")).split("/")
  server3_stats = (server3_ping[(server3_ping.find('=')+2):].strip(" ms\n0")).split("/")

  # load is distributed by comapring to the worst performer.
  worst = max(float(server1_stats[1]),float(server2_stats[1]),float(server3_stats[1]))

  # check how much faster each server is than the worst value
  server1_fraction = worst / float(server1_stats[1]) 
  server2_fraction = worst / float(server2_stats[1])
  server3_fraction = worst / float(server3_stats[1])

  # calculate the ratios to each other so that they add up to 1.
  total = server1_fraction+server2_fraction+server3_fraction
  server_weights = [server1_fraction/total,server2_fraction/total,server3_fraction/total]
  print(server_weights)

  # scale so that no server gets < 0.10 (and consequently none gets 1.0). Get from the next lowest that has more than 0.10
  while min(server_weights) < 0.1:
    min_index = server_weights.index(min(server_weights))
    max_index = server_weights.index(max(server_weights))
    mid_index = list(set([0,1,2]) - set([min_index,max_index]))[0]

    if server_weights[min_index] < 0.10:
      needed = 0.10 - server_weights[min_index]
      if server_weights[mid_index] > 0.10:
        # get as much as you can from it
        available = server_weights[mid_index] - 0.10
        if available > needed:
          server_weights[mid_index] -= needed
          server_weights[min_index] += needed
          needed = 0
        else:
          server_weights[mid_index] = 0.10
          server_weights[min_index] += available
          needed -= available
      if needed > 0:
        server_weights[min_index] = 0.10
        server_weights[max_index] -= needed 

# output processed weights
print(server_weights)

######################################## STEP 3: SENDING THE PAYLOAD ############################## 
f_size = os.path.getsize(FILE)
payload_size = MAX_PAYLOAD if f_size >= 700 else (10 if f_size >= 70 else 1) # set appropriate payload sizes to fit min weight
num_segments = math.ceil(f_size/payload_size) # calculate the number of segments needed
num_servers = [0] * 3

num_servers[0] = math.ceil(server_weights[0] * num_segments)
num_servers[1] = math.ceil(server_weights[1] * num_segments)
num_servers[2] = math.ceil(server_weights[2] * num_segments)

min_index = num_servers.index(min(server_weights))
max_index = num_servers.index(max(server_weights))
mid_index = list(set([0,1,2]) - set([min_index,max_index]))[0]
# For sure max_index would be significantly  more than 0.1 of the data, we prioritize the smaller weights getting the ceiling values.
if num_segments < 5:     
  num_servers[min_index] = 1 # Additional handling for the case when there are less than 5 segments to send, since simply using ceiling
  num_servers[mid_index] = 1 # could cause the max_index to get nothing. Note that when less than 10 segments, weights may be off.
num_servers[max_index] = num_segments - num_servers[mid_index] - num_servers[min_index]

print(f_size)
print(num_segments)
print(num_servers[0])
print(num_servers[1])
print(num_servers[2])

segments_array = [0] * num_segments 

def create_segment(tid,seq,payload):
  return "Type:2;TID:%d;SEQ:%d;DATA:%s" % (tid,seq,payload)

# divide the file into the segments
f = open(FILE, "r", encoding="utf8")
for i in range(num_segments):
  rem = f_size - (num_segments-1)*payload_size
  if rem != 0 and ((max_index == 0 and i == num_servers[0]-1) or (max_index == 1 and i == num_servers[0]+num_servers[1]-1) or (max_index == 2 and i == num_segments-1)):
    payload = f.read(rem)
  else:
    payload = f.read(payload_size)
  segments_array[i] = payload 
f.close
udp_socket_o.settimeout(3.0) # this sets the 3s timeout for any message.
# send each of the segments
for i in range(num_segments):
  # determine the server to send to
  server_address = ""
  if i < num_servers[0]:
    server_address = server1[0]
  elif i < num_servers[0]+num_servers[1]:
    server_address = server2[0]
  else:
    server_address = server3[0]

  # indicate what is being sent
  print("sending:",segments_array[i]," to ",server_address)
  
  s_message = "".encode()

  # waiting for reply.
  for _ in range(MAX_RET):
    try:
      udp_socket_o.sendto((segments_array[i]).encode(), (server_address,UDP_PORT_S))
      s_message, addr = udp_socket_o.recvfrom(1024)

      response = (s_message.decode()).split(";")
      r_type = int((response[0])[5:])
      r_tid = int((response[1])[4:])
      r_seq = int((response[2])[4:])

      if r_type == 3 and r_tid == TID and r_seq == i: 
        break

      
    except socket.timeout:
      pass

  # throw error if no more retries left and end execution
  if s_message.decode() == "":
    raise Exception("segment not received.")
