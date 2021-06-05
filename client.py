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

MAX_RET = 30 #maximum number of times to resend something.
MAX_PAYLOAD = 100
UDP_PORT_S = 4650

print ("orchestrator IP", OIP, "Port", UDP_PORT_O, "File", FILE, "Balanced:", BALANCED, "Server Number", SERVER_NUM )

# UDP setup

udp_socket_o = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP socket for the orchestrator

udp_socket_o.bind(('',4650))

udp_socket_o.settimeout(3.0) # this sets the 3s timeout for any message.

##################################### STEP 1: INTENT MESSAGE, REPLY ###############################

# sending of intent message.
intent_message = "Type:0;"
o_message = "".encode()

# waiting for reply. No concurrence possible here since you can't check the servers without the addresses.
for _ in range(MAX_RET):
  try:
    udp_socket_o.sendto(intent_message.encode(), (OIP,UDP_PORT_O))
    o_message, addr = udp_socket_o.recvfrom(1024)
    break
  except socket.timeout:
    pass

# throw error if no more retries left and end execution
if o_message.decode() == "":
  raise Exception("Orchestrator reply not received.")


#o_message = "Type:1;TID:23;DATA:[{'ip address': '8.8.8.8','name': 'Google'}, {ip address: 1.2.3.4,area: Test},{ip address: 18.220.32.204,area: CS145 sample server}]"

# now we analyze the contents
fields = (o_message.decode()).split(";")
data = ((fields[2])[6:-1]).strip("}{").split("}, {")

#processing on the string to put the values in an array, where 0 is the ip address and 1 is the area.
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

print(o_type,TID,server1,server2,server3)

####################################### STEP 2: LOAD BALANCING #####################################

server_weights = [0] * 3 

if not BALANCED:
  # Set the load fraction to 1 for the selected server
  server_weights[SERVER_NUM-1] = 1
else:
  # TO DO: Abstract the weighting so I could try different functions.


  # otherwise, check ping with some ICMP to determine rtt to each server.
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

  server1_fraction = worst / float(server1_stats[1]) # get how many times faster server is from the worst. 
  server2_fraction = worst / float(server2_stats[1])
  server3_fraction = worst / float(server3_stats[1])

  # calculate the ratios to each other so that they add up to 1.
  something = server1_fraction+server2_fraction+server3_fraction

  server_weights = [server1_fraction/something,server2_fraction/something,server3_fraction/something]
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

# processed weights
print(server_weights)

######################################## STEP 3: SENDING THE PAYLOAD ##############################

f_size = os.path.getsize(FILE)
num_segments = math.ceil(f_size/MAX_PAYLOAD)

# Get the actual number of segments for each server
num_server_1 = math.ceil(server_weights[0] * num_segments)
num_server_2 = math.ceil(server_weights[1] * num_segments)
num_server_3 = num_segments - num_server_1 - num_server_2

print(f_size)
print(num_segments)
print(num_server_1)
print(num_server_2)
print(num_server_3)

segments_array = [0] * num_segments 

def create_segment(tid,seq,payload):
  return "Type:2;TID:%d;SEQ:%d;DATA:%s" % (tid,seq,payload)

# divide the file into the segment

f = open(FILE, "r", encoding="utf8")

for i in range(num_segments):
  payload = f.read(100)
  segments_array[i] = create_segment(TID,i,payload)

f.close

for i in range(num_segments):
  server_address = ""
  if i < num_server_1:
    server_address = server1[0]
  elif i < num_server_1+num_server_2:
    server_address = server2[0]
  else:
    server_address = server3[0]

  print("sent:",segments_array[i]," to ",server_address)
  
  # waiting for reply.
  for _ in range(MAX_RET):
    try:
      udp_socket_o.sendto((segments_array[i]).encode(), (server_address,UDP_PORT_S))
      s_message, addr = udp_socket_o.recvfrom(1024)

      #response = str(s_message).split(";")
      #r_type = int((response[0])[5:])
      #r_tid = int((response[1])[4:])
      #r_seq = (response[2])[4:]

      break

      # do we need to consider that a different not matching ack could be sent? tried to do that, so let's see.
    except socket.timeout:
      pass

  # throw error if no more retries left and end execution
  if o_message == "":
    raise Exception("Orchestrator reply not received.")
